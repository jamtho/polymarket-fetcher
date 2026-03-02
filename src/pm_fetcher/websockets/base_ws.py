# Copyright (c) 2026 James Thompson. All rights reserved.

"""Base WebSocket client — reconnection, keepalive, message queue, writer."""

from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from typing import Any

import aiohttp
import orjson
import structlog

from pm_fetcher.storage.json_writer import JsonWriter

log = structlog.get_logger()


class BaseWebSocket(ABC):
    """Base class for WebSocket connections with auto-reconnect and keepalive.

    Messages are placed into a bounded queue and drained to JSONL by a
    dedicated writer coroutine, decoupling reception from disk I/O.
    """

    name: str = "base_ws"

    def __init__(
        self,
        url: str,
        writer: JsonWriter,
        *,
        ping_interval: float = 8.0,
        reconnect_base: float = 1.0,
        reconnect_max: float = 60.0,
        reconnect_jitter: float = 0.2,
        max_consecutive_failures: int = 5,
        queue_size: int = 10_000,
    ) -> None:
        self._url = url
        self._writer = writer
        self._ping_interval = ping_interval
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max
        self._reconnect_jitter = reconnect_jitter
        self._max_failures = max_consecutive_failures
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._consecutive_failures = 0
        self._connected = False

    @property
    def is_fallback_active(self) -> bool:
        """True if WS has failed too many times and HTTP fallback should activate."""
        return self._consecutive_failures >= self._max_failures

    async def on_connect(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Called after connection is established. Override to send subscriptions."""

    @abstractmethod
    async def on_message(self, data: dict[str, Any]) -> None:
        """Process a received message. Put into queue for writing."""
        ...

    async def _enqueue(self, data: dict[str, Any]) -> None:
        """Put a message on the write queue, dropping if full."""
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            log.warning("ws queue full, dropping message", ws=self.name)

    async def _keepalive(self, ws: aiohttp.ClientWebSocketResponse, stop: asyncio.Event) -> None:
        """Send periodic pings to keep the connection alive."""
        while not stop.is_set() and not ws.closed:
            try:
                await ws.ping()
                await asyncio.sleep(self._ping_interval)
            except Exception:
                break

    async def _drain_queue(self, stop: asyncio.Event) -> None:
        """Drain messages from the queue to the JSONL writer."""
        while not stop.is_set():
            try:
                msg = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._writer.write(msg, source=self.name)
            except asyncio.TimeoutError:
                continue
            except Exception:
                log.exception("drain error", ws=self.name)

        # Drain remaining messages on shutdown
        while not self._queue.empty():
            try:
                msg = self._queue.get_nowait()
                await self._writer.write(msg, source=self.name)
            except Exception:
                break

    def _backoff_delay(self) -> float:
        """Exponential backoff with jitter."""
        delay = min(
            self._reconnect_base * (2 ** self._consecutive_failures),
            self._reconnect_max,
        )
        jitter = delay * self._reconnect_jitter * random.random()
        return delay + jitter

    async def _connect_and_listen(self, stop: asyncio.Event) -> None:
        """Single connection attempt — connect, subscribe, listen."""
        self._session = aiohttp.ClientSession()
        try:
            async with self._session.ws_connect(self._url) as ws:
                self._ws = ws
                self._connected = True
                self._consecutive_failures = 0
                log.info("ws connected", ws=self.name, url=self._url)

                await self.on_connect(ws)

                keepalive_stop = asyncio.Event()
                keepalive_task = asyncio.create_task(self._keepalive(ws, keepalive_stop))

                try:
                    async for msg in ws:
                        if stop.is_set():
                            break
                        if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                            try:
                                data = orjson.loads(msg.data)
                                await self.on_message(data)
                            except orjson.JSONDecodeError:
                                log.warning(
                                    "ws json decode error", ws=self.name,
                                    preview=str(msg.data)[:200],
                                )
                            except Exception as exc:
                                log.exception(
                                    "ws message handler error", ws=self.name,
                                    error=str(exc),
                                )
                        elif msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            break
                finally:
                    keepalive_stop.set()
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except asyncio.CancelledError:
                        pass

        except (aiohttp.ClientError, OSError) as e:
            log.warning("ws connection error", ws=self.name, error=str(e))
        finally:
            self._connected = False
            self._ws = None
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = None

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop — reconnects with backoff until stop_event is set."""
        drain_task = asyncio.create_task(self._drain_queue(stop_event))

        try:
            while not stop_event.is_set():
                await self._connect_and_listen(stop_event)

                if stop_event.is_set():
                    break

                self._consecutive_failures += 1
                if self._consecutive_failures >= self._max_failures:
                    log.warning(
                        "ws max failures reached, HTTP fallback active",
                        ws=self.name,
                        failures=self._consecutive_failures,
                    )

                delay = self._backoff_delay()
                log.info("ws reconnecting", ws=self.name, delay=f"{delay:.1f}s")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass
        finally:
            stop_event.set()  # Signal drain to stop
            drain_task.cancel()
            try:
                await drain_task
            except asyncio.CancelledError:
                pass

    async def send_json(self, data: dict[str, Any]) -> None:
        """Send a JSON message on the current connection."""
        if self._ws and not self._ws.closed:
            await self._ws.send_str(orjson.dumps(data).decode())
