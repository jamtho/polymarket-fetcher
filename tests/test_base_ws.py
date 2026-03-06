# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import orjson
import pytest

from pm_fetcher.websockets.base_ws import BaseWebSocket


class StubWebSocket(BaseWebSocket):
    """Concrete subclass for testing the base class."""

    name = "test_ws"

    def __init__(self, **kwargs: Any) -> None:
        writer = AsyncMock()
        writer.write = AsyncMock()
        super().__init__(url="wss://example.com", writer=writer, **kwargs)
        self.received: list[Any] = []

    async def on_message(self, data: dict[str, Any]) -> None:
        self.received.append(data)


class StubWithTextPong(StubWebSocket):
    """Stub that filters text keepalive messages."""

    _text_pong_messages = {"PONG", "heartbeat"}
    pings_received: list[str]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pings_received = []

    async def _on_text_ping(self, ws: aiohttp.ClientWebSocketResponse, text: str) -> None:
        self.pings_received.append(text)


# --- Backoff ---


class TestBackoff:
    def test_first_failure(self):
        ws = StubWebSocket()
        ws._consecutive_failures = 0
        delay = ws._backoff_delay()
        # base=1.0 * 2^0 = 1.0, plus up to 20% jitter
        assert 1.0 <= delay <= 1.2

    def test_scales_exponentially(self):
        ws = StubWebSocket()
        ws._consecutive_failures = 3
        delay = ws._backoff_delay()
        # base=1.0 * 2^3 = 8.0, plus up to 20% jitter
        assert 8.0 <= delay <= 9.6

    def test_capped_at_max(self):
        ws = StubWebSocket(reconnect_max=30.0)
        ws._consecutive_failures = 10
        delay = ws._backoff_delay()
        # 2^10 = 1024, capped at 30, plus jitter
        assert 30.0 <= delay <= 36.0


# --- Fallback ---


class TestFallback:
    def test_not_active_initially(self):
        ws = StubWebSocket()
        assert not ws.is_fallback_active

    def test_active_after_max_failures(self):
        ws = StubWebSocket(max_consecutive_failures=3)
        ws._consecutive_failures = 3
        assert ws.is_fallback_active


# --- Queue ---


class TestQueue:
    @pytest.mark.asyncio
    async def test_enqueue_and_drain(self):
        ws = StubWebSocket(queue_size=10)
        stop = asyncio.Event()

        await ws._enqueue({"msg": 1})
        await ws._enqueue({"msg": 2})
        assert ws._queue.qsize() == 2

        # Stop after a short drain
        async def stop_soon():
            await asyncio.sleep(0.1)
            stop.set()

        await asyncio.gather(ws._drain_queue(stop), stop_soon())

        ws._writer.write.assert_any_call({"msg": 1}, source="test_ws")
        ws._writer.write.assert_any_call({"msg": 2}, source="test_ws")

    @pytest.mark.asyncio
    async def test_enqueue_drops_when_full(self):
        ws = StubWebSocket(queue_size=1)
        await ws._enqueue({"msg": 1})
        await ws._enqueue({"msg": 2})  # should be dropped
        assert ws._queue.qsize() == 1


# --- Text ping/pong filtering ---


class TestTextPongFiltering:
    def test_base_has_empty_set(self):
        ws = StubWebSocket()
        assert ws._text_pong_messages == set()

    def test_subclass_has_custom_set(self):
        ws = StubWithTextPong()
        assert ws._text_pong_messages == {"PONG", "heartbeat"}
