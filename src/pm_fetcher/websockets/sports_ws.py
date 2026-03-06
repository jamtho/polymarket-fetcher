# Copyright (c) 2026 James Thompson. All rights reserved.

"""Sports WebSocket — live sports event scores."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import structlog

from pm_fetcher.storage.json_writer import JsonWriter
from pm_fetcher.websockets.base_ws import BaseWebSocket

log = structlog.get_logger()


class SportsWebSocket(BaseWebSocket):
    """Connects to the Sports channel — auto-streams all active sports events.

    No subscription needed; server sends text "ping" every 5s,
    we must respond with "pong" within 10s.
    """

    name = "ws_sports"
    _text_pong_messages = {"ping", "pong"}

    def __init__(
        self,
        url: str,
        writer: JsonWriter,
        *,
        ping_interval: float = 8.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(url, writer, ping_interval=ping_interval, **kwargs)

    async def _keepalive(self, ws: aiohttp.ClientWebSocketResponse, stop: asyncio.Event) -> None:
        """No-op — sports channel keepalive is server-initiated."""
        await stop.wait()

    async def _on_text_ping(self, ws: aiohttp.ClientWebSocketResponse, text: str) -> None:
        """Respond to server text pings with pong."""
        if text == "ping":
            await ws.send_str("pong")

    async def on_message(self, data: dict[str, Any]) -> None:
        """Write all sports events to JSONL."""
        await self._enqueue(data)
