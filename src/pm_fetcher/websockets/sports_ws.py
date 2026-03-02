# Copyright (c) 2026 James Thompson. All rights reserved.

"""Sports WebSocket — live sports event scores."""

from __future__ import annotations

from typing import Any

import structlog

from pm_fetcher.storage.json_writer import JsonWriter
from pm_fetcher.websockets.base_ws import BaseWebSocket

log = structlog.get_logger()


class SportsWebSocket(BaseWebSocket):
    """Connects to the Sports channel — auto-streams all active sports events.

    No subscription needed; responds to server pings within 10s.
    """

    name = "ws_sports"

    def __init__(
        self,
        url: str,
        writer: JsonWriter,
        *,
        ping_interval: float = 8.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(url, writer, ping_interval=ping_interval, **kwargs)

    async def on_message(self, data: dict[str, Any]) -> None:
        """Write all sports events to JSONL."""
        await self._enqueue(data)
