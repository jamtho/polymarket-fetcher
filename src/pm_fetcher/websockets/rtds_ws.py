"""RTDS WebSocket — activity feed and crypto prices."""

from __future__ import annotations

from typing import Any

import aiohttp
import structlog

from pm_fetcher.storage.json_writer import JsonWriter
from pm_fetcher.websockets.base_ws import BaseWebSocket

log = structlog.get_logger()


class RtdsWebSocket(BaseWebSocket):
    """Connects to RTDS for the activity (trades) feed and crypto prices.

    Subscribes to `activity` and `crypto_prices` channels.
    PING every 4s (server requirement).
    """

    name = "ws_rtds"

    def __init__(
        self,
        url: str,
        activity_writer: JsonWriter,
        crypto_writer: JsonWriter,
        *,
        ping_interval: float = 4.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(url, activity_writer, ping_interval=ping_interval, **kwargs)
        self._activity_writer = activity_writer
        self._crypto_writer = crypto_writer

    async def on_connect(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Subscribe to activity and crypto_prices channels."""
        await self.send_json({"type": "subscribe", "channel": "activity"})
        await self.send_json({"type": "subscribe", "channel": "crypto_prices"})
        log.info("rtds subscribed", channels=["activity", "crypto_prices"])

    async def on_message(self, data: dict[str, Any]) -> None:
        """Route messages to activity or crypto writer."""
        channel = data.get("channel", "")
        if channel == "crypto_prices":
            await self._crypto_writer.write(data)
        else:
            # Default to activity writer
            await self._enqueue(data)
