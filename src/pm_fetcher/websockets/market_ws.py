"""Market WebSocket — orderbook, trades, prices with dynamic subscriptions."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import structlog

from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter
from pm_fetcher.websockets.base_ws import BaseWebSocket

log = structlog.get_logger()


class MarketWebSocket(BaseWebSocket):
    """Connects to the Market channel and subscribes to asset IDs.

    Dynamically updates subscriptions as new markets are discovered.
    Batches subscriptions in groups of `batch_size`.
    """

    name = "ws_market"

    def __init__(
        self,
        url: str,
        writers: dict[str, JsonWriter],
        state: State,
        *,
        batch_size: int = 200,
        ping_interval: float = 8.0,
        **kwargs: Any,
    ) -> None:
        # Use the first writer as the default
        default_writer = next(iter(writers.values()))
        super().__init__(url, default_writer, ping_interval=ping_interval, **kwargs)
        self._writers = writers
        self._state = state
        self._batch_size = batch_size
        self._subscribed_ids: set[str] = set()

    async def on_connect(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Subscribe to all known active token IDs."""
        token_ids = self._state.active_token_ids
        if not token_ids:
            log.info("no token IDs to subscribe", ws=self.name)
            return

        await self._subscribe_batch(token_ids)

    async def _subscribe_batch(self, token_ids: list[str]) -> None:
        """Subscribe in batches to avoid message size limits."""
        new_ids = [tid for tid in token_ids if tid not in self._subscribed_ids]
        if not new_ids:
            return

        for i in range(0, len(new_ids), self._batch_size):
            batch = new_ids[i : i + self._batch_size]
            msg = {
                "type": "subscribe",
                "channel": "market",
                "assets_ids": batch,
            }
            await self.send_json(msg)
            self._subscribed_ids.update(batch)

        log.info("ws subscribed", ws=self.name, count=len(new_ids), total=len(self._subscribed_ids))

    async def update_subscriptions(self) -> None:
        """Called periodically to subscribe to newly discovered tokens."""
        token_ids = self._state.active_token_ids
        new_ids = [tid for tid in token_ids if tid not in self._subscribed_ids]
        if new_ids and self._connected:
            await self._subscribe_batch(new_ids)

    async def on_message(self, data: dict[str, Any]) -> None:
        """Route messages to the appropriate writer based on event type."""
        event_type = data.get("event_type", data.get("type", "unknown"))

        # Route to specific writers
        writer_key = self._route_event(event_type)
        writer = self._writers.get(writer_key, self._writer)

        await self._enqueue({**data, "_event_type": event_type})

        # Also write directly via the specific writer
        if writer_key in self._writers:
            await self._writers[writer_key].write(data)

    @staticmethod
    def _route_event(event_type: str) -> str:
        """Map event types to writer keys."""
        routes = {
            "book": "book",
            "price_change": "price_change",
            "last_trade_price": "last_trade_price",
            "tick_size_change": "book",
            "new_market": "book",
            "resolution": "book",
        }
        return routes.get(event_type, "book")
