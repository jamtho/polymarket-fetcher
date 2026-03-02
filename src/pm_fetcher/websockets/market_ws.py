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

    Message formats from the server:
    - Initial book snapshot: list of dicts [{market, asset_id, bids, asks, ...}, ...]
    - Price changes: {market, price_changes: [{asset_id, price, side, ...}]}
    - Last trade: {market, last_trade_price: ...}
    - Other events with event_type field
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

    async def on_message(self, data: Any) -> None:
        """Route messages to the appropriate writer based on message shape.

        Handles both list (book snapshots) and dict (event updates) formats.
        """
        # Book snapshot: server sends a list of orderbook entries
        if isinstance(data, list):
            if data:  # skip empty []
                writer = self._writers.get("book", self._writer)
                await writer.write_batch(data)
            return

        if not isinstance(data, dict):
            return

        # Detect event type from message shape
        event_type = self._detect_event_type(data)
        writer_key = self._route_event(event_type)

        if writer_key in self._writers:
            await self._writers[writer_key].write(data)
        else:
            await self._writer.write(data)

    @staticmethod
    def _detect_event_type(data: dict[str, Any]) -> str:
        """Infer event type from message keys."""
        if "event_type" in data:
            return data["event_type"]
        if "price_changes" in data:
            return "price_change"
        if "last_trade_price" in data:
            return "last_trade_price"
        if "bids" in data or "asks" in data:
            return "book"
        if "type" in data:
            return data["type"]
        return "unknown"

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
