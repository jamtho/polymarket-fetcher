"""CLOB poller — price/midpoint snapshots and orderbook for active markets."""

from __future__ import annotations

import structlog

from pm_fetcher.clients.clob import ClobClient
from pm_fetcher.pollers.base_poller import BasePoller
from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter

log = structlog.get_logger()


class ClobPricePoller(BasePoller):
    """Polls CLOB prices and midpoints for active tokens."""

    name = "clob_prices"

    def __init__(
        self,
        clob_client: ClobClient,
        writer: JsonWriter,
        state: State,
        *,
        min_interval: float = 30.0,
        max_interval: float = 120.0,
    ) -> None:
        super().__init__(writer, state, min_interval=min_interval, max_interval=max_interval)
        self._clob = clob_client

    async def poll_once(self) -> bool:
        token_ids = self._state.active_token_ids
        if not token_ids:
            log.debug("no active tokens for price polling")
            return False

        prices = await self._clob.get_prices(token_ids)
        midpoints = await self._clob.get_midpoints(token_ids)

        # Merge prices and midpoints by token_id
        mid_map = {m["token_id"]: m for m in midpoints if "token_id" in m}
        records = []
        for p in prices:
            tid = p.get("token_id", "")
            record = {"token_id": tid, "price": p.get("price")}
            if tid in mid_map:
                record["midpoint"] = mid_map[tid].get("mid")
            records.append(record)

        if records:
            await self._writer.write_batch(records)
            return True
        return False


class ClobBookPoller(BasePoller):
    """Polls CLOB orderbooks for top-N active tokens."""

    name = "clob_books"

    def __init__(
        self,
        clob_client: ClobClient,
        writer: JsonWriter,
        state: State,
        *,
        top_n: int = 100,
        min_interval: float = 120.0,
        max_interval: float = 300.0,
    ) -> None:
        super().__init__(writer, state, min_interval=min_interval, max_interval=max_interval)
        self._clob = clob_client
        self._top_n = top_n

    async def poll_once(self) -> bool:
        token_ids = self._state.active_token_ids[: self._top_n]
        if not token_ids:
            log.debug("no active tokens for book polling")
            return False

        books = await self._clob.get_books(token_ids)
        if books:
            await self._writer.write_batch(books)
            return True
        return False
