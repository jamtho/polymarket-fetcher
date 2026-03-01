"""Data API pollers — trades and holders."""

from __future__ import annotations

import structlog

from pm_fetcher.clients.data_api import DataApiClient
from pm_fetcher.pollers.base_poller import BasePoller
from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter

log = structlog.get_logger()


class TradesPoller(BasePoller):
    """Polls recent trades from the Data API."""

    name = "data_trades"

    def __init__(
        self,
        data_client: DataApiClient,
        writer: JsonWriter,
        state: State,
        *,
        min_interval: float = 60.0,
        max_interval: float = 300.0,
    ) -> None:
        super().__init__(writer, state, min_interval=min_interval, max_interval=max_interval)
        self._data = data_client

    async def poll_once(self) -> bool:
        trades = await self._data.get_trades(limit=100)
        if trades:
            await self._writer.write_batch(trades)
            return True
        return False


class HoldersPoller(BasePoller):
    """Polls top holder data for known markets."""

    name = "data_holders"

    def __init__(
        self,
        data_client: DataApiClient,
        writer: JsonWriter,
        state: State,
        *,
        min_interval: float = 600.0,
        max_interval: float = 1800.0,
    ) -> None:
        super().__init__(writer, state, min_interval=min_interval, max_interval=max_interval)
        self._data = data_client

    async def poll_once(self) -> bool:
        # Use a subset of known markets to avoid excessive requests
        market_ids = self._state.known_market_ids[:50]
        if not market_ids:
            return False

        changed = False
        for mid in market_ids:
            try:
                holders = await self._data.get_holders(mid)
                if holders:
                    await self._writer.write_batch(
                        [{"condition_id": mid, **h} if isinstance(h, dict) else {"condition_id": mid, "data": h}
                         for h in holders]
                    )
                    changed = True
            except Exception:
                pass

        return changed
