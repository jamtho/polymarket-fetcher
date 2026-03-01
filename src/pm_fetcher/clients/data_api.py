"""Data API client — trades, holders."""

from __future__ import annotations

from typing import Any

import aiohttp

from pm_fetcher.clients.base_http import BaseHttpClient
from pm_fetcher.clients.rate_limiter import TokenBucket


class DataApiClient(BaseHttpClient):
    """Client for the Data API (https://data-api.polymarket.com)."""

    _group_name = "data_api"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        rate_limiter: TokenBucket,
        base_url: str = "https://data-api.polymarket.com",
    ) -> None:
        super().__init__(session, rate_limiter)
        self._base_url = base_url

    async def get_trades(
        self,
        market: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch recent trades, optionally filtered by market conditionId."""
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if market:
            params["market"] = market
        result = await self._get("/trades", params=params)
        return result if isinstance(result, list) else []

    async def get_holders(self, condition_id: str) -> list[dict[str, Any]]:
        """Get top holders for a market's tokens.

        Returns a list of token holder records with amounts.
        """
        result = await self._get("/holders", params={"market": condition_id})
        return result if isinstance(result, list) else []

    async def get_market_trades(self, condition_id: str, limit: int = 100) -> list[dict[str, Any]]:
        """Fetch trades for a specific market."""
        result = await self._get(
            "/trades",
            params={"market": condition_id, "limit": limit},
        )
        return result if isinstance(result, list) else []
