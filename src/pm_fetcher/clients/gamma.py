"""Gamma API client — events, markets, tags, series, sports."""

from __future__ import annotations

from typing import Any

import aiohttp

from pm_fetcher.clients.base_http import BaseHttpClient
from pm_fetcher.clients.rate_limiter import TokenBucket


class GammaClient(BaseHttpClient):
    """Client for the Gamma API (https://gamma-api.polymarket.com)."""

    _group_name = "gamma"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        rate_limiter: TokenBucket,
        base_url: str = "https://gamma-api.polymarket.com",
    ) -> None:
        super().__init__(session, rate_limiter)
        self._base_url = base_url

    async def get_markets(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """Fetch a page of markets."""
        return await self._get(
            "/markets",
            params={"limit": limit, "offset": offset, "active": "true"},
        )

    async def get_all_markets(self) -> list[dict[str, Any]]:
        """Paginate through all active markets."""
        return await self._get_all("/markets", params={"active": "true"})

    async def get_events(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """Fetch a page of events."""
        return await self._get(
            "/events",
            params={"limit": limit, "offset": offset, "active": "true"},
        )

    async def get_all_events(self) -> list[dict[str, Any]]:
        """Paginate through all active events."""
        return await self._get_all("/events", params={"active": "true"})

    async def get_market(self, market_id: str) -> dict[str, Any]:
        """Fetch a single market by ID."""
        return await self._get(f"/markets/{market_id}")

    async def get_event(self, event_id: str) -> dict[str, Any]:
        """Fetch a single event by ID."""
        return await self._get(f"/events/{event_id}")

    async def get_tags(self) -> list[dict[str, Any]]:
        """Fetch all tags."""
        return await self._get("/tags")

    async def get_series(self) -> list[dict[str, Any]]:
        """Fetch all series."""
        result = await self._get("/series")
        return result if isinstance(result, list) else []

    async def get_sports(self) -> list[dict[str, Any]]:
        """Fetch all sports data."""
        result = await self._get("/sports")
        return result if isinstance(result, list) else []
