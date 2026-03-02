"""Gamma API client — events, markets, tags, series, sports."""

from __future__ import annotations

from typing import Any

import aiohttp
import structlog

from pm_fetcher.clients.base_http import BaseHttpClient
from pm_fetcher.clients.rate_limiter import TokenBucket

log = structlog.get_logger()


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
        """Fetch a page of open markets."""
        return await self._get(
            "/markets",
            params={"limit": limit, "offset": offset, "closed": "false"},
        )

    async def get_all_markets(self, max_pages: int = 500) -> list[dict[str, Any]]:
        """Paginate through all open markets.

        Args:
            max_pages: Safety limit on number of pages to fetch.
        """
        return await self._get_all(
            "/markets",
            params={"closed": "false"},
            max_pages=max_pages,
        )

    async def get_markets_page(
        self, limit: int = 100, offset: int = 0, *, closed: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a single page of markets with optional closed filter.

        Args:
            closed: "true", "false", or None (no filter = all markets).
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if closed is not None:
            params["closed"] = closed
        return await self._get("/markets", params=params)

    async def get_events(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """Fetch a page of open events."""
        return await self._get(
            "/events",
            params={"limit": limit, "offset": offset, "closed": "false"},
        )

    async def get_all_events(self, max_pages: int = 500) -> list[dict[str, Any]]:
        """Paginate through all open events."""
        return await self._get_all(
            "/events",
            params={"closed": "false"},
            max_pages=max_pages,
        )

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
        """Fetch all series metadata (strips embedded events to save space)."""
        result = await self._get("/series")
        if not isinstance(result, list):
            return []
        # Strip the massive nested 'events' field — we fetch events separately
        for item in result:
            if isinstance(item, dict):
                item.pop("events", None)
        return result

    async def get_sports(self) -> list[dict[str, Any]]:
        """Fetch all sports data."""
        result = await self._get("/sports")
        return result if isinstance(result, list) else []
