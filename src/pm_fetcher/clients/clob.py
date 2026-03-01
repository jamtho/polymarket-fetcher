"""CLOB API client — price, book, midpoint, spread."""

from __future__ import annotations

from typing import Any

import aiohttp

from pm_fetcher.clients.base_http import BaseHttpClient
from pm_fetcher.clients.rate_limiter import TokenBucket


class ClobClient(BaseHttpClient):
    """Client for the CLOB API (https://clob.polymarket.com)."""

    _group_name = "clob"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        rate_limiter: TokenBucket,
        base_url: str = "https://clob.polymarket.com",
    ) -> None:
        super().__init__(session, rate_limiter)
        self._base_url = base_url

    async def get_price(self, token_id: str) -> dict[str, Any]:
        """Get the last trade price for a token."""
        return await self._get("/price", params={"token_id": token_id})

    async def get_prices(self, token_ids: list[str]) -> list[dict[str, Any]]:
        """Get prices for multiple tokens."""
        results = []
        for tid in token_ids:
            try:
                price = await self.get_price(tid)
                results.append({"token_id": tid, **price})
            except Exception:
                pass
        return results

    async def get_midpoint(self, token_id: str) -> dict[str, Any]:
        """Get the midpoint price for a token."""
        return await self._get("/midpoint", params={"token_id": token_id})

    async def get_spread(self, token_id: str) -> dict[str, Any]:
        """Get the bid-ask spread for a token."""
        return await self._get("/spread", params={"token_id": token_id})

    async def get_book(self, token_id: str) -> dict[str, Any]:
        """Get the full orderbook for a token."""
        return await self._get("/book", params={"token_id": token_id})

    async def get_midpoints(self, token_ids: list[str]) -> list[dict[str, Any]]:
        """Get midpoints for multiple tokens."""
        results = []
        for tid in token_ids:
            try:
                mid = await self.get_midpoint(tid)
                results.append({"token_id": tid, **mid})
            except Exception:
                pass
        return results

    async def get_books(self, token_ids: list[str]) -> list[dict[str, Any]]:
        """Get orderbooks for multiple tokens."""
        results = []
        for tid in token_ids:
            try:
                book = await self.get_book(tid)
                results.append({"asset_id": tid, **book})
            except Exception:
                pass
        return results
