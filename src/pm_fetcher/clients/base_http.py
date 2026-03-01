"""Shared HTTP client with rate limiting, retry, and session management."""

from __future__ import annotations

from typing import Any

import aiohttp
import structlog
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from pm_fetcher.clients.rate_limiter import TokenBucket

log = structlog.get_logger()


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 429, 5xx, and connection errors."""
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status == 429 or exc.status >= 500
    if isinstance(exc, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError)):
        return True
    return False


class BaseHttpClient:
    """Shared async HTTP client with per-group rate limiting and retry.

    Subclasses set `_base_url` and `_rate_limiter` and call `_get`/`_get_all`.
    """

    _base_url: str = ""
    _rate_limiter: TokenBucket | None = None
    _group_name: str = "default"

    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucket | None = None) -> None:
        self._session = session
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True,
    )
    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make a rate-limited, retried HTTP request."""
        if self._rate_limiter is not None:
            await self._rate_limiter.acquire()

        url = f"{self._base_url}{path}"
        log.debug("http request", method=method, url=url)

        async with self._session.request(method, url, **kwargs) as resp:
            if resp.status == 429:
                if self._rate_limiter is not None:
                    retry_after = float(resp.headers.get("Retry-After", "5"))
                    self._rate_limiter.pause(retry_after)
                    log.warning("rate limited", group=self._group_name, pause=retry_after)
                resp.raise_for_status()

            resp.raise_for_status()
            return await resp.json()

    async def _get(self, path: str, **kwargs: Any) -> Any:
        return await self._request("GET", path, **kwargs)

    async def _get_all(
        self,
        path: str,
        *,
        limit: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Paginate through a list endpoint, returning all results."""
        all_results: list[dict[str, Any]] = []
        offset = 0
        params = kwargs.pop("params", {})

        while True:
            page_params = {**params, limit_param: limit, offset_param: offset}
            data = await self._get(path, params=page_params, **kwargs)

            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Try common response shapes
                items = data.get("data", data.get("results", data.get("items", [])))
                if not isinstance(items, list):
                    items = [data]
            else:
                break

            all_results.extend(items)

            if len(items) < limit:
                break
            offset += limit

        return all_results
