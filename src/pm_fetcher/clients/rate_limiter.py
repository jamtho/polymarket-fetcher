# Copyright (c) 2026 James Thompson. All rights reserved.

"""Async token-bucket rate limiter per endpoint group."""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async token-bucket rate limiter.

    Allows `rate` requests per second with burst capacity equal to `rate`.
    When a 429 is received, `pause()` can be called to block all requests
    for a cooldown period.
    """

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._max_tokens = rate
        self._tokens = rate
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._paused_until: float = 0.0

    async def acquire(self) -> None:
        """Wait until a token is available, then consume one."""
        while True:
            async with self._lock:
                now = time.monotonic()

                # Respect pause (from 429 responses)
                if now < self._paused_until:
                    wait = self._paused_until - now
                    # Release lock while waiting
                else:
                    # Refill tokens
                    elapsed = now - self._last_refill
                    self._tokens = min(
                        self._max_tokens, self._tokens + elapsed * self._rate
                    )
                    self._last_refill = now

                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return

                    wait = (1.0 - self._tokens) / self._rate

            await asyncio.sleep(wait)

    def pause(self, seconds: float = 5.0) -> None:
        """Pause all requests for `seconds` (called on 429)."""
        self._paused_until = max(
            self._paused_until, time.monotonic() + seconds
        )


class RateLimiterGroup:
    """Collection of named rate limiters for different endpoint groups."""

    def __init__(self) -> None:
        self._buckets: dict[str, TokenBucket] = {}

    def add(self, name: str, rate: float) -> None:
        self._buckets[name] = TokenBucket(rate)

    def get(self, name: str) -> TokenBucket:
        return self._buckets[name]

    def pause(self, name: str, seconds: float = 5.0) -> None:
        if name in self._buckets:
            self._buckets[name].pause(seconds)
