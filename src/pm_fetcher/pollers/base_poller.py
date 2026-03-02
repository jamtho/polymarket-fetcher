# Copyright (c) 2026 James Thompson. All rights reserved.

"""Base poller — scheduling loop with adaptive intervals."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

import structlog

from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter

log = structlog.get_logger()


class BasePoller(ABC):
    """Base class for all pollers.

    Subclasses implement `poll_once()` which returns True if data changed.
    The interval adapts: doubles on no-change, resets on change.
    """

    name: str = "base"

    def __init__(
        self,
        writer: JsonWriter,
        state: State,
        *,
        min_interval: float = 60.0,
        max_interval: float = 300.0,
    ) -> None:
        self._writer = writer
        self._state = state
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._current_interval = min_interval

    @abstractmethod
    async def poll_once(self) -> bool:
        """Execute one poll cycle. Return True if data changed."""
        ...

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run the polling loop until stop_event is set."""
        log.info("poller starting", poller=self.name, interval=self._current_interval)

        while not stop_event.is_set():
            changed = False
            try:
                changed = await self.poll_once()
                self._state.set_last_fetch(self.name)
            except Exception:
                log.exception("poll error", poller=self.name)

            # Adaptive interval
            if changed:
                self._current_interval = self._min_interval
            else:
                self._current_interval = min(
                    self._current_interval * 2, self._max_interval
                )

            log.debug(
                "poll cycle done",
                poller=self.name,
                changed=changed,
                next_interval=self._current_interval,
            )

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._current_interval
                )
                break
            except asyncio.TimeoutError:
                pass

        log.info("poller stopped", poller=self.name)
