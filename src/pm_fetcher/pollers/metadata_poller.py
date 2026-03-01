"""Metadata poller — tags, series, sports (slow-changing reference data)."""

from __future__ import annotations

import structlog

from pm_fetcher.clients.gamma import GammaClient
from pm_fetcher.pollers.base_poller import BasePoller
from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter

log = structlog.get_logger()


class MetadataPoller(BasePoller):
    """Polls tags, series, and sports from the Gamma API.

    These are near-static reference data, polled infrequently (1h default).
    Each type gets its own JSONL writer.
    """

    name = "metadata"

    def __init__(
        self,
        gamma_client: GammaClient,
        tags_writer: JsonWriter,
        series_writer: JsonWriter,
        sports_writer: JsonWriter,
        state: State,
        *,
        min_interval: float = 3600.0,
        max_interval: float = 7200.0,
    ) -> None:
        super().__init__(tags_writer, state, min_interval=min_interval, max_interval=max_interval)
        self._gamma = gamma_client
        self._series_writer = series_writer
        self._sports_writer = sports_writer

    async def poll_once(self) -> bool:
        changed = False

        try:
            tags = await self._gamma.get_tags()
            if tags:
                if isinstance(tags, list):
                    await self._writer.write_batch(tags)
                elif isinstance(tags, dict):
                    await self._writer.write(tags)
                changed = True
        except Exception:
            log.exception("failed to fetch tags")

        try:
            series = await self._gamma.get_series()
            if series:
                await self._series_writer.write_batch(series)
                changed = True
        except Exception:
            log.exception("failed to fetch series")

        try:
            sports = await self._gamma.get_sports()
            if sports:
                await self._sports_writer.write_batch(sports)
                changed = True
        except Exception:
            log.exception("failed to fetch sports")

        return changed
