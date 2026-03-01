"""Persistent state — known markets, last-fetch timestamps.

State is saved as a JSON file and loaded on startup for resumption.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import orjson
import structlog

from pm_fetcher.utils.clock import unix_now

log = structlog.get_logger()


class State:
    """Thread-safe persistent state manager."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._data: dict[str, Any] = {
            "known_market_ids": [],
            "known_event_ids": [],
            "active_token_ids": [],
            "last_fetch": {},
        }
        self._dirty = False

    async def load(self) -> None:
        """Load state from disk. Start fresh if missing or corrupt."""
        if not self._path.exists():
            log.info("no state file, starting fresh", path=str(self._path))
            return

        try:
            raw = self._path.read_bytes()
            loaded = orjson.loads(raw)
            if isinstance(loaded, dict):
                self._data.update(loaded)
                log.info(
                    "state loaded",
                    markets=len(self._data.get("known_market_ids", [])),
                    tokens=len(self._data.get("active_token_ids", [])),
                )
            else:
                log.warning("state file has unexpected format, starting fresh")
        except Exception:
            log.exception("failed to load state, starting fresh")

    async def save(self) -> None:
        """Persist state to disk."""
        async with self._lock:
            if not self._dirty:
                return
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._path.write_bytes(orjson.dumps(self._data, option=orjson.OPT_INDENT_2))
                self._dirty = False
                log.debug("state saved")
            except Exception:
                log.exception("failed to save state")

    @property
    def known_market_ids(self) -> list[str]:
        return self._data.get("known_market_ids", [])

    @known_market_ids.setter
    def known_market_ids(self, value: list[str]) -> None:
        self._data["known_market_ids"] = value
        self._dirty = True

    @property
    def known_event_ids(self) -> list[str]:
        return self._data.get("known_event_ids", [])

    @known_event_ids.setter
    def known_event_ids(self, value: list[str]) -> None:
        self._data["known_event_ids"] = value
        self._dirty = True

    @property
    def active_token_ids(self) -> list[str]:
        return self._data.get("active_token_ids", [])

    @active_token_ids.setter
    def active_token_ids(self, value: list[str]) -> None:
        self._data["active_token_ids"] = value
        self._dirty = True

    def get_last_fetch(self, key: str) -> float | None:
        return self._data.get("last_fetch", {}).get(key)

    def set_last_fetch(self, key: str, ts: float | None = None) -> None:
        if "last_fetch" not in self._data:
            self._data["last_fetch"] = {}
        self._data["last_fetch"][key] = ts or unix_now()
        self._dirty = True
