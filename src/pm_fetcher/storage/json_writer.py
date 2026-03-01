"""Append-only JSONL writer with time-based file rotation."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import orjson
import structlog

from pm_fetcher.utils.clock import hour_label, unix_now, utc_now

log = structlog.get_logger()


class JsonWriter:
    """Writes records as JSONL with automatic hourly (or daily) rotation.

    Each record gets `_fetched_at` and `_source` metadata prepended.
    Files are named like `data/raw/gamma/markets/2026-03-01T15.jsonl`.
    """

    def __init__(
        self,
        base_dir: Path,
        stream_name: str,
        *,
        daily: bool = False,
    ) -> None:
        self._base_dir = base_dir / "raw" / stream_name
        self._daily = daily
        self._stream_name = stream_name
        self._current_label: str | None = None
        self._file: Any = None
        self._lock = asyncio.Lock()

    def _make_label(self) -> str:
        now = utc_now()
        if self._daily:
            return now.strftime("%Y-%m-%d")
        return hour_label(now)

    def _file_path(self, label: str) -> Path:
        return self._base_dir / f"{label}.jsonl"

    async def _ensure_file(self, label: str) -> None:
        """Open a new file if the rotation label has changed."""
        if label == self._current_label and self._file is not None:
            return

        if self._file is not None:
            self._file.close()

        path = self._file_path(label)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(path, "ab")  # noqa: SIM115, ASYNC230
        self._current_label = label
        log.debug("rotated jsonl", stream=self._stream_name, file=str(path))

    async def write(self, record: dict[str, Any], source: str | None = None) -> None:
        """Write a single record to the current JSONL file."""
        record = {"_fetched_at": unix_now(), "_source": source or self._stream_name, **record}
        line = orjson.dumps(record) + b"\n"

        async with self._lock:
            label = self._make_label()
            await self._ensure_file(label)
            self._file.write(line)

    async def write_batch(self, records: list[dict[str, Any]], source: str | None = None) -> None:
        """Write multiple records atomically."""
        ts = unix_now()
        src = source or self._stream_name
        lines = b"".join(
            orjson.dumps({"_fetched_at": ts, "_source": src, **r}) + b"\n"
            for r in records
        )

        async with self._lock:
            label = self._make_label()
            await self._ensure_file(label)
            self._file.write(lines)

    async def flush(self) -> None:
        """Flush the current file to disk."""
        async with self._lock:
            if self._file is not None:
                self._file.flush()

    async def close(self) -> None:
        """Close the current file."""
        async with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None
                self._current_label = None

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    @property
    def current_file(self) -> Path | None:
        if self._current_label is not None:
            return self._file_path(self._current_label)
        return None
