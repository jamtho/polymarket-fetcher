"""JSONL -> Parquet compaction via polars.

Runs periodically to convert closed JSONL files (past the current hour
boundary) into Hive-partitioned Parquet with zstd compression.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import polars as pl
import structlog

from pm_fetcher.config import StorageConfig
from pm_fetcher.utils.clock import utc_now

log = structlog.get_logger()


class Compactor:
    """Converts completed JSONL files to Parquet and cleans up old data."""

    def __init__(self, data_dir: Path, config: StorageConfig) -> None:
        self._raw_dir = data_dir / "raw"
        self._parquet_dir = data_dir / "parquet"
        self._config = config

    async def run_once(self) -> int:
        """Compact all eligible JSONL files. Returns count of files compacted."""
        count = 0
        now = utc_now()
        current_hour_label = now.strftime("%Y-%m-%dT%H")
        current_day_label = now.strftime("%Y-%m-%d")

        if not self._raw_dir.exists():
            return 0

        for jsonl_path in sorted(self._raw_dir.rglob("*.jsonl")):
            label = jsonl_path.stem  # e.g. "2026-03-01T15" or "2026-03-01"
            # Skip the currently-active file
            if label == current_hour_label or label == current_day_label:
                continue
            # Skip empty files
            if jsonl_path.stat().st_size == 0:
                jsonl_path.unlink(missing_ok=True)
                continue

            try:
                await self._compact_file(jsonl_path)
                count += 1
            except Exception:
                log.exception("compaction failed", file=str(jsonl_path))

        return count

    async def _compact_file(self, jsonl_path: Path) -> None:
        """Convert a single JSONL file to Parquet."""
        # Determine the stream path relative to raw/
        rel = jsonl_path.relative_to(self._raw_dir)
        stream_parts = rel.parent.parts  # e.g. ("gamma", "markets")
        label = jsonl_path.stem

        # Parse date/hour from label
        if "T" in label:
            dt_part = label[:10]
            hour_part = label[11:]
        else:
            dt_part = label
            hour_part = "00"

        # Build output path: data/parquet/gamma/markets/dt=2026-03-01/hour=15.parquet
        out_dir = self._parquet_dir / Path(*stream_parts) / f"dt={dt_part}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"hour={hour_part}.parquet"

        # Read and convert in a thread to not block the event loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._do_compact, jsonl_path, out_path)

        # Remove original after successful compaction
        jsonl_path.unlink(missing_ok=True)
        log.info("compacted", src=str(jsonl_path), dst=str(out_path))

    def _do_compact(self, src: Path, dst: Path) -> None:
        """Synchronous compaction (runs in thread executor)."""
        df = pl.read_ndjson(src, ignore_errors=True)
        if df.is_empty():
            return
        df.write_parquet(dst, compression=self._config.parquet_compression)

    async def cleanup_old_files(self) -> None:
        """Remove raw JSONL older than retention and old Parquet."""
        now = utc_now()

        # Clean raw JSONL
        raw_cutoff = now - timedelta(hours=self._config.raw_retention_hours)
        if self._raw_dir.exists():
            for f in self._raw_dir.rglob("*.jsonl"):
                try:
                    mtime = f.stat().st_mtime
                    from datetime import datetime, timezone
                    file_time = datetime.fromtimestamp(mtime, tz=timezone.utc)
                    if file_time < raw_cutoff:
                        f.unlink(missing_ok=True)
                        log.debug("cleaned raw", file=str(f))
                except Exception:
                    log.exception("cleanup error", file=str(f))

        # Clean old Parquet
        parquet_cutoff = now - timedelta(days=self._config.parquet_retention_days)
        if self._parquet_dir.exists():
            for f in self._parquet_dir.rglob("*.parquet"):
                try:
                    mtime = f.stat().st_mtime
                    from datetime import datetime, timezone
                    file_time = datetime.fromtimestamp(mtime, tz=timezone.utc)
                    if file_time < parquet_cutoff:
                        f.unlink(missing_ok=True)
                        log.debug("cleaned parquet", file=str(f))
                except Exception:
                    log.exception("cleanup error", file=str(f))

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Compaction loop — runs until stop_event is set."""
        while not stop_event.is_set():
            try:
                n = await self.run_once()
                if n:
                    log.info("compaction pass done", files_compacted=n)
                await self.cleanup_old_files()
            except Exception:
                log.exception("compaction loop error")

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._config.compaction_interval
                )
                break
            except asyncio.TimeoutError:
                pass
