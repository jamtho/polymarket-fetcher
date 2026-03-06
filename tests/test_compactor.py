# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import orjson
import polars as pl
import pytest

from pm_fetcher.config import StorageConfig
from pm_fetcher.storage.compactor import Compactor


def _write_jsonl(path: Path, records: list[dict]) -> None:
    """Helper to write JSONL test data."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for r in records:
            f.write(orjson.dumps(r) + b"\n")


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def compactor(data_dir: Path) -> Compactor:
    return Compactor(data_dir, StorageConfig())


@pytest.fixture
def frozen_now() -> datetime:
    """A fixed 'now' for deterministic tests — 2026-03-06T15:30:00 UTC."""
    return datetime(2026, 3, 6, 15, 30, 0, tzinfo=timezone.utc)


class TestCompaction:
    @pytest.mark.asyncio
    async def test_compacts_closed_hour(self, data_dir: Path, compactor: Compactor, frozen_now: datetime):
        """A JSONL file from a past hour should be compacted to Parquet."""
        records = [
            {"_fetched_at": 1.0, "_source": "test", "id": "a", "value": 1},
            {"_fetched_at": 2.0, "_source": "test", "id": "b", "value": 2},
        ]
        jsonl_path = data_dir / "raw" / "gamma" / "markets" / "2026-03-06T14.jsonl"
        _write_jsonl(jsonl_path, records)

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            count = await compactor.run_once()

        assert count == 1

        # JSONL should be deleted after compaction
        assert not jsonl_path.exists()

        # Parquet should exist with correct path
        parquet_path = data_dir / "parquet" / "gamma" / "markets" / "dt=2026-03-06" / "hour=14.parquet"
        assert parquet_path.exists()

        # Verify contents
        df = pl.read_parquet(parquet_path)
        assert len(df) == 2
        assert set(df["id"].to_list()) == {"a", "b"}

    @pytest.mark.asyncio
    async def test_skips_current_hour(self, data_dir: Path, compactor: Compactor, frozen_now: datetime):
        """The currently-active hour file should not be compacted."""
        jsonl_path = data_dir / "raw" / "test" / "2026-03-06T15.jsonl"
        _write_jsonl(jsonl_path, [{"id": "1"}])

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            count = await compactor.run_once()

        assert count == 0
        assert jsonl_path.exists()

    @pytest.mark.asyncio
    async def test_skips_current_day(self, data_dir: Path, compactor: Compactor, frozen_now: datetime):
        """The currently-active daily file should not be compacted."""
        jsonl_path = data_dir / "raw" / "tags" / "2026-03-06.jsonl"
        _write_jsonl(jsonl_path, [{"id": "1"}])

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            count = await compactor.run_once()

        assert count == 0
        assert jsonl_path.exists()

    @pytest.mark.asyncio
    async def test_deletes_empty_files(self, data_dir: Path, compactor: Compactor, frozen_now: datetime):
        """Empty JSONL files should be deleted without creating Parquet."""
        jsonl_path = data_dir / "raw" / "test" / "2026-03-06T14.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_path.write_bytes(b"")

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            count = await compactor.run_once()

        assert count == 0
        assert not jsonl_path.exists()
        # No parquet directory should be created
        assert not (data_dir / "parquet").exists()

    @pytest.mark.asyncio
    async def test_daily_file_compaction(self, data_dir: Path, compactor: Compactor, frozen_now: datetime):
        """Daily JSONL files should produce hour=00 Parquet."""
        jsonl_path = data_dir / "raw" / "tags" / "2026-03-05.jsonl"
        _write_jsonl(jsonl_path, [{"tag": "politics"}])

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            count = await compactor.run_once()

        assert count == 1
        parquet_path = data_dir / "parquet" / "tags" / "dt=2026-03-05" / "hour=00.parquet"
        assert parquet_path.exists()

    @pytest.mark.asyncio
    async def test_multiple_streams(self, data_dir: Path, compactor: Compactor, frozen_now: datetime):
        """Multiple streams should all be compacted in one pass."""
        _write_jsonl(
            data_dir / "raw" / "gamma" / "markets" / "2026-03-06T14.jsonl",
            [{"id": "m1"}],
        )
        _write_jsonl(
            data_dir / "raw" / "clob" / "prices" / "2026-03-06T14.jsonl",
            [{"id": "p1"}],
        )

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            count = await compactor.run_once()

        assert count == 2

    @pytest.mark.asyncio
    async def test_no_raw_dir(self, data_dir: Path, compactor: Compactor):
        """Should return 0 if raw directory doesn't exist."""
        count = await compactor.run_once()
        assert count == 0


class TestCleanup:
    @pytest.mark.asyncio
    async def test_cleans_old_raw_files(self, data_dir: Path, frozen_now: datetime):
        """Raw JSONL files older than retention should be deleted."""
        config = StorageConfig(raw_retention_hours=24)
        compactor = Compactor(data_dir, config)

        old_file = data_dir / "raw" / "test" / "old.jsonl"
        old_file.parent.mkdir(parents=True, exist_ok=True)
        old_file.write_bytes(b'{"id":"old"}\n')

        # Set mtime to 48 hours ago
        old_mtime = time.time() - 48 * 3600
        os.utime(old_file, (old_mtime, old_mtime))

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            await compactor.cleanup_old_files()

        assert not old_file.exists()

    @pytest.mark.asyncio
    async def test_keeps_recent_raw_files(self, data_dir: Path, frozen_now: datetime):
        """Raw JSONL files within retention should be kept."""
        config = StorageConfig(raw_retention_hours=48)
        compactor = Compactor(data_dir, config)

        recent_file = data_dir / "raw" / "test" / "recent.jsonl"
        recent_file.parent.mkdir(parents=True, exist_ok=True)
        recent_file.write_bytes(b'{"id":"recent"}\n')
        # Default mtime is now, which is within 48h

        with patch("pm_fetcher.storage.compactor.utc_now", return_value=frozen_now):
            await compactor.cleanup_old_files()

        assert recent_file.exists()
