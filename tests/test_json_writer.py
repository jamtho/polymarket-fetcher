# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import orjson
import pytest

from pm_fetcher.storage.json_writer import JsonWriter


@pytest.fixture
def writer(tmp_path: Path) -> JsonWriter:
    return JsonWriter(tmp_path, "gamma/markets")


@pytest.fixture
def daily_writer(tmp_path: Path) -> JsonWriter:
    return JsonWriter(tmp_path, "gamma/tags", daily=True)


class TestWrite:
    @pytest.mark.asyncio
    async def test_writes_record_with_metadata(self, writer: JsonWriter, tmp_path: Path):
        """Each record should get _fetched_at and _source prepended."""
        await writer.write({"id": "abc", "question": "Will X happen?"})
        await writer.flush()

        files = list((tmp_path / "raw" / "gamma" / "markets").rglob("*.jsonl"))
        assert len(files) == 1

        line = files[0].read_bytes().strip()
        rec = orjson.loads(line)
        assert rec["id"] == "abc"
        assert rec["question"] == "Will X happen?"
        assert "_fetched_at" in rec
        assert isinstance(rec["_fetched_at"], float)
        assert rec["_source"] == "gamma/markets"

    @pytest.mark.asyncio
    async def test_source_override(self, writer: JsonWriter, tmp_path: Path):
        """Explicit source param should override the default."""
        await writer.write({"id": "1"}, source="custom_source")
        await writer.flush()

        files = list((tmp_path / "raw" / "gamma" / "markets").rglob("*.jsonl"))
        rec = orjson.loads(files[0].read_bytes().strip())
        assert rec["_source"] == "custom_source"

    @pytest.mark.asyncio
    async def test_multiple_writes_append(self, writer: JsonWriter, tmp_path: Path):
        """Multiple writes in the same hour should append to the same file."""
        await writer.write({"id": "1"})
        await writer.write({"id": "2"})
        await writer.write({"id": "3"})
        await writer.flush()

        files = list((tmp_path / "raw" / "gamma" / "markets").rglob("*.jsonl"))
        assert len(files) == 1

        lines = files[0].read_bytes().strip().split(b"\n")
        assert len(lines) == 3


class TestWriteBatch:
    @pytest.mark.asyncio
    async def test_batch_writes_all_records(self, writer: JsonWriter, tmp_path: Path):
        """write_batch should write all records with the same timestamp."""
        records = [{"id": str(i)} for i in range(5)]
        await writer.write_batch(records)
        await writer.flush()

        files = list((tmp_path / "raw" / "gamma" / "markets").rglob("*.jsonl"))
        lines = files[0].read_bytes().strip().split(b"\n")
        assert len(lines) == 5

        # All records should share the same _fetched_at
        timestamps = {orjson.loads(l)["_fetched_at"] for l in lines}
        assert len(timestamps) == 1


class TestRotation:
    @pytest.mark.asyncio
    async def test_hourly_rotation_label(self, writer: JsonWriter, tmp_path: Path):
        """Hourly writer should create files named like YYYY-MM-DDTHH.jsonl."""
        await writer.write({"id": "1"})

        files = list((tmp_path / "raw" / "gamma" / "markets").rglob("*.jsonl"))
        assert len(files) == 1
        # Stem should be like "2026-03-06T21"
        stem = files[0].stem
        assert "T" in stem
        assert len(stem) == 13  # YYYY-MM-DDTHH

    @pytest.mark.asyncio
    async def test_daily_rotation_label(self, daily_writer: JsonWriter, tmp_path: Path):
        """Daily writer should create files named like YYYY-MM-DD.jsonl."""
        await daily_writer.write({"id": "1"})

        files = list((tmp_path / "raw" / "gamma" / "tags").rglob("*.jsonl"))
        assert len(files) == 1
        stem = files[0].stem
        assert "T" not in stem
        assert len(stem) == 10  # YYYY-MM-DD

    @pytest.mark.asyncio
    async def test_rotation_on_label_change(self, tmp_path: Path):
        """Changing the hour label should create a new file."""
        writer = JsonWriter(tmp_path, "test")

        # Write with one label
        with patch("pm_fetcher.storage.json_writer.hour_label", return_value="2026-03-01T10"):
            with patch("pm_fetcher.storage.json_writer.utc_now"):
                await writer.write({"id": "1"})

        # Write with a different label
        with patch("pm_fetcher.storage.json_writer.hour_label", return_value="2026-03-01T11"):
            with patch("pm_fetcher.storage.json_writer.utc_now"):
                await writer.write({"id": "2"})

        files = list((tmp_path / "raw" / "test").rglob("*.jsonl"))
        assert len(files) == 2
        stems = sorted(f.stem for f in files)
        assert stems == ["2026-03-01T10", "2026-03-01T11"]


class TestCloseAndFlush:
    @pytest.mark.asyncio
    async def test_close_allows_reopen(self, writer: JsonWriter, tmp_path: Path):
        """After close, the next write should open a new file handle."""
        await writer.write({"id": "1"})
        await writer.close()
        assert writer.current_file is None

        await writer.write({"id": "2"})
        await writer.flush()
        files = list((tmp_path / "raw" / "gamma" / "markets").rglob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_bytes().strip().split(b"\n")
        assert len(lines) == 2

    @pytest.mark.asyncio
    async def test_flush_without_file_is_safe(self, writer: JsonWriter):
        """Flushing before any writes should not raise."""
        await writer.flush()

    @pytest.mark.asyncio
    async def test_close_without_file_is_safe(self, writer: JsonWriter):
        """Closing before any writes should not raise."""
        await writer.close()
