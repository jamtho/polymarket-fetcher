# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

from pathlib import Path

import orjson
import pytest

from pm_fetcher.state import State


@pytest.fixture
def state_path(tmp_path: Path) -> Path:
    return tmp_path / "state.json"


class TestLoadSave:
    @pytest.mark.asyncio
    async def test_starts_fresh_when_no_file(self, state_path: Path):
        """Missing state file should initialize with empty defaults."""
        state = State(state_path)
        await state.load()

        assert state.known_market_ids == []
        assert state.active_token_ids == []
        assert state.known_event_ids == []

    @pytest.mark.asyncio
    async def test_save_and_load_roundtrip(self, state_path: Path):
        """State should survive a save/load cycle."""
        state = State(state_path)
        state.known_market_ids = ["m1", "m2"]
        state.active_token_ids = ["t1", "t2", "t3"]
        state.set_last_fetch("markets", 1000.0)
        await state.save()

        state2 = State(state_path)
        await state2.load()

        assert state2.known_market_ids == ["m1", "m2"]
        assert state2.active_token_ids == ["t1", "t2", "t3"]
        assert state2.get_last_fetch("markets") == 1000.0

    @pytest.mark.asyncio
    async def test_save_creates_parent_dirs(self, tmp_path: Path):
        """Save should create parent directories if they don't exist."""
        path = tmp_path / "nested" / "dir" / "state.json"
        state = State(path)
        state.known_market_ids = ["m1"]
        await state.save()

        assert path.exists()

    @pytest.mark.asyncio
    async def test_corrupt_file_starts_fresh(self, state_path: Path):
        """Corrupt JSON should not crash — start with defaults."""
        state_path.write_bytes(b"not valid json{{{")

        state = State(state_path)
        await state.load()

        assert state.known_market_ids == []
        assert state.active_token_ids == []

    @pytest.mark.asyncio
    async def test_non_dict_file_starts_fresh(self, state_path: Path):
        """A valid JSON file that isn't a dict should be ignored."""
        state_path.write_bytes(orjson.dumps([1, 2, 3]))

        state = State(state_path)
        await state.load()

        assert state.known_market_ids == []


class TestDirtyTracking:
    @pytest.mark.asyncio
    async def test_not_dirty_initially(self, state_path: Path):
        """New state should not be dirty."""
        state = State(state_path)
        await state.save()  # should be a no-op
        assert not state_path.exists()

    @pytest.mark.asyncio
    async def test_dirty_after_market_ids(self, state_path: Path):
        """Setting market IDs should mark state as dirty."""
        state = State(state_path)
        state.known_market_ids = ["m1"]
        await state.save()
        assert state_path.exists()

    @pytest.mark.asyncio
    async def test_dirty_after_token_ids(self, state_path: Path):
        """Setting token IDs should mark state as dirty."""
        state = State(state_path)
        state.active_token_ids = ["t1"]
        await state.save()
        assert state_path.exists()

    @pytest.mark.asyncio
    async def test_dirty_after_last_fetch(self, state_path: Path):
        """Setting last_fetch should mark state as dirty."""
        state = State(state_path)
        state.set_last_fetch("key", 100.0)
        await state.save()
        assert state_path.exists()

    @pytest.mark.asyncio
    async def test_not_dirty_after_save(self, state_path: Path):
        """Save should clear the dirty flag."""
        state = State(state_path)
        state.known_market_ids = ["m1"]
        await state.save()

        # Modify the file externally to detect if save writes again
        mtime_after_first_save = state_path.stat().st_mtime_ns

        await state.save()  # should be a no-op

        assert state_path.stat().st_mtime_ns == mtime_after_first_save


class TestBackfill:
    @pytest.mark.asyncio
    async def test_backfill_defaults(self, state_path: Path):
        state = State(state_path)
        assert state.backfill_completed is False
        assert state.backfill_offset == 0

    @pytest.mark.asyncio
    async def test_backfill_roundtrip(self, state_path: Path):
        state = State(state_path)
        state.backfill_completed = True
        state.backfill_offset = 5000
        await state.save()

        state2 = State(state_path)
        await state2.load()
        assert state2.backfill_completed is True
        assert state2.backfill_offset == 5000


class TestLastFetch:
    @pytest.mark.asyncio
    async def test_missing_key_returns_none(self, state_path: Path):
        state = State(state_path)
        assert state.get_last_fetch("nonexistent") is None

    @pytest.mark.asyncio
    async def test_auto_timestamp(self, state_path: Path):
        """set_last_fetch without explicit ts should use current time."""
        state = State(state_path)
        state.set_last_fetch("key")
        ts = state.get_last_fetch("key")
        assert ts is not None
        assert isinstance(ts, float)
        assert ts > 0
