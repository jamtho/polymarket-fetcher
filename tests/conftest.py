# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from pm_fetcher.storage.json_writer import JsonWriter


@pytest.fixture
def tmp_writer(tmp_path: Path) -> JsonWriter:
    """A real JsonWriter pointed at a temp directory."""
    return JsonWriter(tmp_path, "test")


@pytest.fixture
def mock_writer() -> AsyncMock:
    """A mock that quacks like a JsonWriter."""
    w = AsyncMock(spec=JsonWriter)
    w.write = AsyncMock()
    w.write_batch = AsyncMock()
    return w


@pytest.fixture
def mock_ws() -> AsyncMock:
    """A mock aiohttp WebSocketResponse."""
    ws = AsyncMock()
    ws.closed = False
    ws.send_str = AsyncMock()
    ws.ping = AsyncMock()
    ws.pong = AsyncMock()
    return ws
