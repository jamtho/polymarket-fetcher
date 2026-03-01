"""UTC time helpers and rotation boundary calculations."""

from __future__ import annotations

import time
from datetime import datetime, timezone


def utc_now() -> datetime:
    """Current UTC datetime."""
    return datetime.now(timezone.utc)


def unix_now() -> float:
    """Current Unix timestamp (UTC)."""
    return time.time()


def hour_boundary(dt: datetime) -> datetime:
    """Truncate datetime to the start of its hour."""
    return dt.replace(minute=0, second=0, microsecond=0)


def day_boundary(dt: datetime) -> datetime:
    """Truncate datetime to the start of its day."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def hour_label(dt: datetime) -> str:
    """Format as '2026-03-01T15' for hourly file rotation."""
    return dt.strftime("%Y-%m-%dT%H")


def day_label(dt: datetime) -> str:
    """Format as '2026-03-01' for daily file rotation."""
    return dt.strftime("%Y-%m-%d")


def next_hour(dt: datetime) -> datetime:
    """Return the start of the next hour after dt."""
    h = hour_boundary(dt)
    return h.replace(hour=h.hour + 1) if h.hour < 23 else (
        h.replace(day=h.day + 1, hour=0)
    )
