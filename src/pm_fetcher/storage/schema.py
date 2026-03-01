"""Polars schema definitions for Parquet compaction.

Schemas are intentionally permissive — we store the raw JSON fields
and only enforce types on known columns. Unknown columns are kept as-is
via polars' schema inference on the JSONL.
"""

from __future__ import annotations

import polars as pl

# Common metadata columns added by JsonWriter
META_COLUMNS = {
    "_fetched_at": pl.Float64,
    "_source": pl.Utf8,
}

# Gamma markets — key fields
GAMMA_MARKETS = {
    **META_COLUMNS,
    "id": pl.Utf8,
    "question": pl.Utf8,
    "conditionId": pl.Utf8,
    "slug": pl.Utf8,
    "active": pl.Boolean,
    "closed": pl.Boolean,
    "volume": pl.Float64,
    "liquidity": pl.Float64,
    "startDate": pl.Utf8,
    "endDate": pl.Utf8,
    "clobTokenIds": pl.Utf8,  # JSON array stored as string
}

# Gamma events
GAMMA_EVENTS = {
    **META_COLUMNS,
    "id": pl.Utf8,
    "title": pl.Utf8,
    "slug": pl.Utf8,
    "active": pl.Boolean,
    "closed": pl.Boolean,
    "volume": pl.Float64,
    "liquidity": pl.Float64,
    "startDate": pl.Utf8,
    "endDate": pl.Utf8,
}

# CLOB prices
CLOB_PRICES = {
    **META_COLUMNS,
    "token_id": pl.Utf8,
    "price": pl.Float64,
    "midpoint": pl.Float64,
}

# CLOB orderbook snapshot
CLOB_BOOKS = {
    **META_COLUMNS,
    "asset_id": pl.Utf8,
    "market": pl.Utf8,
    "bids": pl.Utf8,  # JSON array as string
    "asks": pl.Utf8,
    "hash": pl.Utf8,
}

# Data API trades
DATA_TRADES = {
    **META_COLUMNS,
    "id": pl.Utf8,
    "market": pl.Utf8,
    "asset_id": pl.Utf8,
    "side": pl.Utf8,
    "size": pl.Float64,
    "price": pl.Float64,
    "timestamp": pl.Utf8,
}

# Lookup from stream name to schema hint
SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "gamma/markets": GAMMA_MARKETS,
    "gamma/events": GAMMA_EVENTS,
    "clob/prices": CLOB_PRICES,
    "clob/books": CLOB_BOOKS,
    "data_api/trades": DATA_TRADES,
}
