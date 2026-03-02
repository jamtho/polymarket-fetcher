# Copyright (c) 2026 James Thompson. All rights reserved.

"""Pydantic Settings — all tunables with env/YAML override."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class PollerIntervals(BaseSettings):
    """Min/max polling intervals in seconds."""

    market_discovery: float = 300  # 5 min
    market_discovery_max: float = 600

    clob_prices: float = 30
    clob_prices_max: float = 120

    clob_books: float = 120  # 2 min
    clob_books_max: float = 300

    data_trades: float = 60
    data_trades_max: float = 300

    data_oi_holders: float = 600  # 10 min (used for holders)
    data_oi_holders_max: float = 1800

    metadata: float = 3600  # 1 hour
    metadata_max: float = 7200


class RateLimitConfig(BaseSettings):
    """Token-bucket rate limit settings per endpoint group."""

    gamma_rps: float = 5.0
    clob_rps: float = 10.0
    data_api_rps: float = 5.0


class WebSocketConfig(BaseSettings):
    """WebSocket connection settings."""

    market_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sports_url: str = "wss://sports-api.polymarket.com/ws"
    rtds_url: str = "wss://ws-live-data.polymarket.com"

    market_ping_interval: float = 8.0
    sports_ping_interval: float = 8.0
    rtds_ping_interval: float = 4.0

    reconnect_base: float = 1.0
    reconnect_max: float = 60.0
    reconnect_jitter: float = 0.2
    max_consecutive_failures: int = 5

    subscription_batch_size: int = 200
    queue_size: int = 10_000


class StorageConfig(BaseSettings):
    """Storage and compaction settings."""

    rotation_hours: int = 1  # JSONL hourly rotation
    compaction_interval: float = 900  # 15 min
    raw_retention_hours: int = 48
    parquet_retention_days: int = 365
    parquet_compression: str = "zstd"


class GammaApiConfig(BaseSettings):
    """Gamma API settings."""

    base_url: str = "https://gamma-api.polymarket.com"


class ClobApiConfig(BaseSettings):
    """CLOB API settings."""

    base_url: str = "https://clob.polymarket.com"


class DataApiConfig(BaseSettings):
    """Data API settings."""

    base_url: str = "https://data-api.polymarket.com"


class Settings(BaseSettings):
    """Top-level application settings."""

    model_config = {"env_prefix": "PM_", "env_nested_delimiter": "__"}

    data_dir: Path = Field(default=Path("data"))
    state_file: Path = Field(default=Path("state.json"))
    config_file: Path = Field(default=Path("config.yaml"))
    log_level: str = "INFO"

    pollers: PollerIntervals = Field(default_factory=PollerIntervals)
    rate_limits: RateLimitConfig = Field(default_factory=RateLimitConfig)
    websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    gamma: GammaApiConfig = Field(default_factory=GammaApiConfig)
    clob: ClobApiConfig = Field(default_factory=ClobApiConfig)
    data_api: DataApiConfig = Field(default_factory=DataApiConfig)

    # Active market limit for CLOB polling
    clob_top_n_markets: int = 100

    @model_validator(mode="before")
    @classmethod
    def load_yaml(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Merge config.yaml values (env vars take precedence)."""
        config_path = Path(values.get("config_file", "config.yaml"))
        if config_path.exists():
            with open(config_path) as f:
                yaml_data = yaml.safe_load(f) or {}
            # YAML provides defaults; explicit values override
            for key, val in yaml_data.items():
                if key not in values or values[key] is None:
                    values[key] = val
        return values
