# Architecture

This document describes the design and implementation of `pm-fetcher`, a long-running Python service that continuously collects data from Polymarket's public APIs and WebSocket streams, stores raw snapshots as JSONL, and compacts them into Parquet files for analysis.

## Table of Contents

- [System Overview](#system-overview)
- [Project Layout](#project-layout)
- [Data Flow](#data-flow)
- [Configuration](#configuration)
- [API Clients](#api-clients)
- [Pollers](#pollers)
- [WebSocket Streams](#websocket-streams)
- [Storage](#storage)
- [State Management](#state-management)
- [Orchestrator](#orchestrator)
- [Error Handling & Resilience](#error-handling--resilience)
- [Key Design Decisions](#key-design-decisions)

---

## System Overview

The system has five major subsystems that run concurrently as asyncio tasks:

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
│   Pollers    │────>│ JSONL Writers│────>│  data/raw/**/*.jsonl
│  (6 pollers) │     │ (15 writers) │     └──────────────────┘
└──────┬──────┘     └──────────────┘               │
       │                                           │ (closed files)
       │ state updates                             ▼
       ▼                                  ┌──────────────────┐
┌─────────────┐                           │    Compactor      │
│    State     │                           │ (JSONL → Parquet) │
│ (state.json) │                           └────────┬─────────┘
└──────┬──────┘                                     │
       │ token IDs                                  ▼
       ▼                                  ┌──────────────────┐
┌─────────────┐     ┌──────────────┐     │data/parquet/**/*.parquet
│ WebSockets  │────>│ Async Queues │────>│  (Hive-partitioned)
│ (3 streams) │     │  → Writers   │     └──────────────────┘
└─────────────┘     └──────────────┘
```

**Market Discovery** is the linchpin: it crawls all open markets from the Gamma API, extracts CLOB token IDs, and feeds them to both the CLOB pollers (as polling targets) and the Market WebSocket (as subscription IDs).

---

## Project Layout

```
src/pm_fetcher/
├── __init__.py
├── __main__.py              # python -m pm_fetcher entry point
├── main.py                  # Orchestrator — wires everything, runs TaskGroup
├── config.py                # Pydantic Settings (YAML + env override)
├── state.py                 # Persistent state (known markets, token IDs)
├── clients/
│   ├── rate_limiter.py      # Async token-bucket rate limiter
│   ├── base_http.py         # Shared HTTP client (retry, rate limit, pagination)
│   ├── gamma.py             # Gamma API (markets, events, tags, series, sports)
│   ├── clob.py              # CLOB API (price, midpoint, spread, book)
│   └── data_api.py          # Data API (trades, holders)
├── pollers/
│   ├── base_poller.py       # Abstract poller with adaptive intervals
│   ├── market_discovery.py  # Crawls Gamma for markets/events/token IDs
│   ├── clob_poller.py       # Price/midpoint and orderbook snapshots
│   ├── data_poller.py       # Trades and holder data
│   └── metadata_poller.py   # Tags, series, sports (slow-changing)
├── websockets/
│   ├── base_ws.py           # Auto-reconnect, keepalive, message queue
│   ├── market_ws.py         # Market channel with dynamic subscriptions
│   ├── sports_ws.py         # Sports channel (auto-stream, no subscription)
│   └── rtds_ws.py           # RTDS (activity feed + crypto prices)
├── storage/
│   ├── json_writer.py       # Append-only JSONL with hourly/daily rotation
│   ├── compactor.py         # JSONL → Parquet conversion + retention cleanup
│   └── schema.py            # Polars type hints for Parquet columns
└── utils/
    ├── logging.py           # structlog setup (JSON for machines, console for TTY)
    └── clock.py             # UTC helpers and file rotation boundaries
```

---

## Data Flow

### 1. Market Discovery → State → Subscriptions

```
Gamma API (/markets, /events)
     │
     ▼
MarketDiscoveryPoller
     │
     ├──> writes markets/events JSONL
     │
     └──> updates State:
            • known_market_ids
            • known_event_ids
            • active_token_ids  ──────┐
                                      │
            ┌─────────────────────────┘
            │
            ├──> ClobPricePoller reads active_token_ids for polling targets
            ├──> ClobBookPoller reads active_token_ids[:top_n] for book snapshots
            └──> MarketWebSocket subscribes to active_token_ids in batches of 200
```

### 2. All Data Sources → JSONL → Parquet

Every data source writes through a `JsonWriter` instance. Each record gets two metadata fields injected at write time:

- `_fetched_at`: Unix timestamp (float) of when the record was captured
- `_source`: String identifying the data source (e.g., `"gamma/markets"`, `"ws_market"`)

The Compactor runs every 15 minutes, finds closed JSONL files (past the current hour boundary), converts them to Parquet with zstd compression, and deletes the originals.

---

## Configuration

**File:** `config.py`

All settings are defined as nested Pydantic models. Configuration loads in this priority order (highest wins):

1. Environment variables (prefix `PM_`, nested with `__`, e.g., `PM_LOG_LEVEL=DEBUG`)
2. `config.yaml` file (loaded via a Pydantic model validator)
3. Hardcoded defaults in the Pydantic model

### Key Settings

| Setting | Default | Description |
|---|---|---|
| `data_dir` | `data` | Root directory for all output |
| `state_file` | `state.json` | Path to persistent state |
| `log_level` | `INFO` | Logging level |
| `clob_top_n_markets` | `100` | How many tokens to poll orderbooks for |

### Poller Intervals (seconds)

Each poller has a `min_interval` (reset-to on change) and `max_interval` (ceiling for backoff):

| Poller | Min | Max | Rationale |
|---|---|---|---|
| Market Discovery | 300 | 600 | New markets appear a few times/day |
| CLOB Prices | 30 | 120 | Gap-fill for markets not on WS |
| CLOB Books | 120 | 300 | Periodic reconciliation |
| Data Trades | 60 | 300 | Supplements WS trade feed |
| Holders | 600 | 1800 | Slow-changing aggregate |
| Metadata | 3600 | 7200 | Near-static reference data |

### Rate Limits (requests per second)

| API Group | Default RPS |
|---|---|
| Gamma | 5.0 |
| CLOB | 10.0 |
| Data API | 5.0 |

### WebSocket Settings

| Setting | Default |
|---|---|
| Market ping interval | 8s |
| Sports ping interval | 8s |
| RTDS ping interval | 4s |
| Reconnect base delay | 1s |
| Reconnect max delay | 60s |
| Reconnect jitter | 20% |
| Max consecutive failures | 5 (activates HTTP fallback) |
| Subscription batch size | 200 |
| Message queue size | 10,000 |

---

## API Clients

**Directory:** `clients/`

### Rate Limiter (`rate_limiter.py`)

`TokenBucket` implements a classic token-bucket algorithm:

- Tokens refill at `rate` per second up to `max_tokens` (equal to `rate`)
- `acquire()` blocks until a token is available, then consumes one
- `pause(seconds)` blocks all requests for a cooldown period — called on HTTP 429

`RateLimiterGroup` is a named collection of buckets (one per API group: gamma, clob, data_api).

### Base HTTP Client (`base_http.py`)

`BaseHttpClient` provides shared behavior for all API clients:

- **Rate limiting:** Calls `bucket.acquire()` before each request
- **Retry:** Via tenacity — 5 attempts with exponential backoff (1s–30s) on 429, 5xx, and connection errors
- **429 handling:** Reads `Retry-After` header and pauses the rate limiter bucket for that duration
- **Pagination:** `_get_all()` handles offset-based pagination with:
  - Automatic response shape detection (list, dict with `data`/`results`/`items` key)
  - `max_pages` safety limit (default 500) to prevent infinite loops
  - Progress logging every 50 pages

### Gamma Client (`gamma.py`)

Talks to `https://gamma-api.polymarket.com`. Key methods:

| Method | Endpoint | Notes |
|---|---|---|
| `get_all_markets()` | `GET /markets?closed=false` | Paginates all open markets (~33k) |
| `get_all_events()` | `GET /events?closed=false` | Paginates all open events |
| `get_tags()` | `GET /tags` | Returns flat list |
| `get_series()` | `GET /series` | **Strips nested `events` field** (57MB+ per record) |
| `get_sports()` | `GET /sports` | Returns flat list |

**Important:** The Gamma API's `active=true` filter does not actually exclude closed markets — all markets have `active=true` in the database. We use `closed=false` instead.

### CLOB Client (`clob.py`)

Talks to `https://clob.polymarket.com`. All read-only endpoints, no auth required.

| Method | Endpoint | Notes |
|---|---|---|
| `get_price(token_id)` | `GET /price` | Last trade price |
| `get_midpoint(token_id)` | `GET /midpoint` | Mid price |
| `get_spread(token_id)` | `GET /spread` | Bid-ask spread |
| `get_book(token_id)` | `GET /book` | Full orderbook |
| `get_prices(token_ids)` | Sequential `/price` | Batch, tolerates individual failures |
| `get_books(token_ids)` | Sequential `/book` | Batch, tolerates individual failures |

### Data API Client (`data_api.py`)

Talks to `https://data-api.polymarket.com`.

| Method | Endpoint | Notes |
|---|---|---|
| `get_trades(market?, limit, offset)` | `GET /trades` | Recent trades, optionally by market |
| `get_holders(condition_id)` | `GET /holders?market=` | Top holders for a market's tokens |

**Note:** The leaderboard and open-interest endpoints documented elsewhere do not exist on this API at time of implementation.

---

## Pollers

**Directory:** `pollers/`

### Base Poller (`base_poller.py`)

All pollers inherit from `BasePoller` which provides:

1. **Scheduling loop:** Calls `poll_once()` repeatedly until `stop_event` is set
2. **Adaptive intervals:** If `poll_once()` returns `True` (data changed), interval resets to `min_interval`. If `False`, interval doubles up to `max_interval`. This conserves API quota when nothing is happening.
3. **State tracking:** After each poll, records `last_fetch` timestamp in State
4. **Error isolation:** Exceptions in `poll_once()` are logged but don't crash the loop

Subclasses implement the abstract `poll_once() -> bool` method.

### Market Discovery (`market_discovery.py`)

The most important poller — everything else depends on its output.

1. Calls `gamma.get_all_markets()` (paginates all ~33k open markets)
2. Writes all market records to JSONL
3. Extracts `clobTokenIds` from each active, non-closed market
   - Handles both JSON-string format (`'["id1","id2"]'`) and native list format
4. Detects new market/event/token IDs via set difference against State
5. Updates State with merged ID sets
6. Saves State to disk if anything changed

### CLOB Pollers (`clob_poller.py`)

**ClobPricePoller:** Fetches prices and midpoints for all `state.active_token_ids`, merges them by token_id, writes batch.

**ClobBookPoller:** Fetches orderbooks for the first `top_n` (default 100) active tokens only, to limit API load.

### Data Pollers (`data_poller.py`)

**TradesPoller:** Fetches the 100 most recent trades.

**HoldersPoller:** Iterates the first 50 known markets and fetches holder data for each. Each holder record is tagged with its `condition_id`.

### Metadata Poller (`metadata_poller.py`)

Fetches tags, series, and sports from the Gamma API using three separate writers. Each data type is independently try/except wrapped so a failure in one doesn't block the others.

---

## WebSocket Streams

**Directory:** `websockets/`

### Base WebSocket (`base_ws.py`)

Provides auto-reconnecting WebSocket with:

- **Keepalive:** Background task sends pings at configured interval
- **Message queue:** Bounded `asyncio.Queue` (default 10k) decouples message reception from disk I/O. If the queue fills up, messages are dropped with a warning.
- **Drain coroutine:** Background task pulls messages from the queue and writes them to JSONL
- **Reconnection:** On disconnect, exponential backoff with jitter: `min(base × 2^failures, max) + random(0, delay × jitter)`
- **Fallback detection:** After `max_consecutive_failures` (default 5), sets `is_fallback_active = True` — logged in health checks but not currently acted on by pollers
- **Graceful shutdown:** On stop, drains remaining queued messages before exiting

Subclasses override:
- `on_connect(ws)` — to send subscriptions
- `on_message(data)` — to process/route messages

### Market WebSocket (`market_ws.py`)

Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`.

- **Dynamic subscriptions:** On connect, subscribes to all known `active_token_ids` in batches of 200. A subscription updater task (in `main.py`) periodically calls `update_subscriptions()` to add newly discovered tokens without reconnecting.
- **Event routing:** Routes messages by `event_type` to dedicated writers:
  - `book`, `tick_size_change`, `new_market`, `resolution` → `ws_market/book`
  - `price_change` → `ws_market/price_change`
  - `last_trade_price` → `ws_market/last_trade_price`
- Requires PING every 10s (configured at 8s for safety margin)

### Sports WebSocket (`sports_ws.py`)

Connects to `wss://sports-api.polymarket.com/ws`. No subscription needed — auto-streams all active sports events. Simple passthrough to JSONL.

### RTDS WebSocket (`rtds_ws.py`)

Connects to `wss://ws-live-data.polymarket.com`.

- Subscribes to two channels: `activity` (trade feed) and `crypto_prices`
- Routes messages by `channel` field to separate writers
- Requires PING every 5s (configured at 4s for safety margin)
- This endpoint occasionally returns 429 on initial connect — handled by the base reconnection logic

---

## Storage

**Directory:** `storage/`

### JSONL Writer (`json_writer.py`)

Each data stream gets its own `JsonWriter` instance. The writer handles:

- **File rotation:** Creates new files at hour boundaries (e.g., `2026-03-01T15.jsonl`) or day boundaries for slow-changing data (tags, series, sports)
- **Metadata injection:** Every record gets `_fetched_at` (Unix timestamp) and `_source` prepended
- **Atomic batch writes:** `write_batch()` pre-serializes all records, acquires the lock once, and writes them in a single I/O operation
- **Thread safety:** All writes are protected by an `asyncio.Lock`

### File Layout

```
data/raw/
├── gamma/markets/2026-03-01T15.jsonl       # Hourly
├── gamma/events/2026-03-01T15.jsonl        # Hourly
├── gamma/tags/2026-03-01.jsonl             # Daily
├── gamma/series/2026-03-01.jsonl           # Daily
├── gamma/sports/2026-03-01.jsonl           # Daily
├── clob/prices/2026-03-01T15.jsonl         # Hourly
├── clob/books/2026-03-01T15.jsonl          # Hourly
├── data_api/trades/2026-03-01T15.jsonl     # Hourly
├── data_api/holders/2026-03-01T15.jsonl    # Hourly
├── ws_market/book/2026-03-01T15.jsonl      # Hourly
├── ws_market/price_change/2026-03-01T15.jsonl
├── ws_market/last_trade_price/2026-03-01T15.jsonl
├── ws_sports/events/2026-03-01T15.jsonl
├── ws_rtds/activity/2026-03-01T15.jsonl
└── ws_rtds/crypto_prices/2026-03-01T15.jsonl
```

### Compactor (`compactor.py`)

Runs every 15 minutes. For each completed JSONL file (past the current hour/day boundary):

1. Reads the JSONL with `polars.read_ndjson()` (error-tolerant)
2. Writes Parquet with zstd compression to a Hive-partitioned path
3. Deletes the original JSONL

Compaction runs in a thread executor to avoid blocking the event loop.

**Output layout:**
```
data/parquet/
├── gamma/markets/dt=2026-03-01/hour=15.parquet
├── clob/prices/dt=2026-03-01/hour=15.parquet
├── ws_market/book/dt=2026-03-01/hour=15.parquet
└── ...
```

**Retention cleanup** (runs after each compaction pass):
- Raw JSONL: deleted after 48 hours (configurable)
- Parquet: deleted after 365 days (configurable)

### Schema Definitions (`schema.py`)

Polars type hints for key Parquet columns. These are intentionally permissive — they define known columns but allow schema inference for unknown fields. Defined for: `gamma/markets`, `gamma/events`, `clob/prices`, `clob/books`, `data_api/trades`.

---

## State Management

**File:** `state.py`

`State` persists runtime state to `state.json` for crash recovery. It tracks:

| Field | Type | Purpose |
|---|---|---|
| `known_market_ids` | `list[str]` | All discovered market IDs (cumulative) |
| `known_event_ids` | `list[str]` | All discovered event IDs (cumulative) |
| `active_token_ids` | `list[str]` | CLOB token IDs for active, non-closed markets |
| `last_fetch` | `dict[str, float]` | Unix timestamp of last successful poll per poller |

State is saved:
- By `MarketDiscoveryPoller` immediately after discovering new markets/tokens
- By a `state_saver` background task every 60 seconds
- By the orchestrator on shutdown

On startup, if `state.json` is missing or corrupt, the system starts fresh (empty state). The `_dirty` flag avoids unnecessary disk writes — `save()` is a no-op if nothing changed.

---

## Orchestrator

**File:** `main.py`

`run()` is the main async entry point. It creates all components and launches them as named `asyncio.Task` instances (not a TaskGroup — individual tasks for better error isolation). The full task list:

| Task | Type | Purpose |
|---|---|---|
| `market_discovery` | Poller | Crawls Gamma for markets/events/tokens |
| `clob_prices` | Poller | Price + midpoint snapshots |
| `clob_books` | Poller | Orderbook snapshots for top-N markets |
| `trades` | Poller | Recent trades from Data API |
| `holders` | Poller | Token holder data |
| `metadata` | Poller | Tags, series, sports |
| `market_ws` | WebSocket | Real-time orderbook, prices, trades |
| `sports_ws` | WebSocket | Live sports events |
| `rtds_ws` | WebSocket | Activity feed + crypto prices |
| `compactor` | Storage | JSONL → Parquet conversion |
| `sub_updater` | Background | Pushes new token IDs to market WS (every 30s) |
| `state_saver` | Background | Persists state to disk (every 60s) |
| `health_logger` | Background | Logs health metrics (every 300s) |

### Shutdown Sequence

1. `stop_event` is set (via KeyboardInterrupt on Windows, SIGINT/SIGTERM on Unix)
2. All pollers and WebSockets check `stop_event` and exit their loops
3. Orchestrator waits up to 10s for tasks to finish, then cancels stragglers
4. All JSONL writers are flushed and closed
5. Final state save

### Entry Points

- `python -m pm_fetcher` — via `__main__.py` → `cli_entry()`
- `pm-fetcher` console script — via `pyproject.toml` → `cli_entry()`

On Windows, `asyncio.WindowsSelectorEventLoopPolicy` is set for compatibility.

---

## Error Handling & Resilience

### HTTP Errors

- **429 (Rate Limited):** Reads `Retry-After` header, pauses the rate limiter bucket for that duration. Tenacity retries the request.
- **5xx (Server Error):** Tenacity retries up to 5 times with exponential backoff (1s–30s).
- **Connection errors:** Same retry behavior as 5xx.
- **All other errors:** Raised to the caller (poller catches and logs, then continues).

### WebSocket Errors

- **Disconnect:** Exponential backoff reconnection: 1s → 2s → 4s → ... → 60s (max), with 20% random jitter.
- **5 consecutive failures:** `is_fallback_active` flag is set. Logged in health checks.
- **Queue overflow:** Messages are silently dropped with a warning log. The queue holds up to 10,000 messages.
- **After reconnect:** Market WS re-subscribes to all known token IDs.

### Poller Errors

- Any exception in `poll_once()` is caught, logged, and the poller continues to the next cycle.
- Each metadata fetch (tags, series, sports) is independently wrapped so one failure doesn't block the others.

### State Corruption

- If `state.json` is missing or contains invalid data, the system starts with empty state and re-discovers everything from the APIs.

---

## Key Design Decisions

**Why JSONL before Parquet?** JSONL is append-only and crash-safe — if the process dies mid-write, at most one line is lost. Parquet requires complete write + finalization. The two-stage pipeline (JSONL → Parquet) gives us both safety and query performance.

**Why `asyncio.create_task` instead of `TaskGroup`?** TaskGroup cancels all tasks if any one fails. We want error isolation — a single poller crashing shouldn't take down WebSocket connections.

**Why strip `events` from series?** The Gamma `/series` endpoint embeds all events inside each series record (~57MB per record). Since we fetch events separately via `/events`, this is pure bloat.

**Why `closed=false` instead of `active=true`?** The Gamma API sets `active=true` on all markets (including closed ones). The `closed=false` filter is what actually returns only open markets.

**Why bounded queues for WebSockets?** Without backpressure, a slow disk or burst of WS messages could cause unbounded memory growth. The 10k queue provides a buffer while the drain coroutine writes to disk. If the queue fills, dropping messages is preferable to OOM.

**Why adaptive intervals?** Markets have bursty activity patterns. Doubling the interval on no-change saves API quota during quiet periods. Resetting on change ensures responsiveness when things happen.

**Why sequential CLOB batch requests instead of concurrent?** The CLOB API has strict rate limits. Sequential requests through the token bucket give predictable throughput without risk of burst-triggered 429s.
