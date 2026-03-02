# pm-fetcher

A long-running Python service that continuously collects data from all public [Polymarket](https://polymarket.com) API endpoints and WebSocket streams. Raw data is stored as JSONL and automatically compacted into Parquet files for analysis.

## What It Collects

| Source | Data | Method | Cadence |
|---|---|---|---|
| **Gamma API** | Markets, events | HTTP polling | 5 min |
| **Gamma API** | Tags, series, sports | HTTP polling | 1 hour |
| **CLOB API** | Prices, midpoints | HTTP polling | 30s |
| **CLOB API** | Orderbooks (top 100) | HTTP polling | 2 min |
| **Data API** | Trades | HTTP polling | 60s |
| **Data API** | Token holders | HTTP polling | 10 min |
| **Market WS** | Orderbook updates, price changes, last trades | WebSocket | Real-time |
| **Sports WS** | Live sports events | WebSocket | Real-time |
| **RTDS WS** | Activity feed, crypto prices | WebSocket | Real-time |

No authentication required — all endpoints are public and read-only.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Quickstart

```bash
# Clone and install
git clone <repo-url>
cd polymarket-fetcher
uv sync

# Run
uv run python -m pm_fetcher
```

Or with pip:

```bash
pip install -e .
python -m pm_fetcher
```

The service will immediately begin:
1. Connecting to all three WebSocket streams
2. Crawling all open markets from the Gamma API (~33k markets, takes ~60s on first run)
3. Polling prices, trades, and metadata at configured intervals
4. Writing JSONL files to `data/raw/`
5. Compacting closed JSONL files to Parquet every 15 minutes

Stop with `Ctrl+C` — the service flushes all writers and saves state before exiting.

## Output

### Raw JSONL

```
data/raw/
├── gamma/markets/2026-03-01T15.jsonl       # Hourly rotation
├── gamma/events/2026-03-01T15.jsonl
├── gamma/tags/2026-03-01.jsonl             # Daily rotation
├── clob/prices/2026-03-01T15.jsonl
├── clob/books/2026-03-01T15.jsonl
├── data_api/trades/2026-03-01T15.jsonl
├── data_api/holders/2026-03-01T15.jsonl
├── ws_market/book/2026-03-01T15.jsonl
├── ws_market/price_change/2026-03-01T15.jsonl
├── ws_market/last_trade_price/2026-03-01T15.jsonl
├── ws_sports/events/2026-03-01T15.jsonl
├── ws_rtds/activity/2026-03-01T15.jsonl
└── ws_rtds/crypto_prices/2026-03-01T15.jsonl
```

Every record includes `_fetched_at` (Unix timestamp) and `_source` metadata.

### Compacted Parquet

```
data/parquet/
├── gamma/markets/dt=2026-03-01/hour=15.parquet
├── clob/prices/dt=2026-03-01/hour=15.parquet
├── ws_market/book/dt=2026-03-01/hour=15.parquet
└── ...
```

Hive-partitioned by date and hour, compressed with zstd. Query with polars:

```python
import polars as pl

# Read all market snapshots
markets = pl.scan_parquet("data/parquet/gamma/markets/").collect()
print(f"{markets.shape[0]} market snapshots, {markets.shape[1]} columns")

# Read today's trades
trades = pl.scan_parquet("data/parquet/data_api/trades/dt=2026-03-01/").collect()

# Read all price changes from WebSocket
prices = pl.scan_parquet("data/parquet/ws_market/price_change/").collect()
```

### Retention

| Data | Default Retention |
|---|---|
| Raw JSONL | 48 hours |
| Parquet | 365 days |

Both are configurable.

## Configuration

Settings can be customized via `config.yaml`, environment variables, or both (env vars take precedence).

### config.yaml

```yaml
log_level: INFO
data_dir: data
clob_top_n_markets: 100
```

### Environment Variables

All settings use the `PM_` prefix with `__` for nesting:

```bash
PM_LOG_LEVEL=DEBUG
PM_DATA_DIR=/mnt/data
PM_CLOB_TOP_N_MARKETS=200

# Rate limits
PM_RATE_LIMITS__GAMMA_RPS=3.0
PM_RATE_LIMITS__CLOB_RPS=8.0

# Poller intervals (seconds)
PM_POLLERS__MARKET_DISCOVERY=600
PM_POLLERS__CLOB_PRICES=60

# Storage
PM_STORAGE__RAW_RETENTION_HOURS=72
PM_STORAGE__PARQUET_RETENTION_DAYS=180
PM_STORAGE__COMPACTION_INTERVAL=600
```

### Full Settings Reference

See the `Settings` class in [`src/pm_fetcher/config.py`](src/pm_fetcher/config.py) for all available options with defaults.

## Historical Backfill (Closed Markets)

By default, the service only tracks **open** markets (~33k). For backtesting, a separate script crawls all ~525k markets (open + closed):

```bash
# Partial test — fetch 500 markets to verify
uv run pm-backfill --limit 500

# Full backfill (~525k markets, ~17 min)
uv run pm-backfill

# Resume an interrupted backfill
uv run pm-backfill --resume

# Start over (clears progress)
uv run pm-backfill --reset
```

| Metric | Value |
|---|---|
| Total markets (open + closed) | ~525,000 |
| Closed/resolved markets | ~492,000 |
| Pages to crawl (100/page) | ~5,254 |
| Time at 5 RPS | ~17 minutes |
| Gamma API limit usage | ~1.25% (5 of 400 RPS) |

The backfill is a **separate script** (`pm-backfill`), not part of the main service. It:
- **Checkpoints every 500 pages** — Ctrl+C and `--resume` to continue
- **Tracks completion in `state.json`** — won't re-run unless you `--reset`
- **Writes to the same `gamma/markets` stream** — compacted into Parquet alongside live data
- **Safe to run while the main service is running** — uses its own rate limiter instance

After the backfill, closed markets are in Parquet for analysis. New closures are captured naturally by the regular poller as markets resolve.

## State & Recovery

The service saves its state to `state.json` (known markets, token IDs, last-fetch timestamps). On restart, it resumes from where it left off. If the state file is missing or corrupt, it starts fresh and re-discovers everything.

State is saved:
- Immediately when new markets or tokens are discovered
- Every 60 seconds by a background task
- On graceful shutdown

## Resilience

- **HTTP errors:** Automatic retry (5 attempts, exponential backoff 1–30s) on 429 and 5xx. Rate limiter pauses on 429 using `Retry-After` header.
- **WebSocket disconnects:** Exponential backoff reconnection (1s → 60s max, with 20% jitter). After 5 consecutive failures, an HTTP fallback flag is set.
- **Poller errors:** Logged and skipped — the poller continues on the next cycle.
- **Adaptive polling:** Intervals double when no new data is detected, reset when changes appear. This conserves API quota during quiet periods.

## API Rate Limit Usage

The service is designed to stay well under Polymarket's documented rate limits. All limits below are per 10-second sliding window as enforced by Cloudflare.

### Gamma API

| Poller | Requests/cycle | Interval | Sustained RPS | API Limit | Usage |
|---|---|---|---|---|---|
| Market Discovery `/markets` | ~340 pages | 5 min | ~1.1 | 30/s | ~3.7% |
| Market Discovery `/events` | ~50 pages | 5 min | ~0.17 | 50/s | ~0.3% |
| Metadata (`/tags`, `/series`, `/sports`) | 3 | 1 hour | negligible | 400/s | <0.1% |

The heaviest moment is the initial crawl on first startup (~340 pages at 5 RPS = ~68 seconds at ~17% of the `/markets` limit). After that it settles to under 4%.

### CLOB API

| Poller | Requests/cycle | Interval | Sustained RPS | API Limit | Usage |
|---|---|---|---|---|---|
| Prices (`/price` + `/midpoint`) | 2 per token | 30s target | 10 (bucket-capped) | 150/s each | ~6.7% |
| Books (`/book`) | 100 | 2 min | ~0.83 | 150/s | ~0.6% |

The price poller is the largest consumer. With ~48k active tokens, a full cycle takes much longer than the 30s interval — the token bucket (10 RPS) is the real throttle, keeping us at ~7% of the CLOB limit.

### Data API

| Poller | Requests/cycle | Interval | Sustained RPS | API Limit | Usage |
|---|---|---|---|---|---|
| Trades | 1 | 60s | 0.017 | 20/s | <0.1% |
| Holders | 50 | 10 min | 0.083 | 100/s | <0.1% |

### Summary

| API Group | Our Token Bucket | API Limit (general) | Steady-State Usage |
|---|---|---|---|
| **Gamma** | 5 RPS | 400 RPS | ~4% |
| **CLOB** | 10 RPS | 900 RPS | ~7% |
| **Data API** | 5 RPS | 100 RPS | <1% |

**Under 10% of all rate limits in steady state.** The token buckets are intentionally set at a fraction of API limits for a wide safety margin. WebSocket connections are persistent and don't count against HTTP rate limits.

## Health Monitoring

Every 5 minutes, the service logs a health check:

```json
{
  "event": "health",
  "markets": 33421,
  "tokens": 48293,
  "ws_market_fallback": false,
  "ws_sports_fallback": false,
  "ws_rtds_fallback": false
}
```

## Testing & Verification

### Quick smoke test

Start the service and let it run for 5 minutes, then check output:

```bash
# Start the service
uv run pm-fetcher

# After ~60 seconds, check raw data is flowing
ls data/raw/gamma/markets/
ls data/raw/ws_market/price_change/

# Verify records have correct metadata
python -c "
import orjson, glob
f = sorted(glob.glob('data/raw/gamma/markets/*.jsonl'))[-1]
line = open(f,'rb').readline()
rec = orjson.loads(line)
print(f'id={rec[\"id\"]}, _source={rec[\"_source\"]}, has_fetched_at={\"_fetched_at\" in rec}')
"
```

Expected within the first 5 minutes:
- `gamma/markets/` — ~33k market records per discovery cycle
- `gamma/events/` — ~8k event records
- `ws_market/price_change/` — thousands of real-time price updates
- `clob/prices/` — price snapshots for active tokens
- `data_api/trades/` — 100 recent trades per cycle

### Verify Parquet compaction

Compaction runs every 15 minutes on JSONL files from completed hours. To see it in action:

```bash
# Run for at least 1 hour + 15 minutes past the hour boundary
uv run pm-fetcher

# Check for Parquet output
ls data/parquet/gamma/markets/
# Expected: dt=YYYY-MM-DD/hour=HH.parquet

# Query with polars
python -c "
import polars as pl
df = pl.scan_parquet('data/parquet/gamma/markets/').collect()
print(f'{df.shape[0]} rows, {df.shape[1]} columns')
print(df.select('id', 'question', 'volume').head(3))
"
```

### Test backfill

```bash
# Fetch 500 markets to verify the backfill works
uv run pm-backfill --limit 500

# Check output includes closed markets
python -c "
import orjson, glob
f = sorted(glob.glob('data/raw/gamma/markets/*.jsonl'))[-1]
lines = open(f,'rb').readlines()[-500:]
closed = sum(1 for l in lines if orjson.loads(l).get('closed'))
print(f'Closed markets in last 500 records: {closed}')
"

# Test resume
uv run pm-backfill --resume --limit 500  # continues from offset 500
```

### Verify WebSocket streams

```bash
# Start the service and watch logs for WS connections
uv run pm-fetcher 2>&1 | grep -E "ws (connected|subscribed|json decode)"

# Expected:
#   ws connected  ws=ws_market
#   ws connected  ws=ws_sports
#   ws connected  ws=ws_rtds
#   ws subscribed ws=ws_market count=NNNNN
```

The `INVALID OPERATION` warnings after WS subscription are expected — the Polymarket WS server rejects some subscription batches when too many tokens are sent. The data still flows correctly for accepted subscriptions.

### Verify rate limits

Monitor the structured logs for rate limit warnings:

```bash
uv run pm-fetcher 2>&1 | grep "rate limited"
```

Under normal operation you should see **zero** rate limit warnings. The token buckets are set at a fraction of API limits (see [API Rate Limit Usage](#api-rate-limit-usage)).

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for a detailed technical deep-dive into the system design, data flow, component interactions, and design decisions.

## Project Structure

```
src/pm_fetcher/
├── main.py                  # Orchestrator — runs 13 concurrent tasks
├── backfill.py              # Standalone closed-market backfill script
├── config.py                # All settings (Pydantic + YAML + env)
├── state.py                 # Persistent state for crash recovery
├── clients/                 # HTTP API clients with rate limiting
│   ├── gamma.py             # Markets, events, tags, series, sports
│   ├── clob.py              # Prices, midpoints, orderbooks
│   └── data_api.py          # Trades, holders
├── pollers/                 # Scheduled polling loops
│   ├── market_discovery.py  # Discovers markets → feeds WS + pollers
│   ├── clob_poller.py       # Price + book snapshots
│   ├── data_poller.py       # Trades + holders
│   └── metadata_poller.py   # Tags, series, sports
├── websockets/              # Real-time streams
│   ├── market_ws.py         # Orderbook, prices, trades
│   ├── sports_ws.py         # Live sports
│   └── rtds_ws.py           # Activity feed, crypto prices
└── storage/                 # JSONL writer + Parquet compactor
    ├── json_writer.py       # Append-only with hourly/daily rotation
    └── compactor.py         # JSONL → Parquet + retention cleanup
```

## Copyright

Copyright (c) 2026 James Thompson. All rights reserved.
