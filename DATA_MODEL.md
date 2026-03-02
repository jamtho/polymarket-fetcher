# Data Model

This document describes every data stream collected by pm-fetcher, including field names, types, and example values. Every record in every stream includes two metadata fields injected at write time:

| Field | Type | Description |
|---|---|---|
| `_fetched_at` | float | Unix timestamp (UTC) of when the record was captured |
| `_source` | string | Identifier for the data source (e.g., `"gamma/markets"`, `"ws_market"`) |

---

## Table of Contents

- [Gamma API Streams](#gamma-api-streams)
  - [gamma/markets](#gammamarkets)
  - [gamma/events](#gammaevents)
  - [gamma/tags](#gammatags)
  - [gamma/series](#gammaseries)
  - [gamma/sports](#gammasports)
- [CLOB API Streams](#clob-api-streams)
  - [clob/prices](#clobprices)
  - [clob/books](#clobbooks)
- [Data API Streams](#data-api-streams)
  - [data_api/trades](#data_apitrades)
  - [data_api/holders](#data_apiholders)
- [WebSocket Streams](#websocket-streams)
  - [ws_market/book](#ws_marketbook)
  - [ws_market/price_change](#ws_marketprice_change)
  - [ws_market/last_trade_price](#ws_marketlast_trade_price)
  - [ws_sports/events](#ws_sportsevents)
  - [ws_rtds/activity](#ws_rtdsactivity)
  - [ws_rtds/crypto_prices](#ws_rtdscrypto_prices)
- [File Rotation](#file-rotation)
- [Parquet Partitioning](#parquet-partitioning)

---

## Gamma API Streams

Source: `https://gamma-api.polymarket.com`

### gamma/markets

One record per open market. Crawled every 5 minutes. Currently ~33,500 open markets per crawl.

| Field | Type | Example | Description |
|---|---|---|---|
| `id` | string | `"531202"` | Market ID |
| `question` | string | `"BitBoy convicted?"` | Market question |
| `conditionId` | string | `"0xb48621f..."` | On-chain condition ID (hex) |
| `slug` | string | `"bitboy-convicted"` | URL slug |
| `outcomes` | string | `'["Yes", "No"]'` | JSON array of outcome labels |
| `outcomePrices` | string | `'["0.295", "0.705"]'` | JSON array of current prices |
| `clobTokenIds` | string | `'["7546712...", "3842963..."]'` | JSON array of CLOB token IDs |
| `volume` | string | `"35094.08"` | Total volume (USD) |
| `volumeNum` | float | `35094.08` | Same as volume, numeric type |
| `volume24hr` | float | `6732.98` | 24-hour volume |
| `volume1wk` | float | `11452.82` | 1-week volume |
| `volume1mo` | float | `20311.12` | 1-month volume |
| `volume1yr` | float | `35069.84` | 1-year volume |
| `liquidity` | string | `"858.65"` | Current liquidity |
| `liquidityNum` | float | `858.65` | Same, numeric type |
| `active` | bool | `true` | Always true in practice |
| `closed` | bool | `false` | Whether market is resolved |
| `startDate` | string | `"2025-03-26T16:49:31.084Z"` | ISO 8601 start date |
| `endDate` | string | `"2026-03-31T12:00:00Z"` | ISO 8601 end date |
| `createdAt` | string | `"2025-03-26T14:44:34.145447Z"` | Creation timestamp |
| `updatedAt` | string | `"2026-03-02T01:27:02.957555Z"` | Last update timestamp |
| `lastTradePrice` | float | `0.298` | Last trade price |
| `bestBid` | float | `0.292` | Current best bid |
| `bestAsk` | float | `0.298` | Current best ask |
| `spread` | float | `0.006` | Bid-ask spread |
| `oneDayPriceChange` | float | `-0.019` | 24h price change |
| `oneWeekPriceChange` | float | `-0.443` | 1-week price change |
| `negRisk` | bool | `false` | Whether market uses neg-risk model |
| `enableOrderBook` | bool | `true` | Whether CLOB is enabled |
| `competitive` | float | `0.96` | Competitiveness score (0–1) |
| `rewardsMinSize` | int | `20` | Minimum order size for rewards |
| `rewardsMaxSpread` | float | `3.5` | Maximum spread for rewards |
| `category` | string | `"Crypto"` | Market category (sometimes present) |
| `image` | string | URL | Market image URL |
| `description` | string | text | Full market description |

Additional fields may be present (the schema is permissive). See `resolutionSource`, `questionID`, `orderPriceMinTickSize`, `umaBond`, `umaReward`, `acceptingOrders`, etc.

### gamma/events

One record per open event. Events group related markets. Currently ~7,900 open events.

| Field | Type | Example | Description |
|---|---|---|---|
| `id` | string | `"16167"` | Event ID |
| `title` | string | `"MicroStrategy sells any Bitcoin by ___?"` | Event title |
| `slug` | string | `"microstrategy-sell..."` | URL slug |
| `description` | string | text | Event description |
| `active` | bool | `true` | Active flag |
| `closed` | bool | `false` | Resolved flag |
| `volume` | float | `20870884.25` | Total event volume (USD) |
| `volume24hr` | float | `47858.89` | 24-hour volume |
| `liquidity` | float | `224651.77` | Current liquidity |
| `openInterest` | int | `0` | Open interest (often 0) |
| `startDate` | string | ISO 8601 | Event start date |
| `endDate` | string | ISO 8601 | Event end date |
| `competitive` | float | `0.888` | Competitiveness score |
| `commentCount` | int | `221` | Number of comments |
| `negRisk` | bool | `false` | Neg-risk model flag |
| `markets` | list | `[{...}, ...]` | Nested market objects |
| `tags` | list | `[{id, label, slug}, ...]` | Associated tags |

### gamma/tags

Reference data — all tags used to categorize markets. Daily rotation.

| Field | Type | Example | Description |
|---|---|---|---|
| `id` | string | `"671"` | Tag ID |
| `label` | string | `"jto"` | Display label |
| `slug` | string | `"jto"` | URL slug |
| `publishedAt` | string | `"2023-12-07 19:16:45.979+00"` | Publish timestamp |
| `createdAt` | string | ISO 8601 | Creation timestamp |
| `updatedAt` | string | ISO 8601 | Last update |
| `requiresTranslation` | bool | `false` | Translation flag |

### gamma/series

Reference data — market series (recurring markets). Daily rotation. The fetcher strips the nested `events` field to save space (events are fetched separately).

| Field | Type | Example | Description |
|---|---|---|---|
| `id` | string | `"10540"` | Series ID |
| `title` | string | `"National Ethereum Reserve"` | Series title |
| `ticker` | string | `"national-ethereum-reserve"` | Ticker symbol |
| `slug` | string | same as ticker | URL slug |
| `seriesType` | string | `"single"` | Series type |
| `recurrence` | string | `"annual"`, `"5m"`, `"daily"` | Recurrence pattern |
| `active` | bool | `true` | Active flag |
| `closed` | bool | `false` | Closed flag |
| `volume` | float | `14161.11` | Total volume |
| `volume24hr` | float | `8.44` | 24-hour volume |
| `liquidity` | float | `5439.01` | Current liquidity |
| `commentCount` | int | `11` | Comment count |

### gamma/sports

Reference data — sport categories for sports betting markets. Daily rotation.

| Field | Type | Example | Description |
|---|---|---|---|
| `id` | int | `1` | Sport ID |
| `sport` | string | `"ncaab"` | Sport code |
| `image` | string | URL | Sport image |
| `resolution` | string | URL | Resolution source |
| `ordering` | string | `"home"` | Display ordering |
| `tags` | string | `"1,100149,100639"` | Comma-separated tag IDs |
| `series` | string | `"39"` | Associated series ID |

---

## CLOB API Streams

Source: `https://clob.polymarket.com`

### clob/prices

Price and midpoint snapshots for active tokens. The fetcher merges `/price` and `/midpoint` responses.

| Field | Type | Example | Description |
|---|---|---|---|
| `token_id` | string | `"7546712..."` | CLOB token ID |
| `price` | varies | `"0.298"` or error object | Last trade price |
| `midpoint` | string | `"0.295"` | Midpoint price |

Note: The `/price` endpoint requires a `side` parameter. The fetcher may receive `{"error": "Invalid side"}` for some tokens — these are still recorded.

### clob/books

Full orderbook snapshots for the top 100 active tokens. Polled every 2 minutes.

| Field | Type | Example | Description |
|---|---|---|---|
| `asset_id` | string | `"7546712..."` | CLOB token ID |
| `market` | string | `"0xb48621f..."` | Market condition ID |
| `timestamp` | string | `"1772415055613"` | Server timestamp (ms) |
| `hash` | string | `"0b89da43..."` | Orderbook hash |
| `bids` | list | `[{"price": "0.29", "size": "100"}, ...]` | Bid levels |
| `asks` | list | `[{"price": "0.30", "size": "50"}, ...]` | Ask levels |
| `last_trade_price` | string | `"0.708"` | Last trade price |
| `min_order_size` | string | `"5"` | Minimum order size |
| `tick_size` | string | `"0.001"` | Price tick size |
| `neg_risk` | bool | `false` | Neg-risk flag |

---

## Data API Streams

Source: `https://data-api.polymarket.com`

### data_api/trades

Recent trades across all markets. 100 trades per poll, every 60 seconds.

| Field | Type | Example | Description |
|---|---|---|---|
| `proxyWallet` | string | `"0x8936..."` | Trader's proxy wallet address |
| `side` | string | `"BUY"` or `"SELL"` | Trade side |
| `asset` | string | `"11318990..."` | Token ID |
| `conditionId` | string | `"0xb98f79..."` | Market condition ID |
| `size` | int | `20` | Trade size (shares) |
| `price` | float | `0.17` | Trade price |
| `timestamp` | int | `1772414817` | Unix timestamp |
| `title` | string | `"Will Iran strike..."` | Market title |
| `slug` | string | `"will-iran-strike..."` | Market slug |
| `icon` | string | URL | Market icon |
| `eventSlug` | string | `"iran-strikes-israel-on"` | Event slug |
| `outcome` | string | `"No"` | Outcome label |
| `outcomeIndex` | int | `1` | Outcome index (0=Yes, 1=No) |
| `name` | string | `"daniel.sousa.me"` | Trader display name |
| `pseudonym` | string | `"Idealistic-Silo"` | Trader pseudonym |
| `transactionHash` | string | `"0x2f6985..."` | On-chain transaction hash |
| `bio` | string | text | Trader bio |
| `profileImage` | string | URL | Trader profile image |

### data_api/holders

Top 20 holders per token for tracked markets. Each record is a token-level holder list. Polled for the first 50 known markets every 10 minutes.

| Field | Type | Example | Description |
|---|---|---|---|
| `condition_id` | string | `"0xb48621f..."` | Market condition ID (added by fetcher) |
| `token` | string | `"7546712..."` | Token ID |
| `holders` | list | `[{...}, ...]` | Top 20 holders |

Each holder object contains:

| Field | Type | Example | Description |
|---|---|---|---|
| `proxyWallet` | string | `"0x0f46e..."` | Holder's proxy wallet |
| `name` | string | `"mirror.of.kalandra"` | Display name |
| `pseudonym` | string | `"Neat-Ghost"` | Pseudonym |
| `asset` | string | `"3842963..."` | Token ID |
| `amount` | float | `522.86` | Amount held |
| `outcomeIndex` | int | `1` | Outcome index |
| `displayUsernamePublic` | bool | `true` | Public name flag |
| `verified` | bool | `false` | Verified flag |

---

## WebSocket Streams

### ws_market/book

Orderbook snapshots from the Market WebSocket. Sent as the initial response after subscribing to asset IDs. Each record is a single book entry.

| Field | Type | Example | Description |
|---|---|---|---|
| `market` | string | `"0x50ddb..."` | Market condition ID |
| `asset_id` | string | `"9437620..."` | Token ID |
| `timestamp` | string | `"1772413453505"` | Server timestamp (ms) |
| `hash` | string | `"3550afb1..."` | Book hash |
| `bids` | list | `[{"price": "0.01", "size": "5685.37"}, ...]` | Bid levels |
| `asks` | list | `[{"price": "0.99", "size": "30"}, ...]` | Ask levels |

### ws_market/price_change

Real-time price change events. This is the highest-volume stream — thousands of events per minute during active trading.

| Field | Type | Example | Description |
|---|---|---|---|
| `market` | string | `"0x50ddb..."` | Market condition ID |
| `price_changes` | list | `[{...}, ...]` | Array of price change entries |

Each price change entry:

| Field | Type | Example | Description |
|---|---|---|---|
| `asset_id` | string | `"8827504..."` | Token ID |
| `price` | string | `"0.75"` | New price |
| `size` | string | `"0"` | Size at this price |
| `side` | string | `"SELL"` | Side |
| `hash` | string | `"da9aff36..."` | Update hash |
| `best_bid` | string | `"0.48"` | New best bid |
| `best_ask` | string | `"0.52"` | New best ask |

### ws_market/last_trade_price

Last trade price updates for subscribed markets. Same structure as price_change but with trade data.

### ws_sports/events

Live sports event data streamed from `wss://sports-api.polymarket.com/ws`. Format varies by sport type.

### ws_rtds/activity

Real-time activity feed from the RTDS WebSocket. Includes live trades and other platform activity.

### ws_rtds/crypto_prices

Real-time cryptocurrency price updates from the RTDS WebSocket.

---

## File Rotation

| Stream | Rotation | Filename Pattern |
|---|---|---|
| `gamma/markets` | Hourly | `2026-03-01T15.jsonl` |
| `gamma/events` | Hourly | `2026-03-01T15.jsonl` |
| `gamma/tags` | Daily | `2026-03-01.jsonl` |
| `gamma/series` | Daily | `2026-03-01.jsonl` |
| `gamma/sports` | Daily | `2026-03-01.jsonl` |
| All other streams | Hourly | `2026-03-01T15.jsonl` |

Rotation happens at UTC hour/day boundaries. The current file is always being appended to; only closed files (from previous hours/days) are eligible for compaction.

---

## Parquet Partitioning

Compacted Parquet files use Hive-style partitioning:

```
data/parquet/{stream}/dt={YYYY-MM-DD}/hour={HH}.parquet
```

Example:
```
data/parquet/gamma/markets/dt=2026-03-01/hour=15.parquet
data/parquet/ws_market/price_change/dt=2026-03-01/hour=22.parquet
```

Compression: zstd. All columns from the JSONL are preserved. Query with polars:

```python
import polars as pl

# Scan all market snapshots efficiently
markets = pl.scan_parquet("data/parquet/gamma/markets/**/*.parquet")

# Filter to a specific date
march_1 = markets.filter(pl.col("_fetched_at") > 1709251200).collect()

# Get unique markets
unique = march_1.select("id", "question", "volume").unique("id")
```
