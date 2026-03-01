# Project Instructions

## Overview
Long-running Python service that continuously fetches data from all public Polymarket API endpoints, storing raw JSONL snapshots and compacting them into Parquet files.

## Development Setup
```bash
uv sync
uv run python -m pm_fetcher
```

## Git Workflow
- Commit regularly as you work (after completing each logical unit of work)
- Do NOT include "Co-Authored-By" or any AI attribution in commit messages
- Keep commit messages concise and descriptive of the changes

## Key Directories
- `src/pm_fetcher/` — main package
- `src/pm_fetcher/clients/` — HTTP API clients (Gamma, CLOB, Data API)
- `src/pm_fetcher/pollers/` — polling loops with adaptive intervals
- `src/pm_fetcher/websockets/` — WS connections (market, sports, RTDS)
- `src/pm_fetcher/storage/` — JSONL writer, Parquet compactor
- `data/` — runtime output directory (gitignored)
