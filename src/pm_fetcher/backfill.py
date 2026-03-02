"""Standalone backfill script — crawls all markets (open + closed) for backtesting.

Usage:
    uv run pm-backfill                    # Full backfill (~525k markets, ~17 min)
    uv run pm-backfill --limit 500        # Partial test (500 markets)
    uv run pm-backfill --limit 5000       # Larger test run
    uv run pm-backfill --resume           # Resume interrupted backfill
    uv run pm-backfill --reset            # Discard progress and start over

Rate limit impact:
    Uses the Gamma token bucket at 5 RPS (1.25% of the 400 RPS API limit).
    Runs independently of the main service — safe to run concurrently.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

import aiohttp
import structlog

from pm_fetcher.clients.gamma import GammaClient
from pm_fetcher.clients.rate_limiter import TokenBucket
from pm_fetcher.config import Settings
from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter
from pm_fetcher.utils.logging import setup_logging

log = structlog.get_logger()


async def backfill(
    settings: Settings,
    *,
    limit: int | None = None,
    resume: bool = False,
    reset: bool = False,
) -> None:
    """Crawl all markets from the Gamma API and write to JSONL.

    Args:
        settings: Application settings.
        limit: Maximum number of markets to fetch (None = all).
        resume: Resume from the last checkpoint.
        reset: Clear previous backfill progress and start fresh.
    """
    setup_logging(settings.log_level)

    # Use a separate state file so the backfill and main service
    # don't clobber each other's state.json
    backfill_state_path = settings.state_file.parent / "backfill_state.json"
    state = State(backfill_state_path)
    await state.load()

    if reset:
        state.backfill_completed = False
        state.backfill_offset = 0
        await state.save()
        log.info("backfill progress reset")

    if state.backfill_completed and not reset:
        log.info(
            "backfill already completed previously — use --reset to re-run",
        )
        return

    start_offset = state.backfill_offset if resume else 0
    if resume and start_offset > 0:
        log.info("resuming backfill", offset=start_offset)

    writer = JsonWriter(settings.data_dir, "gamma/markets")
    bucket = TokenBucket(settings.rate_limits.gamma_rps)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        gamma = GammaClient(session, bucket, settings.gamma.base_url)

        offset = start_offset
        page_size = 100
        total_fetched = 0
        checkpoint_pages = 500
        page = 0
        t0 = time.monotonic()

        log.info(
            "starting backfill",
            offset=offset,
            limit=limit or "all",
            rate_limit=f"{settings.rate_limits.gamma_rps} RPS",
        )

        try:
            while True:
                # Check item limit
                if limit is not None and total_fetched >= limit:
                    log.info("reached item limit", limit=limit)
                    break

                # Fetch one page
                fetch_limit = page_size
                if limit is not None:
                    fetch_limit = min(page_size, limit - total_fetched)

                try:
                    batch = await gamma.get_markets_page(
                        limit=fetch_limit, offset=offset,
                    )
                except Exception:
                    log.exception("page error, retrying in 10s", offset=offset)
                    await asyncio.sleep(10)
                    continue

                if not batch:
                    break

                await writer.write_batch(batch)
                total_fetched += len(batch)
                offset += len(batch)
                page += 1

                # Checkpoint
                if page % checkpoint_pages == 0:
                    state.backfill_offset = offset
                    await state.save()

                # Progress logging
                if page % 100 == 0:
                    elapsed = time.monotonic() - t0
                    rps = total_fetched / elapsed if elapsed > 0 else 0
                    log.info(
                        "backfill progress",
                        pages=page,
                        items=total_fetched,
                        offset=offset,
                        elapsed=f"{elapsed:.0f}s",
                        rate=f"{rps:.0f} items/s",
                    )

                if len(batch) < fetch_limit:
                    break

        except KeyboardInterrupt:
            log.info("interrupted by user")
        finally:
            # Save progress
            state.backfill_offset = offset
            await state.save()
            await writer.flush()
            await writer.close()

        elapsed = time.monotonic() - t0
        if limit is None and total_fetched > 0 and page > 0:
            # Completed full backfill
            state.backfill_completed = True
            state.backfill_offset = 0
            await state.save()

        log.info(
            "backfill finished",
            total_items=total_fetched,
            elapsed=f"{elapsed:.0f}s",
            completed=state.backfill_completed,
            output=str(settings.data_dir / "raw" / "gamma" / "markets"),
        )


def cli_entry() -> None:
    """CLI entry point for `pm-backfill`."""
    parser = argparse.ArgumentParser(
        description="Backfill all Polymarket markets (open + closed) for backtesting.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of markets to fetch (default: all ~525k)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the last checkpoint offset",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear previous backfill progress and start fresh",
    )
    args = parser.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    settings = Settings()

    try:
        asyncio.run(backfill(settings, limit=args.limit, resume=args.resume, reset=args.reset))
    except KeyboardInterrupt:
        pass
