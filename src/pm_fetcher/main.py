"""Asyncio orchestrator — wires all components into a TaskGroup."""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog

from pm_fetcher.config import Settings

log = structlog.get_logger()


async def run(settings: Settings | None = None) -> None:
    """Main entry point — starts all pollers, websockets, and compactor."""
    if settings is None:
        settings = Settings()

    from pm_fetcher.utils.logging import setup_logging

    setup_logging(settings.log_level)

    log.info("starting pm-fetcher", data_dir=str(settings.data_dir))

    # Placeholder — will be filled in Phase 5
    log.info("pm-fetcher stopped")


def cli_entry() -> None:
    """CLI entry point for `python -m pm_fetcher` and console script."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
