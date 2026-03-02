# Copyright (c) 2026 James Thompson. All rights reserved.

"""Asyncio orchestrator — wires all components into a TaskGroup."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import aiohttp
import structlog

from pm_fetcher.clients.clob import ClobClient
from pm_fetcher.clients.data_api import DataApiClient
from pm_fetcher.clients.gamma import GammaClient
from pm_fetcher.clients.rate_limiter import RateLimiterGroup
from pm_fetcher.config import Settings
from pm_fetcher.pollers.clob_poller import ClobBookPoller, ClobPricePoller
from pm_fetcher.pollers.data_poller import HoldersPoller, TradesPoller
from pm_fetcher.pollers.market_discovery import MarketDiscoveryPoller
from pm_fetcher.pollers.metadata_poller import MetadataPoller
from pm_fetcher.state import State
from pm_fetcher.storage.compactor import Compactor
from pm_fetcher.storage.json_writer import JsonWriter
from pm_fetcher.websockets.market_ws import MarketWebSocket
from pm_fetcher.websockets.rtds_ws import RtdsWebSocket
from pm_fetcher.websockets.sports_ws import SportsWebSocket

log = structlog.get_logger()


def _make_writer(data_dir: Path, stream: str, *, daily: bool = False) -> JsonWriter:
    return JsonWriter(data_dir, stream, daily=daily)


async def run(settings: Settings | None = None) -> None:
    """Main entry point — starts all pollers, websockets, and compactor."""
    if settings is None:
        settings = Settings()

    from pm_fetcher.utils.logging import setup_logging

    setup_logging(settings.log_level)
    log.info("starting pm-fetcher", data_dir=str(settings.data_dir))

    # --- State ---
    state = State(settings.state_file)
    await state.load()

    # --- Rate limiters ---
    rate_limiters = RateLimiterGroup()
    rate_limiters.add("gamma", settings.rate_limits.gamma_rps)
    rate_limiters.add("clob", settings.rate_limits.clob_rps)
    rate_limiters.add("data_api", settings.rate_limits.data_api_rps)

    # --- JSONL Writers ---
    writers = {
        "gamma/markets": _make_writer(settings.data_dir, "gamma/markets"),
        "gamma/events": _make_writer(settings.data_dir, "gamma/events"),
        "gamma/tags": _make_writer(settings.data_dir, "gamma/tags", daily=True),
        "gamma/series": _make_writer(settings.data_dir, "gamma/series", daily=True),
        "gamma/sports": _make_writer(settings.data_dir, "gamma/sports", daily=True),
        "clob/prices": _make_writer(settings.data_dir, "clob/prices"),
        "clob/books": _make_writer(settings.data_dir, "clob/books"),
        "data_api/trades": _make_writer(settings.data_dir, "data_api/trades"),
        "data_api/holders": _make_writer(settings.data_dir, "data_api/holders"),
        "ws_market/book": _make_writer(settings.data_dir, "ws_market/book"),
        "ws_market/price_change": _make_writer(settings.data_dir, "ws_market/price_change"),
        "ws_market/last_trade_price": _make_writer(settings.data_dir, "ws_market/last_trade_price"),
        "ws_sports/events": _make_writer(settings.data_dir, "ws_sports/events"),
        "ws_rtds/activity": _make_writer(settings.data_dir, "ws_rtds/activity"),
        "ws_rtds/crypto_prices": _make_writer(settings.data_dir, "ws_rtds/crypto_prices"),
    }

    # --- Stop event ---
    stop_event = asyncio.Event()

    # Graceful shutdown handler
    def _signal_handler() -> None:
        log.info("shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (asyncio.unix_events.signal.SIGINT, asyncio.unix_events.signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

    # --- HTTP session ---
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # --- API Clients ---
        gamma = GammaClient(session, rate_limiters.get("gamma"), settings.gamma.base_url)
        clob = ClobClient(session, rate_limiters.get("clob"), settings.clob.base_url)
        data_api = DataApiClient(session, rate_limiters.get("data_api"), settings.data_api.base_url)

        # --- Pollers ---
        market_discovery = MarketDiscoveryPoller(
            gamma,
            writers["gamma/markets"],
            writers["gamma/events"],
            state,
            min_interval=settings.pollers.market_discovery,
            max_interval=settings.pollers.market_discovery_max,
        )
        clob_prices = ClobPricePoller(
            clob, writers["clob/prices"], state,
            min_interval=settings.pollers.clob_prices,
            max_interval=settings.pollers.clob_prices_max,
        )
        clob_books = ClobBookPoller(
            clob, writers["clob/books"], state,
            top_n=settings.clob_top_n_markets,
            min_interval=settings.pollers.clob_books,
            max_interval=settings.pollers.clob_books_max,
        )
        trades = TradesPoller(
            data_api, writers["data_api/trades"], state,
            min_interval=settings.pollers.data_trades,
            max_interval=settings.pollers.data_trades_max,
        )
        holders = HoldersPoller(
            data_api, writers["data_api/holders"], state,
            min_interval=settings.pollers.data_oi_holders,
            max_interval=settings.pollers.data_oi_holders_max,
        )
        metadata = MetadataPoller(
            gamma,
            writers["gamma/tags"],
            writers["gamma/series"],
            writers["gamma/sports"],
            state,
            min_interval=settings.pollers.metadata,
            max_interval=settings.pollers.metadata_max,
        )

        # --- WebSockets ---
        ws_cfg = settings.websocket
        market_ws = MarketWebSocket(
            ws_cfg.market_url,
            {
                "book": writers["ws_market/book"],
                "price_change": writers["ws_market/price_change"],
                "last_trade_price": writers["ws_market/last_trade_price"],
            },
            state,
            batch_size=ws_cfg.subscription_batch_size,
            ping_interval=ws_cfg.market_ping_interval,
            reconnect_base=ws_cfg.reconnect_base,
            reconnect_max=ws_cfg.reconnect_max,
            reconnect_jitter=ws_cfg.reconnect_jitter,
            max_consecutive_failures=ws_cfg.max_consecutive_failures,
            queue_size=ws_cfg.queue_size,
        )
        sports_ws = SportsWebSocket(
            ws_cfg.sports_url,
            writers["ws_sports/events"],
            ping_interval=ws_cfg.sports_ping_interval,
            reconnect_base=ws_cfg.reconnect_base,
            reconnect_max=ws_cfg.reconnect_max,
            reconnect_jitter=ws_cfg.reconnect_jitter,
            max_consecutive_failures=ws_cfg.max_consecutive_failures,
            queue_size=ws_cfg.queue_size,
        )
        rtds_ws = RtdsWebSocket(
            ws_cfg.rtds_url,
            writers["ws_rtds/activity"],
            writers["ws_rtds/crypto_prices"],
            ping_interval=ws_cfg.rtds_ping_interval,
            reconnect_base=ws_cfg.reconnect_base,
            reconnect_max=ws_cfg.reconnect_max,
            reconnect_jitter=ws_cfg.reconnect_jitter,
            max_consecutive_failures=ws_cfg.max_consecutive_failures,
            queue_size=ws_cfg.queue_size,
        )

        # --- Compactor ---
        compactor = Compactor(settings.data_dir, settings.storage)

        # --- Subscription updater ---
        async def subscription_updater() -> None:
            """Periodically tell the market WS about newly discovered tokens."""
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=30.0)
                    break
                except asyncio.TimeoutError:
                    try:
                        await market_ws.update_subscriptions()
                    except Exception:
                        log.exception("subscription update error")

        # --- State saver ---
        async def state_saver() -> None:
            """Periodically save state to disk."""
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=60.0)
                    break
                except asyncio.TimeoutError:
                    await state.save()

        # --- Health logger ---
        async def health_logger() -> None:
            """Periodic health check log."""
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=300.0)
                    break
                except asyncio.TimeoutError:
                    log.info(
                        "health",
                        markets=len(state.known_market_ids),
                        tokens=len(state.active_token_ids),
                        ws_market_fallback=market_ws.is_fallback_active,
                        ws_sports_fallback=sports_ws.is_fallback_active,
                        ws_rtds_fallback=rtds_ws.is_fallback_active,
                    )

        # --- Run everything ---
        log.info("starting all tasks")

        # Use individual tasks instead of TaskGroup for better error isolation
        tasks = [
            asyncio.create_task(market_discovery.run(stop_event), name="market_discovery"),
            asyncio.create_task(clob_prices.run(stop_event), name="clob_prices"),
            asyncio.create_task(clob_books.run(stop_event), name="clob_books"),
            asyncio.create_task(trades.run(stop_event), name="trades"),
            asyncio.create_task(holders.run(stop_event), name="holders"),
            asyncio.create_task(metadata.run(stop_event), name="metadata"),
            asyncio.create_task(market_ws.run(stop_event), name="market_ws"),
            asyncio.create_task(sports_ws.run(stop_event), name="sports_ws"),
            asyncio.create_task(rtds_ws.run(stop_event), name="rtds_ws"),
            asyncio.create_task(compactor.run_loop(stop_event), name="compactor"),
            asyncio.create_task(subscription_updater(), name="sub_updater"),
            asyncio.create_task(state_saver(), name="state_saver"),
            asyncio.create_task(health_logger(), name="health_logger"),
        ]


        try:
            # Wait for stop signal (KeyboardInterrupt on Windows)
            await stop_event.wait()
        except asyncio.CancelledError:
            stop_event.set()
        finally:
            stop_event.set()
            log.info("shutting down, waiting for tasks...")

            # Give tasks time to finish gracefully
            done, pending = await asyncio.wait(tasks, timeout=10.0)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.wait(pending, timeout=5.0)

            # Flush and close all writers
            for w in writers.values():
                try:
                    await w.flush()
                    await w.close()
                except Exception:
                    pass

            # Final state save
            await state.save()

            log.info("pm-fetcher stopped")


def cli_entry() -> None:
    """CLI entry point for `python -m pm_fetcher` and console script."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
