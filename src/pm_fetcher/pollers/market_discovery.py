# Copyright (c) 2026 James Thompson. All rights reserved.

"""Market discovery poller — crawls Gamma /markets and /events."""

from __future__ import annotations

import structlog

from pm_fetcher.clients.gamma import GammaClient
from pm_fetcher.pollers.base_poller import BasePoller
from pm_fetcher.state import State
from pm_fetcher.storage.json_writer import JsonWriter

log = structlog.get_logger()


class MarketDiscoveryPoller(BasePoller):
    """Discovers new markets and events from the Gamma API.

    Updates state with known market/event/token IDs for use by
    other pollers and WebSocket subscriptions.
    """

    name = "market_discovery"

    def __init__(
        self,
        gamma_client: GammaClient,
        market_writer: JsonWriter,
        event_writer: JsonWriter,
        state: State,
        *,
        min_interval: float = 300.0,
        max_interval: float = 600.0,
    ) -> None:
        super().__init__(market_writer, state, min_interval=min_interval, max_interval=max_interval)
        self._gamma = gamma_client
        self._event_writer = event_writer

    async def poll_once(self) -> bool:
        """Fetch all markets and events, detect new ones."""
        changed = False

        # Fetch markets
        markets = await self._gamma.get_all_markets()
        if markets:
            await self._writer.write_batch(markets)
            market_ids = [m.get("id", "") for m in markets if m.get("id")]
            old_ids = set(self._state.known_market_ids)
            new_ids = set(market_ids) - old_ids

            if new_ids:
                log.info("new markets discovered", count=len(new_ids))
                changed = True

            self._state.known_market_ids = list(set(market_ids) | old_ids)

            # Extract active token IDs for CLOB polling and WS subscriptions
            token_ids: list[str] = []
            for m in markets:
                if m.get("active") and not m.get("closed"):
                    clob_ids = m.get("clobTokenIds")
                    if isinstance(clob_ids, str):
                        # Often stored as JSON string like '["id1","id2"]'
                        import json
                        try:
                            ids = json.loads(clob_ids)
                            if isinstance(ids, list):
                                token_ids.extend(ids)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    elif isinstance(clob_ids, list):
                        token_ids.extend(str(t) for t in clob_ids)

            if token_ids:
                old_tokens = set(self._state.active_token_ids)
                new_tokens = set(token_ids) - old_tokens
                if new_tokens:
                    log.info("new tokens discovered", count=len(new_tokens))
                    changed = True
                self._state.active_token_ids = list(set(token_ids) | old_tokens)

        # Fetch events
        events = await self._gamma.get_all_events()
        if events:
            await self._event_writer.write_batch(events)
            event_ids = [e.get("id", "") for e in events if e.get("id")]
            old_event_ids = set(self._state.known_event_ids)
            new_event_ids = set(event_ids) - old_event_ids

            if new_event_ids:
                log.info("new events discovered", count=len(new_event_ids))
                changed = True

            self._state.known_event_ids = list(set(event_ids) | old_event_ids)

        if changed:
            await self._state.save()

        log.info(
            "discovery poll complete",
            markets=len(markets),
            events=len(events),
            total_tokens=len(self._state.active_token_ids),
        )

        return changed
