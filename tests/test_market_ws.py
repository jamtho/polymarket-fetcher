# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call

import orjson
import pytest
import pytest_asyncio

from pm_fetcher.websockets.market_ws import MarketWebSocket


class FakeState:
    """Minimal State stub with configurable active token IDs."""

    def __init__(self, token_ids: list[str] | None = None) -> None:
        self._ids = token_ids or []

    @property
    def active_token_ids(self) -> list[str]:
        return self._ids


def _make_market_ws(
    writers: dict[str, AsyncMock],
    state: FakeState | None = None,
    batch_size: int = 200,
) -> MarketWebSocket:
    return MarketWebSocket(
        url="wss://example.com/ws/market",
        writers=writers,
        state=state or FakeState(),
        batch_size=batch_size,
    )


# --- Subscribe message format ---


class TestSubscribeFormat:
    @pytest.mark.asyncio
    async def test_initial_subscribe_uses_type_market(self, mock_writer, mock_ws):
        """First subscription must use {"type": "market"} format."""
        state = FakeState(["token_a", "token_b"])
        ws_client = _make_market_ws({"book": mock_writer}, state)
        ws_client._ws = mock_ws
        ws_client._connected = True

        await ws_client.on_connect(mock_ws)

        sent = orjson.loads(mock_ws.send_str.call_args[0][0])
        assert sent["type"] == "market"
        assert sent["assets_ids"] == ["token_a", "token_b"]
        assert sent["custom_feature_enabled"] is True
        assert "channel" not in sent
        assert "operation" not in sent

    @pytest.mark.asyncio
    async def test_dynamic_subscribe_uses_operation(self, mock_writer, mock_ws):
        """Subsequent subscriptions must use {"operation": "subscribe"} format."""
        state = FakeState(["token_a"])
        ws_client = _make_market_ws({"book": mock_writer}, state)
        ws_client._ws = mock_ws
        ws_client._connected = True

        # Initial subscribe
        await ws_client.on_connect(mock_ws)
        mock_ws.send_str.reset_mock()

        # Add new tokens dynamically
        state._ids = ["token_a", "token_b", "token_c"]
        await ws_client.update_subscriptions()

        sent = orjson.loads(mock_ws.send_str.call_args[0][0])
        assert sent["operation"] == "subscribe"
        assert sorted(sent["assets_ids"]) == ["token_b", "token_c"]
        assert sent["custom_feature_enabled"] is True
        assert "type" not in sent

    @pytest.mark.asyncio
    async def test_no_duplicate_subscriptions(self, mock_writer, mock_ws):
        """Already-subscribed IDs should not be re-sent."""
        state = FakeState(["token_a", "token_b"])
        ws_client = _make_market_ws({"book": mock_writer}, state)
        ws_client._ws = mock_ws
        ws_client._connected = True

        await ws_client.on_connect(mock_ws)
        mock_ws.send_str.reset_mock()

        # Same tokens, no new ones
        await ws_client.update_subscriptions()
        mock_ws.send_str.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_state_sends_nothing(self, mock_writer, mock_ws):
        """No tokens = no subscribe messages."""
        ws_client = _make_market_ws({"book": mock_writer}, FakeState([]))
        ws_client._ws = mock_ws
        ws_client._connected = True

        await ws_client.on_connect(mock_ws)
        mock_ws.send_str.assert_not_called()


# --- Batching ---


class TestBatching:
    @pytest.mark.asyncio
    async def test_batches_by_size(self, mock_writer, mock_ws):
        """Tokens should be split into batches of batch_size."""
        ids = [f"t{i}" for i in range(5)]
        state = FakeState(ids)
        ws_client = _make_market_ws({"book": mock_writer}, state, batch_size=2)
        ws_client._ws = mock_ws
        ws_client._connected = True

        await ws_client.on_connect(mock_ws)

        # 5 tokens / batch_size 2 = 3 messages
        assert mock_ws.send_str.call_count == 3

        # First batch should use initial format
        first = orjson.loads(mock_ws.send_str.call_args_list[0][0][0])
        assert first["type"] == "market"

        # Subsequent batches should use operation format
        second = orjson.loads(mock_ws.send_str.call_args_list[1][0][0])
        assert second["operation"] == "subscribe"

        third = orjson.loads(mock_ws.send_str.call_args_list[2][0][0])
        assert third["operation"] == "subscribe"


# --- Event detection and routing ---


class TestEventDetection:
    @pytest.mark.parametrize(
        "data, expected",
        [
            ({"event_type": "resolution"}, "resolution"),
            ({"price_changes": []}, "price_change"),
            ({"last_trade_price": 0.5}, "last_trade_price"),
            ({"bids": [], "asks": []}, "book"),
            ({"asks": []}, "book"),
            ({"type": "tick_size_change"}, "tick_size_change"),
            ({"something": "else"}, "unknown"),
        ],
    )
    def test_detect_event_type(self, data: dict, expected: str):
        assert MarketWebSocket._detect_event_type(data) == expected

    @pytest.mark.parametrize(
        "event_type, expected_writer",
        [
            ("book", "book"),
            ("price_change", "price_change"),
            ("last_trade_price", "last_trade_price"),
            ("tick_size_change", "book"),
            ("new_market", "book"),
            ("resolution", "book"),
            ("unknown", "book"),
            ("something_random", "book"),
        ],
    )
    def test_route_event(self, event_type: str, expected_writer: str):
        assert MarketWebSocket._route_event(event_type) == expected_writer


# --- Message routing to writers ---


class TestMessageRouting:
    @pytest.mark.asyncio
    async def test_book_snapshot_list(self, mock_writer):
        """A list of dicts should go to the book writer via write_batch."""
        book_writer = AsyncMock()
        ws_client = _make_market_ws({"book": book_writer})

        await ws_client.on_message([{"asset_id": "a", "bids": [], "asks": []}])
        book_writer.write_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_list_skipped(self, mock_writer):
        """Empty list should not trigger any writes."""
        book_writer = AsyncMock()
        ws_client = _make_market_ws({"book": book_writer})

        await ws_client.on_message([])
        book_writer.write_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_price_change_routed(self):
        """Price change dict should go to price_change writer."""
        pc_writer = AsyncMock()
        book_writer = AsyncMock()
        ws_client = _make_market_ws({"book": book_writer, "price_change": pc_writer})

        await ws_client.on_message({"market": "m1", "price_changes": [{"p": 0.5}]})
        pc_writer.write.assert_called_once()
        book_writer.write.assert_not_called()

    @pytest.mark.asyncio
    async def test_last_trade_routed(self):
        """Last trade dict should go to last_trade_price writer."""
        lt_writer = AsyncMock()
        book_writer = AsyncMock()
        ws_client = _make_market_ws({"book": book_writer, "last_trade_price": lt_writer})

        await ws_client.on_message({"market": "m1", "last_trade_price": 0.42})
        lt_writer.write.assert_called_once()
        book_writer.write.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_event_falls_back_to_book(self):
        """Unknown event types should fall back to book writer."""
        book_writer = AsyncMock()
        ws_client = _make_market_ws({"book": book_writer})

        await ws_client.on_message({"something": "unexpected"})
        book_writer.write.assert_called_once()


# --- Keepalive ---


class TestKeepalive:
    @pytest.mark.asyncio
    async def test_sends_text_ping(self, mock_writer, mock_ws):
        """Market keepalive should send text PING, not WS-level ping."""
        ws_client = _make_market_ws({"book": mock_writer})
        stop = asyncio.Event()

        # Let it send one PING then stop
        async def stop_after_ping():
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(
            ws_client._keepalive(mock_ws, stop),
            stop_after_ping(),
        )

        mock_ws.send_str.assert_called_with("PING")
        mock_ws.ping.assert_not_called()

    def test_pong_in_text_pong_messages(self):
        """PONG should be recognized as a text keepalive response."""
        ws_client = _make_market_ws({"book": AsyncMock()})
        assert "PONG" in ws_client._text_pong_messages
