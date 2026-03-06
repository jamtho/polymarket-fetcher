# Copyright (c) 2026 James Thompson. All rights reserved.

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from pm_fetcher.websockets.sports_ws import SportsWebSocket


def _make_sports_ws(writer: AsyncMock) -> SportsWebSocket:
    return SportsWebSocket(url="wss://example.com/ws", writer=writer)


class TestTextPing:
    @pytest.mark.asyncio
    async def test_responds_pong_to_ping(self, mock_ws, mock_writer):
        """Sports WS must respond with 'pong' when server sends 'ping'."""
        ws_client = _make_sports_ws(mock_writer)
        await ws_client._on_text_ping(mock_ws, "ping")
        mock_ws.send_str.assert_called_once_with("pong")

    @pytest.mark.asyncio
    async def test_ignores_pong(self, mock_ws, mock_writer):
        """Sports WS should not respond to 'pong' messages."""
        ws_client = _make_sports_ws(mock_writer)
        await ws_client._on_text_ping(mock_ws, "pong")
        mock_ws.send_str.assert_not_called()

    def test_text_pong_messages_set(self, mock_writer):
        """Both 'ping' and 'pong' should be in the skip set."""
        ws_client = _make_sports_ws(mock_writer)
        assert ws_client._text_pong_messages == {"ping", "pong"}


class TestKeepalive:
    @pytest.mark.asyncio
    async def test_keepalive_is_noop(self, mock_ws, mock_writer):
        """Sports keepalive should just wait for stop — no outgoing pings."""
        ws_client = _make_sports_ws(mock_writer)
        stop = asyncio.Event()

        async def stop_soon():
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(
            ws_client._keepalive(mock_ws, stop),
            stop_soon(),
        )

        mock_ws.send_str.assert_not_called()
        mock_ws.ping.assert_not_called()
