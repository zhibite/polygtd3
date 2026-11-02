"""Polymarket CLOB Market WebSocket feed.

Subscribes to:  wss://ws-subscriptions-clob.polymarket.com/ws/market

Provides:
  - PriceCache: last_trade_price / midpoint per token_id
  - Tick size change notifications

H-NEW-2026-07-11: orderbook parsing (best_bid / best_ask / book events) has
been removed. The strategy now trades off REST midpoint + WS last_trade_price
only — see StrategyEngine / ActionRouter docs.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

import websockets
# `websockets.State` is only exported at the top level from v14 onward;
# `websockets.protocol.State` is stable across 11–15.
from websockets.protocol import State as WsState

from src.infra.logging import get_logger

log = get_logger(__name__)

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class PriceCache:
    """Per-token price snapshot, updated by MarketWS and REST polling."""

    last_trade_price: dict[str, float] = field(default_factory=dict)
    tick_size: dict[str, float] = field(default_factory=dict)
    midpoint: dict[str, float] = field(default_factory=dict)  # REST /midpoint poller
    # Microstructure snapshot for the external-signal gates (2026-11-02).
    best_bid: dict[str, float] = field(default_factory=dict)
    best_ask: dict[str, float] = field(default_factory=dict)
    bid_depth: dict[str, float] = field(default_factory=dict)
    ask_depth: dict[str, float] = field(default_factory=dict)
    # token -> deque[(ts_seconds, signed_shares)] over the last few minutes
    trade_flow: dict[str, deque[tuple[float, float]]] = field(
        default_factory=dict
    )

    def last(self, token: str) -> float:
        return self.last_trade_price.get(token, 0.0)

    def mid(self, token: str) -> float:
        return self.midpoint.get(token, 0.0)

    def get_midpoint(self, token: str) -> float:
        """REST midpoint price — more stable than last_trade_price for strategy decisions."""
        return self.midpoint.get(token, 0.0)

    def set_best_bid_ask(
        self, token: str, bid: object, ask: object
    ) -> None:
        """Update the top of book from a WS BBA / price_change event."""
        try:
            b = float(bid)  # type: ignore[arg-type]
            a = float(ask)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return
        if b > 0:
            self.best_bid[token] = b
        if a > 0:
            self.best_ask[token] = a

    def set_book(
        self, token: str, bids: object, asks: object, levels: int = 5
    ) -> None:
        """Store top-of-book depth from a REST /book response."""
        def _levels(side: object) -> tuple[float, float]:
            if not isinstance(side, list):
                return 0.0, 0.0
            best = 0.0
            depth = 0.0
            for row in side[:levels]:
                if not isinstance(row, dict):
                    continue
                try:
                    px = float(row.get("price", 0))
                    sz = float(row.get("size", 0))
                except (TypeError, ValueError):
                    continue
                if px <= 0 or sz <= 0:
                    continue
                if best <= 0:
                    best = px
                depth += sz
            return best, depth

        best_bid, bid_depth = _levels(bids)
        best_ask, ask_depth = _levels(asks)
        if best_bid > 0:
            self.best_bid[token] = best_bid
            self.bid_depth[token] = bid_depth
        if best_ask > 0:
            self.best_ask[token] = best_ask
            self.ask_depth[token] = ask_depth

    def record_trade(
        self, token: str, signed_size: float, ts: float | None = None
    ) -> None:
        """Append a signed trade (BUY +, SELL -) for the CVD signal."""
        if not token or signed_size == 0:
            return
        now = time.time() if ts is None else float(ts)
        dq = self.trade_flow.get(token)
        if dq is None:
            dq = deque()
            self.trade_flow[token] = dq
        dq.append((now, float(signed_size)))
        cutoff = now - 300.0
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        while len(dq) > 5000:
            dq.popleft()

    def flow(self, token: str, window_s: float = 60.0) -> tuple[float, float]:
        """Signed volume and total absolute volume over the window."""
        dq = self.trade_flow.get(token)
        if not dq:
            return 0.0, 0.0
        cutoff = time.time() - max(1.0, float(window_s))
        cvd = 0.0
        total = 0.0
        for ts, size in dq:
            if ts < cutoff:
                continue
            cvd += size
            total += abs(size)
        return cvd, total

    def spread(self, token: str) -> float | None:
        bid = self.best_bid.get(token, 0.0)
        ask = self.best_ask.get(token, 0.0)
        if bid <= 0 or ask <= 0 or ask < bid:
            return None
        mid = (bid + ask) / 2.0
        if mid <= 0:
            return None
        return (ask - bid) / mid

    def book_imbalance(self, token: str) -> float | None:
        bid_depth = self.bid_depth.get(token, 0.0)
        ask_depth = self.ask_depth.get(token, 0.0)
        total = bid_depth + ask_depth
        if total <= 0:
            return None
        return (bid_depth - ask_depth) / total

    def clear_token(self, token: str) -> None:
        """Clear cached data for a specific token (e.g., on window change)."""
        self.last_trade_price.pop(token, None)
        self.tick_size.pop(token, None)
        self.midpoint.pop(token, None)
        self.best_bid.pop(token, None)
        self.best_ask.pop(token, None)
        self.bid_depth.pop(token, None)
        self.ask_depth.pop(token, None)
        self.trade_flow.pop(token, None)


MarketWSConnected = Callable[[], None]
MarketWSDisconnected = Callable[[], None]


class MarketWS:
    """Async WebSocket consumer for CLOB market data."""

    def __init__(
        self,
        cache: PriceCache,
        on_connect: MarketWSConnected | None = None,
        on_disconnect: MarketWSDisconnected | None = None,
    ) -> None:
        self._cache = cache
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        # H-V6-10: dump the first N raw WS messages so we can see what
        # Polymarket is actually sending. Once we know the real shape, the
        # diagnostic can be turned off. The 10x log-flood cap is to keep
        # the firehose manageable in production.
        self._raw_dump_remaining: int = 5
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._url = MARKET_WS_URL
        self._pending_tokens: set[str] = set()
        self._ws: websockets.WebSocketClientProtocol | None = None
        # Expired-window tokens cannot be unsubscribed from an open
        # Polymarket channel; prune them locally, filter their events and
        # cycle the socket after enough have accumulated. Used by the main
        # 15m feed and by the read-only 5m signal feed.
        self._pruned_since_connect = 0
        self._force_reconnect_pending = False

    _RECONNECT_AFTER_PRUNED = 128

    def set_active_tokens(self, token_ids: set[str]) -> None:
        """Replace the active subscription set and subscribe any new ids."""
        active = {t for t in token_ids if t}
        removed = self._pending_tokens - active
        new_tokens = active - self._pending_tokens
        self._pending_tokens = active
        self._pruned_since_connect += len(removed)
        if (
            new_tokens
            and self._ws is not None
            and self._ws.state == WsState.OPEN
        ):
            asyncio.create_task(self._subscribe(self._ws, list(new_tokens)))
        if (
            removed
            and self._pruned_since_connect >= self._RECONNECT_AFTER_PRUNED
            and self._ws is not None
            and self._ws.state == WsState.OPEN
            and not self._force_reconnect_pending
        ):
            self._force_reconnect_pending = True
            asyncio.create_task(self._cycle_socket())

    async def _cycle_socket(self) -> None:
        ws = self._ws
        try:
            if ws is not None:
                await ws.close()
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._force_reconnect_pending = False
            self._pruned_since_connect = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe_tokens(self, token_ids: list[str]) -> None:
        """Queue token subscriptions and send immediately if WS is connected."""
        new_tokens = [tid for tid in token_ids if tid not in self._pending_tokens]
        for tid in new_tokens:
            self._pending_tokens.add(tid)
        if new_tokens and self._ws is not None and self._ws.state == WsState.OPEN:
            # WS is already connected — send subscription right away
            asyncio.create_task(self._subscribe(self._ws, new_tokens))

    @property
    def subscribed_tokens(self) -> frozenset[str]:
        return frozenset(self._pending_tokens)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("market_ws.started", url=self._url)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("market_ws.stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                async with websockets.connect(self._url, ping_interval=10) as ws:
                    self._ws = ws
                    delay = 1.0
                    log.debug("market_ws.connected")

                    if self._pending_tokens:
                        await self._subscribe(ws, list(self._pending_tokens))

                    if self._on_connect:
                        asyncio.create_task(self._on_connect())

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._process(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("market_ws.error", error=str(exc), retry_in_s=delay)
                if self._on_disconnect:
                    self._on_disconnect()
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
            finally:
                self._ws = None

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol, tokens: list[str]) -> None:
        payload = {
            "type": "market",
            "assets_ids": tokens,
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(payload))
        log.info("market_ws.subscribed", tokens=[t[:16] for t in tokens])

    @staticmethod
    def _signed_size(payload: dict) -> float:
        """Per-event trade size, positive for BUY and negative for SELL."""
        try:
            size = float(payload.get("size", 0) or 0)
        except (TypeError, ValueError):
            return 0.0
        side = str(payload.get("side", "") or "").strip().upper()
        if side == "BUY":
            return size
        if side == "SELL":
            return -size
        return 0.0

    async def _process(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Skip non-dict messages (some WS messages may be arrays)
        if not isinstance(msg, dict):
            return

        # H-V6-10 diagnostic: dump the first few raw messages so we can
        # confirm the real Polymarket wire shape. Production symptom
        # 2026-07-11 01:30 round 01:30 — STATUS shows bids=0.0000 for
        # every token every tick, no `best_bid_ask` event was ever
        # parsed; we need the raw JSON to verify what to feed the parser.
        if self._raw_dump_remaining > 0:
            self._raw_dump_remaining -= 1
            log.info(
                "market_ws.raw_sample",
                keys=sorted(msg.keys()),
                asset_id=msg.get("asset_id"),
                event_type=msg.get("event_type"),
                payload=str(msg)[:400],
            )

        # ── Polymarket Market Channel payload schema (relevant subset) ────────
        #   price_change:          { event_type, market,
        #                            price_changes: [{ asset_id, price, side,
        #                            size, ... }], timestamp }
        #   last_trade_price:      { event_type, asset_id, price, side, size,
        #                            fee_rate_bps, timestamp }
        #
        # orderbook events (best_bid_ask / book / bba dict) are NOT parsed —
        # the strategy trades off REST midpoint + WS last_trade_price only.
        event_type = msg.get("event_type") or msg.get("type") or ""

        # Extract token id — last_trade_price uses top-level `asset_id`;
        # price_change puts it inside each price_changes[].
        token = (
            msg.get("asset_id")
            or msg.get("token")
            or ""
        )
        market_field = msg.get("market")
        if not token and isinstance(market_field, dict):
            token = market_field.get("asset_id") or market_field.get("assetID") or ""

        # ── dedicated last_trade_price event ─────────────────────────────────
        # Schema: { event_type: "last_trade_price", asset_id, price, ... }
        # The price is a TOP-LEVEL `price` field. The old parser only looked
        # for a `last_trade_price` field, which this event never carries, so
        # every last-trade event was dropped.
        in_active = (
            not self._pending_tokens or token in self._pending_tokens
        )
        if event_type == "last_trade_price" and in_active:
            price_raw = msg.get("price")
            if price_raw not in (None, "") and token:
                try:
                    self._cache.last_trade_price[token] = float(price_raw)
                except (TypeError, ValueError):
                    pass
                self._cache.record_trade(token, self._signed_size(msg))

        # ── best_bid_ask / book ─────────────────────────────────────────────
        # Re-enabled for the microstructure signal gates. Older bot
        # versions deliberately ignored these events; the strategy still
        # trades off midpoint + last trade, but the signal layer uses the
        # top of book and depth.
        if event_type in ("best_bid_ask", "bbo") and token and in_active:
            bb = msg.get("best_bid") or msg.get("bestBid")
            ba = msg.get("best_ask") or msg.get("bestAsk")
            if bb is not None or ba is not None:
                self._cache.set_best_bid_ask(token, bb, ba)
        elif event_type == "book" and token and in_active:
            self._cache.set_book(
                token,
                msg.get("bids") or msg.get("buys"),
                msg.get("asks") or msg.get("sells"),
            )

        # price_change → per-asset updates (each carries its own best_bid/ask)
        pcs = msg.get("price_changes", [])
        if isinstance(pcs, list):
            for pc in pcs:
                if not isinstance(pc, dict):
                    continue
                pc_token = pc.get("asset_id") or token
                if not pc_token:
                    continue
                if self._pending_tokens and pc_token not in self._pending_tokens:
                    continue
                # Bump last_trade_price from price_changes too (these are
                # trade executions, the price is the actual fill).
                pc_price = pc.get("price")
                if pc_price is not None and pc_price != "":
                    try:
                        self._cache.last_trade_price[pc_token] = float(pc_price)
                    except (TypeError, ValueError):
                        pass
                    self._cache.record_trade(
                        pc_token, self._signed_size(pc)
                    )
                bb = pc.get("best_bid")
                ba = pc.get("best_ask")
                if bb is not None or ba is not None:
                    self._cache.set_best_bid_ask(pc_token, bb, ba)


        # last_trade_price (top-level field on dedicated event_type)
        if not in_active:
            return
        ltp = msg.get("last_trade_price")
        if isinstance(ltp, list) and len(ltp) >= 1:
            try:
                self._cache.last_trade_price[token] = float(ltp[0])
            except (TypeError, ValueError):
                pass
        elif isinstance(ltp, dict):
            price = ltp.get("price") or ltp.get("last_trade_price")
            if price not in (None, ""):
                try:
                    self._cache.last_trade_price[token] = float(price)
                except (TypeError, ValueError):
                    pass
        elif isinstance(ltp, str) and ltp and token:
            # top-level last_trade_price event: { event_type, asset_id,
            # price, side, size, fee_rate_bps, timestamp }
            try:
                self._cache.last_trade_price[token] = float(ltp)
            except (TypeError, ValueError):
                pass
