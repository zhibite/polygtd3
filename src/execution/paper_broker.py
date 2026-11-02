"""Paper-trading execution engine (2026-11-01).

Every configured market runs the real `StrategyEngine`; this module is its
`ActionHandler`. It simulates Polymarket GTD orders locally instead of
posting to the CLOB:

  * BUY  (T1..T4): a resting GTD at the strategy price. It fills when the
    live REST midpoint crosses down through the limit (maker, fee 0). If
    the mid is already at/below the limit when the order is placed, the
    order is marketable and fills immediately at the mid (taker, fee).
  * SELL (TP):   a resting GTD at the take-profit target. It fills when
    the mid crosses up through the target (maker). If the mid is already
    at/above the target at placement it fills immediately (taker).
  * GTD expiry:  after GTD_LIFETIME_S the order is cancelled. An unfilled
    T1 releases the entry slot so the engine can try again inside the T1
    time window.
  * Round end:   every still-resting order is cancelled. Any inventory
    left unsold stays as BUY fills in SQLite and is booked by the
    dashboard's settlement accounting once Gamma resolves the market
    (a full-ladder loss is recorded in full, per operator spec).

The broker mirrors only the state transitions the engine needs:
`bind_server_order_id` on placement, `on_order_filled` on fills,
`on_order_expired` / placed/watching flags on cancels, and the TP
snapshot bookkeeping (register_tp / record_tp_fill / close_tp_order).
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.config import strategy as _strategy
from src.infra.logging import get_logger
from src.strategy.engine import (
    Action,
    ActionCancelTier,
    ActionOpenT1,
    ActionOpenTN,
    ActionStopLoss,
    ActionTakeProfit,
)

if TYPE_CHECKING:  # pragma: no cover
    from src.strategy.engine import StrategyEngine

log = get_logger(__name__)


@dataclass
class PaperOrder:
    """A simulated order resting on the local paper book."""

    order_id: str
    market: str
    asset: str
    interval: str
    round_start_ts: int
    side: str              # BUY | SELL
    token_id: str
    tier_name: str         # T1..T4 | TP | SL1 | SL2
    price: float
    size: float            # shares
    expires_at: float
    placed_at: float
    tier: int = 0          # 1..4 for BUY tiers, 0 for TP/SL
    status: str = "live"
    size_matched: float = 0.0


def taker_fee(shares: float, price: float, rate: float) -> float:
    """Official Polymarket taker fee: C × rate × p × (1 - p)."""
    return max(0.0, shares) * max(0.0, rate) * max(0.0, price) * max(
        0.0, 1.0 - price
    )


class PaperBroker:
    """Local GTD fill simulator + DB mirror for one (asset, interval) market."""

    def __init__(
        self,
        *,
        market: str,
        asset: str,
        interval: str,
        price_cache: Any,
        db: Any,
        killswitch: Any | None = None,
        taker_fee_rate: float | None = None,
        t1_entry_window_s: float | None = None,
    ) -> None:
        self.market = market
        self.asset = asset
        self.interval = interval
        self._pc = price_cache
        self._db = db
        self._kill = killswitch
        # A T1 GTD may only be live during the market's entry window
        # (15m: 300 s, 5m: 60 s). GTD_LIFETIME_S is 300 s, which is longer
        # than a whole 5m round; without this the order could fill late in
        # the window, after the "enter within 1 minute" cutoff.
        self._t1_entry_window_s = float(t1_entry_window_s or 0.0)
        cfg = _strategy()
        self._taker_fee_rate = float(
            cfg.TAKER_FEE_RATE if taker_fee_rate is None else taker_fee_rate
        )
        self._gtd_lifetime_s = int(cfg.GTD_LIFETIME_S)
        self.engine: "StrategyEngine | None" = None
        self.round_start_ts: int = 0
        self._orders: dict[str, PaperOrder] = {}
        self._round_meta: dict[str, Any] = {}
        self._round_row_written = False

    # ------------------------------------------------------------------
    # Round lifecycle
    # ------------------------------------------------------------------

    def begin_round(self, round_start_ts: int, market_info: Any = None) -> None:
        self.round_start_ts = int(round_start_ts)
        self._round_row_written = False
        self._round_meta = self._meta_from_market(round_start_ts, market_info)

    def set_round_meta(self, round_start_ts: int, market_info: Any = None) -> None:
        """Late discovery: refresh slug / condition / token ids."""
        if int(round_start_ts) != self.round_start_ts:
            return
        self._round_meta = self._meta_from_market(round_start_ts, market_info)
        if self._round_row_written:
            self._ensure_round_row()

    def _meta_from_market(self, round_start_ts: int, mkt: Any) -> dict[str, Any]:
        return {
            "round_start_ts": int(round_start_ts),
            "slug": getattr(mkt, "slug", None) if mkt else None,
            "condition_id": getattr(mkt, "condition_id", None) if mkt else None,
            "up_token": getattr(mkt, "token_up_id", None) if mkt else None,
            "down_token": getattr(mkt, "token_down_id", None) if mkt else None,
            "tick_size": getattr(mkt, "tick_size", 0.01) if mkt else 0.01,
        }

    async def cancel_round(self, round_start_ts: int) -> int:
        """Cancel every resting order belonging to a finished round."""
        targets = [
            o for o in self._orders.values()
            if o.round_start_ts == int(round_start_ts)
        ]
        for order in targets:
            await self._cancel_order(order, status="cancelled",
                                     reason="round_end")
        return len(targets)

    async def cancel_all_open(self, reason: str = "killswitch") -> int:
        targets = list(self._orders.values())
        for order in targets:
            await self._cancel_order(order, status="cancelled", reason=reason)
        return len(targets)

    def open_orders_for_round(self, round_start_ts: int) -> list[PaperOrder]:
        return [
            o for o in self._orders.values()
            if o.round_start_ts == int(round_start_ts) and o.status == "live"
        ]

    # ------------------------------------------------------------------
    # Action handling
    # ------------------------------------------------------------------

    async def __call__(self, action: Action) -> None:
        eng = self.engine
        if eng is None:
            return
        ks = self._kill
        if ks is not None and getattr(ks, "active", False):
            log.warning(
                "paper.blocked_by_killswitch",
                market=self.market,
                action=type(action).__name__,
            )
            # Release every engine latch the dropped action would have
            # held. Without this, T1 (entry_token) and TP (_tp_inflight)
            # stay latched forever and never re-arm after "恢复运行".
            if eng._round is not None:
                pos = eng._round.position
                if isinstance(action, ActionStopLoss):
                    eng._stop_loss_inflight = False
                    eng._stop_loss_retry_after_ts = 0.0
                elif isinstance(action, ActionTakeProfit):
                    pos._tp_inflight = False
                elif isinstance(action, ActionOpenT1):
                    pos.entry_side = None
                    pos.entry_token = None
            return

        if isinstance(action, ActionOpenT1):
            # T1 is a GTD at the strategy price, like T2-T4. `_place_buy`
            # fills a marketable limit immediately as a taker at the mid;
            # otherwise it rests until filled or the GTD expires.
            self._place_buy(action, tier=1, tier_name="T1")
        elif isinstance(action, ActionOpenTN):
            self._place_buy(
                action, tier=action.tier, tier_name=f"T{action.tier}"
            )
        elif isinstance(action, ActionTakeProfit):
            self._take_profit(action)
        elif isinstance(action, ActionStopLoss):
            self._stop_loss(action)
        elif isinstance(action, ActionCancelTier):
            await self._cancel_action(action)

    # ------------------------------------------------------------------
    # BUY side
    # ------------------------------------------------------------------

    def _place_buy(self, action: Any, *, tier: int, tier_name: str) -> None:
        eng = self.engine
        if eng is None or eng._round is None:
            return
        if eng._round.round_start_ts != self.round_start_ts:
            return
        price = float(action.price)
        notional = float(action.notional)
        if price <= 0 or notional <= 0:
            return
        size = notional / price
        order_id = self._new_order_id()
        expires_at = time.time() + self._gtd_lifetime_s
        eng.bind_server_order_id(
            tier=tier,
            server_order_id=order_id,
            token_id=action.token,
            expires_at=expires_at,
            expected_price=price,
            placed_size=size,
        )
        tier_snap = eng._round.position.tiers[tier - 1]
        tier_snap.watching = True
        order = PaperOrder(
            order_id=order_id,
            market=self.market,
            asset=self.asset,
            interval=self.interval,
            round_start_ts=self.round_start_ts,
            side="BUY",
            token_id=action.token,
            tier_name=tier_name,
            price=price,
            size=size,
            expires_at=expires_at,
            placed_at=time.time(),
            tier=tier,
        )
        self._orders[order_id] = order
        self._ensure_round_row()
        self._db_insert_order(order)
        log.info(
            "paper.order_placed",
            market=self.market,
            tier=tier_name,
            side="BUY",
            token=action.token[:16],
            price=price,
            shares=round(size, 4),
            notional=round(notional, 2),
            expires_in_s=self._gtd_lifetime_s,
        )
        # Marketable at placement → immediate taker fill at the mid. This
        # is the GTD equivalent of crossing the book.
        mid = self._mid(action.token)
        if mid > 0 and mid <= price:
            self._fill_buy(order, fill_price=min(mid, price), role="TAKER")

    def _fill_buy(self, order: PaperOrder, fill_price: float, role: str) -> None:
        eng = self.engine
        if eng is None:
            return
        if eng._round is None:
            # Round already tore down; drop the orphan instead of retrying
            # it forever on every tick.
            self._orders.pop(order.order_id, None)
            order.status = "cancelled"
            self._db_update_status(order, "cancelled")
            return
        if self._orders.pop(order.order_id, None) is None:
            return
        fill_price = max(0.0, min(1.0, float(fill_price)))
        fee = (
            taker_fee(order.size, fill_price, self._taker_fee_rate)
            if role == "TAKER"
            else 0.0
        )
        order.status = "filled"
        order.size_matched = order.size
        self._db_fill(order, fill_price, order.size, role, fee)
        same_round = eng._round.round_start_ts == order.round_start_ts
        if same_round:
            tier_snap = eng._round.position.tiers[order.tier - 1]
            tier_snap.watching = False
            eng.on_order_filled(
                order.token_id, order.order_id, fill_price, order.size
            )
        log.info(
            "paper.fill",
            market=self.market,
            tier=order.tier_name,
            side="BUY",
            token=order.token_id[:16],
            price=fill_price,
            shares=round(order.size, 4),
            role=role,
            fee=round(fee, 6),
            stale_round=not same_round,
        )

    # ------------------------------------------------------------------
    # SELL side (take-profit / stop-loss)
    # ------------------------------------------------------------------

    def _take_profit(self, action: ActionTakeProfit) -> None:
        eng = self.engine
        if eng is None or eng._round is None:
            return
        if eng._round.round_start_ts != self.round_start_ts:
            return
        pos = eng._round.position
        qty = float(action.qty)
        if qty <= 1e-9:
            pos._tp_inflight = False
            return
        order_id = self._new_order_id()
        price = float(action.price)
        pos.register_tp(order_id, action.token, qty)
        order = PaperOrder(
            order_id=order_id,
            market=self.market,
            asset=self.asset,
            interval=self.interval,
            round_start_ts=self.round_start_ts,
            side="SELL",
            token_id=action.token,
            tier_name="TP",
            price=price,
            size=qty,
            expires_at=time.time() + self._gtd_lifetime_s,
            placed_at=time.time(),
            tier=0,
        )
        self._orders[order_id] = order
        self._ensure_round_row()
        self._db_insert_order(order)
        eng.notify_tp_recovered()
        log.info(
            "paper.order_placed",
            market=self.market,
            tier="TP",
            side="SELL",
            token=action.token[:16],
            price=price,
            shares=round(qty, 4),
        )
        mid = self._mid(action.token)
        if action.taker:
            # Compact position (<5 shares). The live router exits with a
            # FAK market sell; simulate that at the mid.
            if mid > 0:
                self._fill_sell(order, fill_price=mid, role="TAKER")
            else:
                self._cancel_order_sync(order, status="cancelled",
                                        reason="tp_no_mid")
            return
        if mid > 0 and mid >= price:
            self._fill_sell(order, fill_price=price, role="TAKER")

    def _stop_loss(self, action: ActionStopLoss) -> None:
        eng = self.engine
        if eng is None or eng._round is None:
            return
        if int(action.round_start_ts) != eng._round.round_start_ts:
            eng._stop_loss_inflight = False
            return
        pos = eng._round.position
        order_id = self._new_order_id()
        pos.register_tp(order_id, action.token, float(action.qty))
        order = PaperOrder(
            order_id=order_id,
            market=self.market,
            asset=self.asset,
            interval=self.interval,
            round_start_ts=self.round_start_ts,
            side="SELL",
            token_id=action.token,
            tier_name=f"SL{action.stage}",
            price=float(action.price),
            size=float(action.qty),
            expires_at=time.time(),
            placed_at=time.time(),
            tier=0,
        )
        self._orders[order_id] = order
        self._ensure_round_row()
        self._db_insert_order(order)
        mid = self._mid(action.token)
        fill_px = mid if mid > 0 else float(action.price)
        self._fill_sell(
            order,
            fill_price=fill_px,
            role="TAKER",
            stop_stage=int(action.stage),
        )

    def _fill_sell(
        self,
        order: PaperOrder,
        fill_price: float,
        role: str,
        stop_stage: int | None = None,
    ) -> None:
        eng = self.engine
        if eng is None:
            return
        if eng._round is None:
            self._orders.pop(order.order_id, None)
            order.status = "cancelled"
            self._db_update_status(order, "cancelled")
            return
        if self._orders.pop(order.order_id, None) is None:
            return
        fill_price = max(0.0, min(1.0, float(fill_price)))
        fee = (
            taker_fee(order.size, fill_price, self._taker_fee_rate)
            if role == "TAKER"
            else 0.0
        )
        order.status = "filled"
        order.size_matched = order.size
        self._db_fill(order, fill_price, order.size, role, fee)
        same_round = eng._round.round_start_ts == order.round_start_ts
        remaining = 0.0
        if same_round:
            pos = eng._round.position
            pos.record_tp_fill(order.order_id, fill_price, order.size)
            if pos.total_qty <= 1e-9:
                # A TP exit is FINAL for the window (engine keeps
                # entry_token deliberately). Drop every other resting
                # order so a stale BUY GTD cannot re-open a position
                # after the take-profit, and keep the T1 slot locked.
                self._cancel_resting_after_close(
                    order.round_start_ts, keep_entry_locked=True
                )
                pos.realize_position_closed(
                    source=f"paper.{order.tier_name}",
                    note="paper sell closed the position",
                )
            remaining = pos.total_qty
            if stop_stage is not None:
                eng.notify_stop_filled(stop_stage, order.size)
            else:
                eng.notify_tp_recovered()
        log.info(
            "paper.fill",
            market=self.market,
            tier=order.tier_name,
            side="SELL",
            token=order.token_id[:16],
            price=fill_price,
            shares=round(order.size, 4),
            role=role,
            fee=round(fee, 6),
            remaining_qty=round(remaining, 4),
            stale_round=not same_round,
        )

    # ------------------------------------------------------------------
    # Cancels / expiry
    # ------------------------------------------------------------------

    async def _cancel_action(self, action: ActionCancelTier) -> None:
        if action.tier < 0:
            pos = (
                self.engine._round.position
                if self.engine and self.engine._round
                else None
            )
            order = self._orders.get(action.server_order_id)
            if order is not None:
                await self._cancel_order(
                    order, status="cancelled", reason=action.reason
                )
            elif pos is not None:
                # The order already reached a terminal state; just release
                # the TP slot so the engine can re-arm.
                pos.close_tp_order(action.server_order_id)
            return
        order = self._orders.get(action.server_order_id)
        if order is not None:
            await self._cancel_order(
                order, status="cancelled", reason=action.reason
            )
        else:
            # Defensive: the slot may be bound to an order we already
            # dropped; clear the flags so the tier is not wedged.
            self._clear_buy_tier(action.tier, action.server_order_id)

    async def _cancel_order(
        self, order: PaperOrder, *, status: str, reason: str
    ) -> None:
        if self._orders.pop(order.order_id, None) is None:
            return
        order.status = status
        self._db_update_status(order, status)
        if order.side == "BUY":
            self._clear_buy_tier(order.tier, order.order_id)
        else:
            pos = (
                self.engine._round.position
                if self.engine and self.engine._round
                else None
            )
            if pos is not None:
                pos.close_tp_order(order.order_id)
        log.info(
            "paper.order_cancelled",
            market=self.market,
            tier=order.tier_name,
            side=order.side,
            order_id=order.order_id[:20],
            status=status,
            reason=reason,
        )

    def _cancel_order_sync(
        self, order: PaperOrder, *, status: str, reason: str
    ) -> None:
        # Cancel path used from sync helpers (no awaits inside).
        if self._orders.pop(order.order_id, None) is None:
            return
        order.status = status
        self._db_update_status(order, status)
        if order.side == "BUY":
            self._clear_buy_tier(order.tier, order.order_id)
        else:
            pos = (
                self.engine._round.position
                if self.engine and self.engine._round
                else None
            )
            if pos is not None:
                pos.close_tp_order(order.order_id)
        log.info(
            "paper.order_cancelled",
            market=self.market,
            tier=order.tier_name,
            side=order.side,
            order_id=order.order_id[:20],
            status=status,
            reason=reason,
        )

    def _cancel_resting_after_close(
        self, round_start_ts: int, *, keep_entry_locked: bool
    ) -> None:
        """Cancel leftover resting orders once the position is flat."""
        for order in list(self._orders.values()):
            if order.round_start_ts != round_start_ts:
                continue
            if self._orders.pop(order.order_id, None) is None:
                continue
            order.status = "cancelled"
            self._db_update_status(order, "cancelled")
            if order.side == "BUY":
                self._clear_buy_tier(
                    order.tier,
                    order.order_id,
                    allow_reentry=not keep_entry_locked,
                )
            else:
                pos = (
                    self.engine._round.position
                    if self.engine and self.engine._round
                    else None
                )
                if pos is not None:
                    pos.close_tp_order(order.order_id)

    def _clear_buy_tier(
        self,
        tier: int,
        server_order_id: str,
        *,
        allow_reentry: bool = True,
    ) -> None:
        eng = self.engine
        if eng is None or eng._round is None or not (1 <= tier <= 4):
            return
        snap = eng._round.position.tiers[tier - 1]
        if snap.server_order_id and snap.server_order_id != server_order_id:
            return
        snap.placed = False
        snap.watching = False
        snap.expires_at = 0.0
        snap.cancel_pending = False
        if tier == 1 and not snap.is_complete and allow_reentry:
            # A T1 GTD that expires/cancels without filling frees the
            # entry slot; otherwise `entry_token` would block a retry for
            # the rest of the window. NOT done after a TP exit, where the
            # entry slot must stay locked until the next window.
            if eng._round.position.total_qty <= 1e-9:
                eng._round.position.entry_side = None
                eng._round.position.entry_token = None
                eng._round.position.baseline_target_price = None

    # ------------------------------------------------------------------
    # Tick — fill resting orders / expire GTDs
    # ------------------------------------------------------------------

    def tick(self, now: float | None = None) -> None:
        """Evaluate every resting paper order against the live midpoint."""
        if not self._orders:
            return
        now = time.time() if now is None else float(now)
        for order in list(self._orders.values()):
            if order.order_id not in self._orders:
                continue
            # T1 entry deadline: do not let a pre-armed first entry fill
            # after the market's T1 window has closed.
            if (
                order.side == "BUY"
                and order.tier == 1
                and self._t1_entry_window_s > 0
                and now > order.round_start_ts + self._t1_entry_window_s
            ):
                self._expire(order)
                continue
            mid = self._mid(order.token_id)
            if mid > 0:
                if order.side == "BUY" and mid <= order.price:
                    self._fill_buy(order, fill_price=order.price, role="MAKER")
                    continue
                if order.side == "SELL" and mid >= order.price:
                    self._fill_sell(order, fill_price=order.price, role="MAKER")
                    continue
            if now >= order.expires_at:
                self._expire(order)

    def _expire(self, order: PaperOrder) -> None:
        expired = self._orders.pop(order.order_id, None)
        if expired is None:
            return
        order.status = "expired"
        self._db_update_status(order, "expired")
        if order.side == "BUY":
            self._clear_buy_tier(order.tier, order.order_id)
            eng = self.engine
            if eng is not None and eng._round is not None:
                eng.on_order_expired(order.order_id)
        else:
            pos = (
                self.engine._round.position
                if self.engine and self.engine._round
                else None
            )
            if pos is not None:
                pos.close_tp_order(order.order_id)
        log.info(
            "paper.order_expired",
            market=self.market,
            tier=order.tier_name,
            side=order.side,
            order_id=order.order_id[:20],
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _mid(self, token_id: str) -> float:
        try:
            return float(self._pc.get_midpoint(token_id) or 0.0)
        except Exception:  # noqa: BLE001
            return 0.0

    def _new_order_id(self) -> str:
        return f"paper-{self.market.lower()}-{uuid.uuid4().hex[:20]}"

    def _ensure_round_row(self) -> None:
        if self._round_row_written or self._db is None:
            return
        meta = self._round_meta or {
            "round_start_ts": self.round_start_ts,
            "slug": None,
            "condition_id": None,
            "up_token": None,
            "down_token": None,
            "tick_size": 0.01,
        }
        try:
            self._db.upsert_round(
                round_start_ts=meta["round_start_ts"],
                slug=meta.get("slug"),
                condition_id=meta.get("condition_id"),
                up_token=meta.get("up_token"),
                down_token=meta.get("down_token"),
                tick_size=float(meta.get("tick_size") or 0.01),
                mode="paper",
            )
            self._round_row_written = True
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "paper.round_row_failed", market=self.market, error=str(exc)
            )

    def _db_insert_order(self, order: PaperOrder) -> None:
        if self._db is None:
            return
        try:
            self._db.insert_order(
                client_order_id=order.order_id,
                server_order_id=order.order_id,
                round_start_ts=order.round_start_ts,
                side=order.side,
                token_id=order.token_id,
                tier_name=order.tier_name,
                price=order.price,
                size=order.size,
                lifetime_s=self._gtd_lifetime_s,
                expires_at=int(order.expires_at),
                placed_at=int(order.placed_at),
                mode="paper",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "paper.order_row_failed", market=self.market, error=str(exc)
            )

    def _db_update_status(self, order: PaperOrder, status: str) -> None:
        if self._db is None:
            return
        try:
            self._db.update_order_status(
                order.order_id, status, order.size_matched
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "paper.order_status_failed", market=self.market, error=str(exc)
            )

    def _db_fill(
        self,
        order: PaperOrder,
        price: float,
        size: float,
        role: str,
        fee: float,
    ) -> None:
        if self._db is None:
            return
        try:
            self._db.insert_fill(
                fill_id=uuid.uuid4().hex,
                order_id=order.order_id,
                round_start_ts=order.round_start_ts,
                token_id=order.token_id,
                side=order.side,
                price=price,
                size=size,
                ts_ms=int(time.time() * 1000),
                confirmed=1,
                trader_side=role,
                fee=fee,
                mode="paper",
            )
            self._db.update_order_status(
                order.order_id, "filled", order.size_matched
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("paper.fill_row_failed", market=self.market, error=str(exc))
