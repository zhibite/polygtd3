"""Signal-driven PAPER trader (2026-11-02).

This is the non-martingale strategy requested after the 6-month study.
It is deliberately standalone:

  * it never creates a CLOB client and never posts an order;
  * it consumes the public market feeds + the existing SignalEngine;
  * it simulates fills locally from the live midpoint / best bid / ask;
  * it writes its simulated orders + fills to the same SQLite tables as
    the martingale bot, so the existing /trades dashboard works.

Entry (all configurable on StrategySettings):

  * only the favourite side, price in [SIGNAL_TRADE_PRICE_MIN,
    SIGNAL_TRADE_PRICE_MAX] (default 0.75-0.90);
  * the 5m confirmation score must be >= SIGNAL_TRADE_M5_MIN_CONFIRM
    (default 0.20).  This is the primary positive driver: the 6-month
    calibration study showed the digital `edge` is anti-predictive, so it
    is deliberately not required (SIGNAL_TRADE_MIN_EDGE=-1 disables it);
  * the SignalEngine's other reasons (spread / imbalance / CVD / vol
    regime / trend / funding / OI / DVOL / event blackout) still filter;
    `edge`, `strike_z` and `5m_against` are ignored by default.
  * maker entry at the best bid (fallback: mid - half spread).

Exit:

  * TP: +SIGNAL_TRADE_TP_PCT (default 10%) at the target, maker by default;
  * SL: -SIGNAL_TRADE_SL_PCT (default 15%), taker at the best bid;
  * TIME: force-close SIGNAL_TRADE_TIME_EXIT_S (default 60s) before the
    window ends, taker at the best bid;
  * optional flip exit when the held side's 5m confirm goes negative.

One entry per 15m window by default.  A forward-research JSONL snapshot is
written every SIGNAL_TRADE_SNAPSHOT_S seconds (default 2s).
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import strategy as _strategy
from src.infra.logging import get_logger

log = get_logger(__name__)

_DATA_DIR = Path(
    os.getenv("POLYMARKET_DATA_DIR")
    or (Path(__file__).resolve().parents[2] / "data")
)


def taker_fee(shares: float, price: float, rate: float) -> float:
    """Official Polymarket taker fee: C × rate × p × (1 - p)."""
    return max(0.0, shares) * max(0.0, rate) * max(0.0, price) * max(
        0.0, 1.0 - price
    )


def _fs(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _bs(value: Any, default: bool = False) -> bool:
    return bool(value) if isinstance(value, bool) else bool(default)


@dataclass
class SignalPosition:
    side: str
    token_id: str
    entry_ts: float
    entry_price: float
    shares: float
    entry_fee: float
    tp_price: float
    sl_price: float
    time_exit_ts: float
    order_id: str
    entry_role: str = "MAKER"
    status: str = "open"
    exit_ts: float = 0.0
    exit_price: float = 0.0
    exit_reason: str = ""
    exit_fee: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class PendingEntry:
    side: str
    token_id: str
    limit_price: float
    created_ts: float
    expires_ts: float
    attempt: int
    snapshot: dict = field(default_factory=dict)


class SignalTrader:
    """One-position, one-entry-per-window signal paper trader."""

    def __init__(
        self,
        *,
        state: Any,
        price_cache: Any,
        binance_cache: Any,
        signals: Any,
        db: Any = None,
        killswitch: Any = None,
        cfg_getter: Any = None,
        snapshot_path: str | Path | None = None,
    ) -> None:
        self._state = state
        self._pc = price_cache
        self._bc = binance_cache
        self._signals = signals
        self._db = db
        self._kill = killswitch
        self._cfg_getter = cfg_getter or _strategy

        self.round_start_ts: int = 0
        self.market: Any = None
        self.up_token: str = ""
        self.down_token: str = ""
        self.window_start_ts: float = 0.0
        self.window_end_ts: float = 0.0
        self.tick_size: float = 0.01
        self.baseline: float = 0.0

        self.position: SignalPosition | None = None
        self.pending: PendingEntry | None = None
        self.entries_this_round: int = 0
        self.attempts_this_round: int = 0
        self.round_pnl: float = 0.0
        self.round_trades: int = 0
        self.entry_window_open: bool = False
        # Set by signal_main when WindowManager marks the window unsafe
        # (late boot / cross-window BTC gap). No entries while non-empty.
        self.skip_reason: str = ""

        self._btc_samples: list[tuple[float, float]] = []
        self._last_snapshot: dict = {}
        self._snapshots: dict[str, dict] = {}
        self._last_snapshot_ts: float = 0.0
        self._skip_log_ts: dict[str, float] = {}
        self._snapshot_lock = threading.Lock()

        raw = snapshot_path or str(
            getattr(self._cfg(), "SIGNAL_TRADE_SNAPSHOT_PATH", "") or ""
        )
        self.snapshot_path = (
            Path(raw) if raw else (_DATA_DIR / "signal_snapshots.jsonl")
        )

        log.info(
            "signal_trader.init",
            snapshot_path=str(self.snapshot_path),
            paper_only=True,
        )

    # ------------------------------------------------------------------
    # Config helper
    # ------------------------------------------------------------------

    def _cfg(self) -> Any:
        try:
            return self._cfg_getter()
        except Exception:  # noqa: BLE001
            return _strategy()

    @staticmethod
    def _c(cfg: Any, name: str, default: Any) -> Any:
        value = getattr(cfg, name, default)
        return default if value is None else value

    # ------------------------------------------------------------------
    # Round lifecycle
    # ------------------------------------------------------------------

    def set_baseline(self, price: float) -> None:
        if price and price > 0:
            self.baseline = float(price)

    def set_skip_reason(self, reason: str) -> None:
        """Mark the current window unsafe (late boot / cross-window gap)."""
        self.skip_reason = str(reason or "")
        if self.skip_reason:
            self.cancel_pending(self.skip_reason)
            log.warning(
                "signal_trader.round_skip",
                round_start_ts=self.round_start_ts,
                reason=self.skip_reason,
                note="no new entries in this window",
            )

    def on_new_round(self, round_start_ts: int, market: Any = None) -> None:
        if self.position is not None:
            self._force_close("round_end", self._cfg())
        if self.pending is not None:
            self.cancel_pending("round_end")
        self.round_start_ts = int(round_start_ts)
        self.entries_this_round = 0
        self.attempts_this_round = 0
        self.round_pnl = 0.0
        self.round_trades = 0
        self.skip_reason = ""
        # Keep the rolling BTC samples across the round boundary: sigma /
        # the flash-move guard describe the market, not the token.
        self._last_snapshot = {}
        self._snapshots = {}
        self._last_snapshot_ts = 0.0
        self._bind_market(market)
        log.info(
            "signal_trader.new_round",
            round_start_ts=self.round_start_ts,
            slug=getattr(market, "slug", "") if market else "",
            paper_only=True,
        )

    def attach_market(self, market: Any) -> None:
        self._bind_market(market)
        log.info(
            "signal_trader.market_attached",
            round_start_ts=self.round_start_ts,
            slug=getattr(market, "slug", "") if market else "",
            up=(self.up_token or "")[:16],
            down=(self.down_token or "")[:16],
        )

    def _bind_market(self, market: Any) -> None:
        if market is None:
            return
        self.market = market
        self.up_token = (
            getattr(market, "up_token", "")
            or getattr(market, "token_up_id", "")
            or ""
        )
        self.down_token = (
            getattr(market, "down_token", "")
            or getattr(market, "token_down_id", "")
            or ""
        )
        self.tick_size = _fs(getattr(market, "tick_size", 0.01), 0.01) or 0.01
        ws = getattr(market, "window_start", None)
        we = getattr(market, "window_end", None)
        try:
            self.window_start_ts = ws.timestamp() if ws else 0.0
        except Exception:  # noqa: BLE001
            self.window_start_ts = 0.0
        try:
            self.window_end_ts = we.timestamp() if we else 0.0
        except Exception:  # noqa: BLE001
            self.window_end_ts = 0.0
        if not self.round_start_ts:
            self.round_start_ts = int(
                getattr(market, "round_start_ts", 0) or self.window_start_ts
            )
        spot = _fs(getattr(market, "btc_spot_at_window_open", 0.0), 0.0)
        if spot > 0:
            self.baseline = spot

    def on_round_end(self) -> None:
        cfg = self._cfg()
        if self.position is not None:
            self._force_close("round_end", cfg)
        if self.pending is not None:
            self.cancel_pending("round_end")
        if self.round_start_ts:
            self._finish_round()
        log.info(
            "signal_trader.round_end",
            round_start_ts=self.round_start_ts,
            round_pnl=round(self.round_pnl, 4),
            trades=self.round_trades,
        )
        self.market = None
        self.up_token = ""
        self.down_token = ""
        self.window_start_ts = 0.0
        self.window_end_ts = 0.0

    # ------------------------------------------------------------------
    # Main tick
    # ------------------------------------------------------------------

    def tick(self) -> None:
        cfg = self._cfg()
        now = time.time()
        self._update_btc_samples(cfg, now)
        if self.round_start_ts <= 0 or not self.market:
            return
        kill_active = bool(getattr(self._kill, "active", False))

        if self.pending is not None:
            self._check_pending(cfg, now)

        if self.position is not None:
            self._manage_position(cfg, now)
        elif not kill_active and _bs(
            self._c(cfg, "SIGNAL_TRADE_ENABLED", True), True
        ):
            self._try_entry(cfg, now)

        self._maybe_snapshot(cfg, now)

    # ------------------------------------------------------------------
    # BTC guards
    # ------------------------------------------------------------------

    def _update_btc_samples(self, cfg: Any, now: float) -> None:
        spot = _fs(getattr(self._bc, "last_price", 0.0), 0.0)
        ts_ms = _fs(getattr(self._bc, "ts_ms", 0), 0.0)
        if spot <= 0 or ts_ms <= 0:
            return
        max_age = _fs(self._c(cfg, "BTC_STALE_MAX_AGE_S", 5.0), 5.0)
        if max_age > 0 and now - ts_ms / 1000.0 > max_age:
            return
        self._btc_samples.append((now, spot))
        window = max(
            60.0, _fs(self._c(cfg, "BTC_VOL_WINDOW_S", 60.0), 60.0)
        )
        cutoff = now - window
        while self._btc_samples and self._btc_samples[0][0] < cutoff:
            self._btc_samples.pop(0)
        if len(self._btc_samples) > 2000:
            del self._btc_samples[: len(self._btc_samples) - 2000]

    def _feed_stale(self, cfg: Any, now: float) -> bool:
        max_age = _fs(self._c(cfg, "BTC_STALE_MAX_AGE_S", 5.0), 5.0)
        if max_age <= 0:
            return False
        ts_ms = _fs(getattr(self._bc, "ts_ms", 0), 0)
        if ts_ms <= 0:
            return True
        return now - ts_ms / 1000.0 > max_age

    def _vol_blocked(self, cfg: Any) -> bool:
        threshold = _fs(self._c(cfg, "BTC_VOL_MAX_MOVE_PCT", 0.3), 0.3)
        if threshold <= 0 or len(self._btc_samples) < 2:
            return False
        low = min(p for _, p in self._btc_samples)
        high = max(p for _, p in self._btc_samples)
        return low > 0 and (high - low) / low * 100.0 > threshold

    def _sigma_1m(self, cfg: Any) -> float:
        floor = max(
            0.0,
            _fs(self._c(cfg, "T4_STRIKE_SIGMA_PCT_PER_MIN", 0.05), 0.05),
        ) / 100.0
        sigma = floor
        if len(self._btc_samples) >= 4:
            low = min(p for _, p in self._btc_samples)
            high = max(p for _, p in self._btc_samples)
            if low > 0:
                sigma = max(sigma, (high - low) / low / 2.0)
        return sigma

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def _try_entry(self, cfg: Any, now: float) -> None:
        if self.skip_reason:
            self.entry_window_open = False
            return
        if self.pending is not None:
            return
        if not self.up_token or not self.down_token:
            return
        remaining = self.window_end_ts - now
        if remaining <= 0:
            return
        start_after = _fs(
            self._c(cfg, "SIGNAL_TRADE_ENTRY_START_S", 60.0), 60.0
        )
        end_before = _fs(
            self._c(cfg, "SIGNAL_TRADE_ENTRY_END_BEFORE_END_S", 180.0), 180.0
        )
        if now < self.window_start_ts + start_after:
            self.entry_window_open = False
            return
        if remaining < end_before:
            self.entry_window_open = False
            return
        self.entry_window_open = True

        max_entries = int(
            self._c(cfg, "SIGNAL_TRADE_MAX_ENTRIES", 1) or 1
        )
        allow_reentry = _bs(
            self._c(cfg, "SIGNAL_TRADE_ALLOW_REENTRY", False), False
        )
        if self.entries_this_round >= max_entries and not allow_reentry:
            return
        max_attempts = int(
            self._c(cfg, "SIGNAL_TRADE_MAX_ATTEMPTS", 3) or 3
        )
        if self.attempts_this_round >= max_attempts:
            return
        if self._feed_stale(cfg, now):
            self._skip("btc_stale", now)
            return
        if self._vol_blocked(cfg):
            self._skip("btc_vol_spike", now)
            return

        spot = _fs(getattr(self._bc, "last_price", 0.0), 0.0)
        strike = self.baseline or _fs(
            getattr(self.market, "btc_spot_at_window_open", 0.0), 0.0
        )
        sigma = self._sigma_1m(cfg)
        if spot <= 0 or strike <= 0 or sigma <= 0:
            return

        price_min = _fs(
            self._c(cfg, "SIGNAL_TRADE_PRICE_MIN", 0.75), 0.75
        )
        price_max = _fs(
            self._c(cfg, "SIGNAL_TRADE_PRICE_MAX", 0.90), 0.90
        )
        m5_min = _fs(
            self._c(cfg, "SIGNAL_TRADE_M5_MIN_CONFIRM", 0.20), 0.20
        )
        require_m5 = _bs(
            self._c(cfg, "SIGNAL_TRADE_REQUIRE_M5", True), True
        )
        use_gates = _bs(
            self._c(cfg, "SIGNAL_TRADE_USE_SIGNAL_GATES", True), True
        )
        ignore = {
            x.strip()
            for x in str(
                self._c(
                    cfg,
                    "SIGNAL_TRADE_IGNORE_REASONS",
                    "edge,strike_z,5m_against",
                )
            ).split(",")
            if x.strip()
        }
        max_spread = _fs(
            self._c(cfg, "SIGNAL_TRADE_MAX_SPREAD", 0.03), 0.03
        )
        min_imbalance = _fs(
            self._c(cfg, "SIGNAL_TRADE_MIN_IMBALANCE", -0.60), -0.60
        )
        max_sell_flow = _fs(
            self._c(cfg, "SIGNAL_TRADE_MAX_SELL_FLOW", 0.60), 0.60
        )
        max_trend = _fs(
            self._c(cfg, "SIGNAL_TRADE_MAX_TREND_AGAINST", 0.70), 0.70
        )
        max_sigma = _fs(
            self._c(cfg, "SIGNAL_TRADE_MAX_SIGMA_PCT", 0.25), 0.25
        )
        min_edge = _fs(
            self._c(cfg, "SIGNAL_TRADE_MIN_EDGE", -1.0), -1.0
        )

        candidates: list[dict] = []
        for side, token in (("UP", self.up_token), ("DOWN", self.down_token)):
            mid = _fs(self._pc.get_midpoint(token), 0.0)
            if mid < price_min or mid > price_max:
                continue
            try:
                decision = self._signals.evaluate(
                    kind="T1",
                    side=side,
                    token=token,
                    mid=mid,
                    spot=spot,
                    strike=strike,
                    remaining_s=remaining,
                    sigma_1m=sigma,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "signal_trader.signal_eval_failed",
                    side=side, error=str(exc),
                )
                continue
            snap = dict(getattr(decision, "snapshot", {}) or {})
            tf = snap.get("tf5m") or {}
            m5 = tf.get("confirm")
            if require_m5 and m5 is None:
                self._skip("m5_unavailable", now)
                continue
            if m5 is not None and float(m5) < m5_min:
                self._skip("m5_below_threshold", now)
                continue
            if use_gates:
                reasons = [
                    r for r in (getattr(decision, "reasons", ()) or ())
                    if r not in ignore
                ]
                if reasons:
                    self._skip("gate:" + ",".join(reasons), now)
                    continue
                spread = snap.get("spread")
                if spread is not None and float(spread) > max_spread:
                    self._skip("spread_wide", now)
                    continue
                imbalance = snap.get("book_imbalance")
                if imbalance is not None and float(imbalance) < min_imbalance:
                    self._skip("book_ask_heavy", now)
                    continue
                cvd = snap.get("cvd_ratio")
                if cvd is not None and float(cvd) < -max_sell_flow:
                    self._skip("sell_flow", now)
                    continue
                trend = snap.get("trend_against")
                if trend is not None and float(trend) > max_trend:
                    self._skip("trend_against", now)
                    continue
                sigma_pct = snap.get("sigma_1m_pct")
                if sigma_pct is not None and float(sigma_pct) > max_sigma:
                    self._skip("vol_regime", now)
                    continue
            edge = snap.get("edge")
            if min_edge > -0.99 and (
                edge is None or float(edge) < min_edge
            ):
                self._skip("edge_below_min", now)
                continue
            candidates.append({
                "side": side,
                "token": token,
                "mid": mid,
                "m5": float(m5) if m5 is not None else None,
                "decision": decision,
                "snapshot": snap,
            })

        if not candidates:
            return

        # The 5m confirmation is the primary positive driver.  If it is
        # missing for both sides, fail open only when REQUIRE_M5 is off.
        candidates.sort(
            key=lambda c: (
                c["m5"] if c["m5"] is not None else -99.0,
                -abs(c["mid"] - (price_min + price_max) / 2.0),
            ),
            reverse=True,
        )
        chosen = candidates[0]
        self.attempts_this_round += 1
        self._last_snapshot = chosen["snapshot"]
        self._snapshots[chosen["side"]] = chosen["snapshot"]

        token = chosen["token"]
        mid = chosen["mid"]
        best_bid = _fs(self._pc.best_bid.get(token, 0.0), 0.0)
        best_ask = _fs(self._pc.best_ask.get(token, 0.0), 0.0)
        maker = _bs(
            self._c(cfg, "SIGNAL_TRADE_ENTRY_MAKER", True), True
        )
        offset = _fs(
            self._c(cfg, "SIGNAL_TRADE_MAKER_OFFSET", 0.005), 0.005
        )
        if maker:
            limit = best_bid if best_bid > 0 else mid - offset
            limit = self._round_limit(limit)
            if limit <= 0:
                return
            if best_ask > 0 and best_ask <= limit:
                self._open_position(
                    cfg, chosen["side"], token, best_ask, "TAKER",
                    chosen["snapshot"], now,
                )
                return
            timeout = _fs(
                self._c(cfg, "SIGNAL_TRADE_ENTRY_TIMEOUT_S", 5.0), 5.0
            )
            self.pending = PendingEntry(
                side=chosen["side"],
                token_id=token,
                limit_price=limit,
                created_ts=now,
                expires_ts=now + max(0.5, timeout),
                attempt=self.attempts_this_round,
                snapshot=chosen["snapshot"],
            )
            log.info(
                "signal_trader.entry_pending",
                side=chosen["side"],
                token=token[:16],
                limit=limit,
                mid=mid,
                m5=chosen["m5"],
                spread=chosen["snapshot"].get("spread"),
                edge=chosen["snapshot"].get("edge"),
            )
        else:
            fill_px = best_ask if 0 < best_ask <= 0.99 else mid
            self._open_position(
                cfg, chosen["side"], token, fill_px, "TAKER",
                chosen["snapshot"], now,
            )

    def _round_limit(self, price: float) -> float:
        tick = self.tick_size or 0.01
        try:
            # BUY limit: floor to the tick (never pay above the strategy).
            out = math.floor(price / tick + 1e-9) * tick
        except Exception:  # noqa: BLE001
            out = price
        return max(0.01, min(0.99, round(out, 6)))

    def _check_pending(self, cfg: Any, now: float) -> None:
        pending = self.pending
        if pending is None:
            return
        if now >= pending.expires_ts:
            log.info(
                "signal_trader.entry_timeout",
                side=pending.side,
                limit=pending.limit_price,
                attempt=pending.attempt,
            )
            self.pending = None
            return
        token = pending.token_id
        mid = _fs(self._pc.get_midpoint(token), 0.0)
        best_ask = _fs(self._pc.best_ask.get(token, 0.0), 0.0)
        last = _fs(self._pc.last_trade_price.get(token, 0.0), 0.0)
        if best_ask > 0 and best_ask <= pending.limit_price:
            # The book crossed our limit: marketable, taker at the ask.
            self._open_position(
                cfg, pending.side, token, best_ask, "TAKER",
                pending.snapshot, now,
            )
            return
        if last > 0 and last <= pending.limit_price:
            self._open_position(
                cfg, pending.side, token, pending.limit_price, "MAKER",
                pending.snapshot, now,
            )
            return
        if mid > 0 and mid <= pending.limit_price:
            # Conservative paper model: the mid crossing through the
            # resting bid is treated as a maker fill at the limit.
            self._open_position(
                cfg, pending.side, token, pending.limit_price, "MAKER",
                pending.snapshot, now,
            )

    def _open_position(
        self,
        cfg: Any,
        side: str,
        token: str,
        price: float,
        role: str,
        snapshot: dict,
        now: float,
    ) -> None:
        price = max(0.01, min(0.99, float(price)))
        stake = _fs(
            self._c(cfg, "SIGNAL_TRADE_STAKE_USD", 2.0), 2.0
        )
        if stake <= 0:
            return
        shares = stake / price
        fee = (
            taker_fee(shares, price, _fs(self._c(cfg, "TAKER_FEE_RATE", 0.07), 0.07))
            if role == "TAKER"
            else 0.0
        )
        tp_pct = _fs(self._c(cfg, "SIGNAL_TRADE_TP_PCT", 0.10), 0.10)
        sl_pct = _fs(self._c(cfg, "SIGNAL_TRADE_SL_PCT", 0.15), 0.15)
        time_exit_before = _fs(
            self._c(cfg, "SIGNAL_TRADE_TIME_EXIT_S", 60.0), 60.0
        )
        tp = min(0.99, price * (1.0 + tp_pct))
        sl = max(0.01, price * (1.0 - sl_pct))
        time_exit_ts = self.window_end_ts - time_exit_before
        order_id = (
            f"sig-{self.round_start_ts}-{side}-{uuid.uuid4().hex[:8]}"
        )
        self.position = SignalPosition(
            side=side,
            token_id=token,
            entry_ts=now,
            entry_price=price,
            shares=shares,
            entry_fee=fee,
            tp_price=tp,
            sl_price=sl,
            time_exit_ts=time_exit_ts,
            order_id=order_id,
            entry_role=role,
        )
        self.pending = None
        self.entries_this_round += 1
        self.round_trades += 1
        self._last_snapshot = snapshot
        self._persist_open(
            order_id=order_id,
            side=side,
            token=token,
            price=price,
            shares=shares,
            role=role,
            fee=fee,
            now=now,
        )
        log.info(
            "signal_trader.position_opened",
            side=side,
            token=token[:16],
            price=price,
            shares=round(shares, 4),
            stake=round(stake, 2),
            role=role,
            tp=round(tp, 4),
            sl=round(sl, 4),
            time_exit_ts=int(time_exit_ts),
            m5=snapshot.get("tf5m", {}).get("confirm")
            if isinstance(snapshot.get("tf5m"), dict) else None,
            edge=snapshot.get("edge"),
        )

    # ------------------------------------------------------------------
    # Exit
    # ------------------------------------------------------------------

    def _manage_position(self, cfg: Any, now: float) -> None:
        pos = self.position
        if pos is None:
            return
        token = pos.token_id
        mid = _fs(self._pc.get_midpoint(token), 0.0)
        best_bid = _fs(self._pc.best_bid.get(token, 0.0), 0.0)
        best_ask = _fs(self._pc.best_ask.get(token, 0.0), 0.0)
        if mid <= 0 and best_bid <= 0:
            return

        # TP first (it can only be at/above the entry).  A resting maker
        # sell fills when the BID reaches the target, not merely because
        # the mid crossed it.
        #
        # Do NOT use `last_trade_price` here: it can be an old print from
        # before the position opened (production 2026-11-02 14:51: a
        # 0.76 DOWN entry was closed as "SL" at 0.765 because a stale
        # last trade was <= the 0.646 stop line).  The book/mid is the
        # current observation; the last-trade cache has no timestamp.
        tp_hit = (
            (best_bid > 0 and best_bid >= pos.tp_price)
            or (best_bid <= 0 and mid > 0 and mid >= pos.tp_price)
        )
        if tp_hit:
            maker = _bs(
                self._c(cfg, "SIGNAL_TRADE_EXIT_MAKER", True), True
            )
            if maker:
                self._close(pos, pos.tp_price, "MAKER", "TP", now)
            else:
                px = best_bid if best_bid >= pos.tp_price else mid
                self._close(pos, px, "TAKER", "TP", now)
            return

        sl_hit = (
            (best_bid > 0 and best_bid <= pos.sl_price)
            or (best_bid <= 0 and mid > 0 and mid <= pos.sl_price)
        )
        if sl_hit:
            px = best_bid if 0 < best_bid <= pos.sl_price else mid
            self._close(pos, px or pos.sl_price, "TAKER", "SL", now)
            return

        if now >= pos.time_exit_ts:
            px = best_bid if best_bid > 0 else mid
            self._close(pos, px, "TAKER", "TIME", now)
            return

        if _bs(
            self._c(cfg, "SIGNAL_TRADE_EXIT_ON_FLIP", False), False
        ):
            confirm = None
            snap = self._snapshots.get(pos.side) or {}
            tf = snap.get("tf5m") if isinstance(snap, dict) else None
            if isinstance(tf, dict):
                confirm = tf.get("confirm")
            if confirm is not None and float(confirm) < _fs(
                self._c(cfg, "SIGNAL_TRADE_FLIP_CONFIRM", 0.0), 0.0
            ):
                px = best_bid if best_bid > 0 else mid
                self._close(pos, px, "TAKER", "FLIP", now)

    def _close(
        self,
        pos: SignalPosition,
        exit_price: float,
        role: str,
        reason: str,
        now: float,
    ) -> None:
        exit_price = max(0.0, min(1.0, float(exit_price)))
        fee = (
            taker_fee(
                pos.shares, exit_price,
                _fs(self._c(self._cfg(), "TAKER_FEE_RATE", 0.07), 0.07),
            )
            if role == "TAKER"
            else 0.0
        )
        pnl = (
            (exit_price - pos.entry_price) * pos.shares
            - pos.entry_fee
            - fee
        )
        pos.status = "closed"
        pos.exit_ts = now
        pos.exit_price = exit_price
        pos.exit_reason = reason
        pos.exit_fee = fee
        pos.realized_pnl = pnl
        self.position = None
        self.round_pnl += pnl
        self._persist_close(pos, exit_price, role, reason, fee, pnl, now)
        self._finish_round()
        log.info(
            "signal_trader.position_closed",
            side=pos.side,
            token=pos.token_id[:16],
            entry=pos.entry_price,
            exit=exit_price,
            shares=round(pos.shares, 4),
            role=role,
            reason=reason,
            pnl=round(pnl, 4),
            pnl_pct=round(
                pnl / (pos.entry_price * pos.shares)
                if pos.entry_price * pos.shares > 0 else 0.0,
                4,
            ),
            round_pnl=round(self.round_pnl, 4),
        )

    def _force_close(self, reason: str, cfg: Any) -> None:
        pos = self.position
        if pos is None:
            return
        best_bid = _fs(self._pc.best_bid.get(pos.token_id, 0.0), 0.0)
        mid = _fs(self._pc.get_midpoint(pos.token_id), 0.0)
        px = best_bid if best_bid > 0 else (mid if mid > 0 else pos.entry_price)
        self._close(pos, px, "TAKER", reason.upper(), time.time())

    def cancel_pending(self, reason: str = "manual") -> None:
        if self.pending is None:
            return
        pending = self.pending
        self.pending = None
        log.info(
            "signal_trader.entry_cancelled",
            side=pending.side,
            limit=pending.limit_price,
            reason=reason,
        )

    def on_shutdown(self) -> None:
        cfg = self._cfg()
        if self.pending is not None:
            self.cancel_pending("shutdown")
        if self.position is not None:
            self._force_close("shutdown", cfg)
        if self.round_start_ts:
            self._finish_round()

    # ------------------------------------------------------------------
    # Snapshots (forward research)
    # ------------------------------------------------------------------

    def _maybe_snapshot(self, cfg: Any, now: float) -> None:
        interval = _fs(
            self._c(cfg, "SIGNAL_TRADE_SNAPSHOT_S", 2.0), 2.0
        )
        if interval <= 0 or now - self._last_snapshot_ts < interval:
            return
        self._last_snapshot_ts = now
        spot = _fs(getattr(self._bc, "last_price", 0.0), 0.0)
        strike = self.baseline or _fs(
            getattr(self.market, "btc_spot_at_window_open", 0.0), 0.0
        )
        remaining = self.window_end_ts - now
        sigma = self._sigma_1m(cfg)
        row: dict[str, Any] = {
            "ts_ms": int(now * 1000),
            "round_start_ts": self.round_start_ts,
            "slug": getattr(self.market, "slug", "") or "",
            "remaining_s": round(remaining, 1),
            "spot": round(spot, 2) if spot > 0 else None,
            "strike": round(strike, 2) if strike > 0 else None,
            "sigma_1m_pct": round(sigma * 100.0, 4) if sigma > 0 else None,
            "entry_window_open": bool(self.entry_window_open),
            "position": self._position_payload(),
            "pending": self._pending_payload(),
        }
        for side, token in (("UP", self.up_token), ("DOWN", self.down_token)):
            mid = _fs(self._pc.get_midpoint(token), 0.0)
            if mid <= 0:
                continue
            try:
                decision = self._signals.evaluate(
                    kind="T1",
                    side=side,
                    token=token,
                    mid=mid,
                    spot=spot,
                    strike=strike,
                    remaining_s=remaining,
                    sigma_1m=sigma,
                )
                snap = dict(getattr(decision, "snapshot", {}) or {})
            except Exception:  # noqa: BLE001
                snap = {}
            self._snapshots[side] = snap
            row[side.lower()] = {
                "mid": round(mid, 4),
                "bid": round(_fs(self._pc.best_bid.get(token, 0.0), 0.0), 4),
                "ask": round(_fs(self._pc.best_ask.get(token, 0.0), 0.0), 4),
                "snapshot": snap,
            }
        # Dashboard uses a single SignalEngine snapshot. While in a
        # position, show the held side; otherwise the side with the
        # highest 5m confirmation.
        if self.position is not None:
            self._last_snapshot = self._snapshots.get(self.position.side, {})
        else:
            price_min = _fs(
                self._c(cfg, "SIGNAL_TRADE_PRICE_MIN", 0.75), 0.75
            )
            price_max = _fs(
                self._c(cfg, "SIGNAL_TRADE_PRICE_MAX", 0.90), 0.90
            )
            best = None
            best_m5 = -99.0
            best_band = None
            best_band_m5 = -99.0
            for side, snap in self._snapshots.items():
                tf = snap.get("tf5m") if isinstance(snap, dict) else None
                m5 = tf.get("confirm") if isinstance(tf, dict) else None
                if m5 is None:
                    continue
                m5f = float(m5)
                if m5f > best_m5:
                    best_m5 = m5f
                    best = snap
                mid = _fs(snap.get("token_mid"), 0.0)
                if (
                    price_min <= mid <= price_max
                    and m5f > best_band_m5
                ):
                    best_band_m5 = m5f
                    best_band = snap
            # Prefer a side the trader could actually enter, so the
            # dashboard does not show an out-of-band favourite.
            self._last_snapshot = best_band or best or {}
        self._write_snapshot(row)

    def _write_snapshot(self, row: dict) -> None:
        try:
            line = json.dumps(row, ensure_ascii=False, default=str)
            with self._snapshot_lock:
                self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                with self.snapshot_path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
        except Exception:  # noqa: BLE001
            # Snapshot failures must never affect the paper trader.
            pass

    def latest_snapshot(self) -> dict:
        return dict(self._last_snapshot or {})

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist_open(
        self,
        *,
        order_id: str,
        side: str,
        token: str,
        price: float,
        shares: float,
        role: str,
        fee: float,
        now: float,
    ) -> None:
        db = self._db
        if db is None:
            return
        try:
            m = self.market
            db.upsert_round(
                round_start_ts=self.round_start_ts,
                slug=getattr(m, "slug", None) if m else None,
                condition_id=getattr(m, "condition_id", None) if m else None,
                up_token=self.up_token or None,
                down_token=self.down_token or None,
                tick_size=self.tick_size,
                binance_baseline=self.baseline or None,
                mode="paper",
            )
            db.insert_order(
                client_order_id=order_id,
                server_order_id=order_id,
                round_start_ts=self.round_start_ts,
                side="BUY",
                token_id=token,
                tier_name="SIG",
                price=price,
                size=shares,
                lifetime_s=0,
                expires_at=0,
                placed_at=int(now),
                mode="paper",
            )
            db.update_order_status(order_id, "filled", shares)
            db.insert_fill(
                fill_id=f"{order_id}:BUY:{uuid.uuid4().hex[:8]}",
                order_id=order_id,
                round_start_ts=self.round_start_ts,
                token_id=token,
                side="BUY",
                price=price,
                size=shares,
                ts_ms=int(now * 1000),
                confirmed=1,
                trader_side=role,
                fee=fee,
                mode="paper",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("signal_trader.db_open_failed", error=str(exc))

    def _persist_close(
        self,
        pos: SignalPosition,
        exit_price: float,
        role: str,
        reason: str,
        fee: float,
        pnl: float,
        now: float,
    ) -> None:
        db = self._db
        if db is None:
            return
        sell_id = f"{pos.order_id}-{reason.lower()}"
        try:
            db.insert_order(
                client_order_id=sell_id,
                server_order_id=sell_id,
                round_start_ts=self.round_start_ts,
                side="SELL",
                token_id=pos.token_id,
                tier_name=reason[:8],
                price=exit_price,
                size=pos.shares,
                lifetime_s=0,
                expires_at=0,
                placed_at=int(now),
                mode="paper",
            )
            db.update_order_status(sell_id, "filled", pos.shares)
            db.insert_fill(
                fill_id=f"{sell_id}:SELL:{uuid.uuid4().hex[:8]}",
                order_id=sell_id,
                round_start_ts=self.round_start_ts,
                token_id=pos.token_id,
                side="SELL",
                price=exit_price,
                size=pos.shares,
                ts_ms=int(now * 1000),
                confirmed=1,
                trader_side=role,
                fee=fee,
                mode="paper",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("signal_trader.db_close_failed", error=str(exc))

    def _finish_round(self) -> None:
        db = self._db
        if db is None or not self.round_start_ts:
            return
        try:
            db.finish_round(self.round_start_ts)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _position_payload(self) -> dict | None:
        pos = self.position
        if pos is None:
            return None
        mid = _fs(self._pc.get_midpoint(pos.token_id), 0.0)
        cost = pos.entry_price * pos.shares
        upnl = (mid - pos.entry_price) * pos.shares if mid > 0 else 0.0
        return {
            "side": pos.side,
            "token": pos.token_id,
            "qty": pos.shares,
            "entry_price": pos.entry_price,
            "avg_cost": pos.entry_price,
            "cost": cost,
            "entry_fee": pos.entry_fee,
            "mid": mid,
            "upnl_usd": upnl,
            "upnl_pct": (upnl / cost) if cost > 0 else 0.0,
            "tp_price": pos.tp_price,
            "sl_price": pos.sl_price,
            "time_exit_ts": pos.time_exit_ts,
            "order_id": pos.order_id,
            "entry_role": pos.entry_role,
            "status": pos.status,
        }

    def _pending_payload(self) -> dict | None:
        p = self.pending
        if p is None:
            return None
        return {
            "side": p.side,
            "token": p.token_id,
            "limit_price": p.limit_price,
            "created_ts": p.created_ts,
            "expires_ts": p.expires_ts,
            "attempt": p.attempt,
        }

    def status(self) -> dict:
        now = time.time()
        pos = self._position_payload()
        return {
            "round_start_ts": self.round_start_ts,
            "slug": getattr(self.market, "slug", "") if self.market else "",
            "up_token": self.up_token,
            "down_token": self.down_token,
            "window_start_ts": self.window_start_ts,
            "window_end_ts": self.window_end_ts,
            "remaining_s": max(0.0, self.window_end_ts - now),
            "entry_window_open": bool(self.entry_window_open),
            "skip_reason": self.skip_reason or None,
            "entries_this_round": self.entries_this_round,
            "attempts_this_round": self.attempts_this_round,
            "round_pnl": round(self.round_pnl, 4),
            "round_trades": self.round_trades,
            "position": pos,
            "pending": self._pending_payload(),
            "last_signal": self.latest_snapshot(),
        }

    def _skip(self, reason: str, now: float) -> None:
        last = self._skip_log_ts.get(reason, 0.0)
        if now - last < 30.0:
            return
        self._skip_log_ts[reason] = now
        log.info("signal_trader.entry_skip", reason=reason)
