"""External-signal gates for the BTC 15m martingale (2026-11-02).

The strategy used to average down on a fixed price drop alone. This module
builds a per-decision snapshot and vetoes entries/averaging that look like
trend-following or adverse selection:

  * digital-option fair value: P(up) = Phi(ln(S/K)/(sigma*sqrt(tau))),
    edge = model probability of the held side - token mid;
  * adverse distance to the strike in sigma units (z-score);
  * trend / volatility regime from the recent Binance trades (ring buffer);
  * CLOB spread, top-of-book imbalance and 60 s signed trade flow (CVD);
  * 5m BTC Up/Down market confirmation (read-only feed);
  * Binance futures funding / open-interest change + Deribit DVOL;
  * a manual event blackout calendar.

The layer is advisory by default in the sense that missing data fails
OPEN: an upstream outage must never wedge the ladder. The gate mode and
all thresholds are runtime-configurable; `SIGNAL_GATES_LOG_ONLY=true`
computes and journals the metrics without blocking.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from src.config import strategy as _strategy
from src.feeds.binance_spot import RingBuffer
from src.infra.logging import get_logger
from src.market.market_ws import PriceCache

log = get_logger(__name__)


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _ema_last(values: list[float], span: int) -> float | None:
    if not values:
        return None
    span = max(1, min(span, len(values)))
    alpha = 2.0 / (span + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = alpha * value + (1.0 - alpha) * ema
    return ema


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for prev, cur in zip(closes[-period - 1:-1], closes[-period:]):
        delta = cur - prev
        if delta >= 0:
            gains += delta
        else:
            losses -= delta
    if losses <= 1e-12:
        return 100.0 if gains > 0 else 50.0
    rs = gains / losses
    return 100.0 - (100.0 / (1.0 + rs))


@dataclass(frozen=True)
class RegimeState:
    trend_score: float = 0.0
    trend_raw: float = 0.0
    sigma_1m: float = 0.0
    rsi: float | None = None
    atr_pct: float | None = None
    bars: int = 0
    ts: float = 0.0


@dataclass
class SignalDecision:
    allowed: bool
    would_block: bool
    mode: str
    reasons: tuple[str, ...]
    size_multiplier: float
    snapshot: dict[str, Any]


class SignalEngine:
    """Compute the signal snapshot and apply the veto gates."""

    def __init__(
        self,
        *,
        ring: RingBuffer | None,
        price_cache: PriceCache,
        macro: Any | None = None,
        tf5m: Any | None = None,
        events: Any | None = None,
    ) -> None:
        self._ring = ring
        self._pc = price_cache
        self._macro = macro
        self._tf5m = tf5m
        self._events = events
        self._regime = RegimeState()
        self._regime_ts = 0.0
        self._last_snapshot: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Config helpers (runtime-config edits apply immediately)
    # ------------------------------------------------------------------

    @staticmethod
    def _cfg_bool(name: str, default: bool) -> bool:
        value = getattr(_strategy(), name, default)
        return bool(value) if isinstance(value, bool) else bool(default)

    @staticmethod
    def _cfg_float(name: str, default: float) -> float:
        value = getattr(_strategy(), name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return float(default)
        return float(value)

    # ------------------------------------------------------------------
    # Regime
    # ------------------------------------------------------------------

    def _build_bars(self, now: float) -> list[list[float]]:
        """Aggregate ring samples into 1-minute [o,h,l,c] bars, oldest first."""
        if self._ring is None:
            return []
        lookback = max(
            120.0, self._cfg_float("SIGNAL_REGIME_LOOKBACK_S", 1200.0)
        )
        samples = self._ring.samples_with_ts(now - lookback, now)
        buckets: dict[int, list[float]] = {}
        for ts, price in samples:
            if price <= 0:
                continue
            minute = int(ts // 60) * 60
            bar = buckets.get(minute)
            if bar is None:
                buckets[minute] = [price, price, price, price]
            else:
                if price > bar[1]:
                    bar[1] = price
                if price < bar[2]:
                    bar[2] = price
                bar[3] = price
        keys = sorted(buckets)[-20:]
        return [buckets[k] for k in keys]

    def _refresh_regime(self, sigma_hint: float) -> RegimeState:
        now = time.time()
        refresh_s = max(
            1.0, self._cfg_float("SIGNAL_REGIME_REFRESH_S", 5.0)
        )
        if now - self._regime_ts < refresh_s and self._regime.ts > 0:
            return self._regime

        bars = self._build_bars(now)
        sigma = max(0.0, float(sigma_hint or 0.0))
        floor = max(
            0.0,
            self._cfg_float("T4_STRIKE_SIGMA_PCT_PER_MIN", 0.05) / 100.0,
        )
        trend_score = 0.0
        trend_raw = 0.0
        rsi = None
        atr_pct = None
        if len(bars) >= 2:
            closes = [bar[3] for bar in bars]
            returns = [
                math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes))
                if closes[i - 1] > 0 and closes[i] > 0
            ]
            if len(returns) >= 2:
                mean = sum(returns) / len(returns)
                var = sum((r - mean) ** 2 for r in returns) / (
                    len(returns) - 1
                )
                sigma = max(sigma, math.sqrt(max(var, 0.0)))
            if len(closes) >= 3:
                fast = _ema_last(closes, 5)
                slow = _ema_last(closes, min(15, len(closes)))
                if fast is not None and slow and slow > 0:
                    trend_raw = (fast - slow) / slow
                    normalizer = max(sigma, floor) * math.sqrt(5.0)
                    if normalizer > 0:
                        trend_score = _clamp(trend_raw / normalizer)
            rsi = _rsi(closes)
            last_close = closes[-1]
            if last_close > 0 and len(bars) >= 2:
                trs: list[float] = []
                for i in range(1, len(bars)):
                    high, low = bars[i][1], bars[i][2]
                    prev_close = bars[i - 1][3]
                    trs.append(
                        max(
                            high - low,
                            abs(high - prev_close),
                            abs(low - prev_close),
                        )
                    )
                if trs:
                    atr_pct = (sum(trs) / len(trs)) / last_close * 100.0
        sigma = max(sigma, floor)
        self._regime = RegimeState(
            trend_score=round(trend_score, 4),
            trend_raw=round(trend_raw, 6),
            sigma_1m=sigma,
            rsi=round(rsi, 2) if rsi is not None else None,
            atr_pct=round(atr_pct, 4) if atr_pct is not None else None,
            bars=len(bars),
            ts=now,
        )
        self._regime_ts = now
        return self._regime

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        *,
        kind: str,
        side: str,
        token: str,
        mid: float,
        spot: float,
        strike: float,
        remaining_s: float | None,
        sigma_1m: float,
    ) -> SignalDecision:
        cfg = _strategy()
        enabled = self._cfg_bool("SIGNAL_GATES_ENABLED", True)
        log_only = self._cfg_bool("SIGNAL_GATES_LOG_ONLY", False)
        mode = "off" if not enabled else ("log" if log_only else "enforce")
        side_u = str(side or "").upper()
        is_t1 = str(kind or "").upper() == "T1"

        regime = self._refresh_regime(sigma_1m)
        sigma = max(0.0, float(sigma_1m or 0.0), regime.sigma_1m)

        z = None
        fair_up = None
        fair_held = None
        edge = None
        if (
            spot > 0
            and strike > 0
            and sigma > 0
            and remaining_s is not None
            and remaining_s > 0
        ):
            minutes = max(float(remaining_s) / 60.0, 1.0 / 60.0)
            z = math.log(spot / strike) / (sigma * math.sqrt(minutes))
            fair_up = _norm_cdf(z)
            fair_held = fair_up if side_u == "UP" else 1.0 - fair_up
            if mid > 0:
                edge = fair_held - mid
        adverse_z = None
        if z is not None:
            adverse_z = z if side_u == "DOWN" else -z
        side_sign = 1.0 if side_u == "UP" else -1.0
        trend_against = max(0.0, -(side_sign * regime.trend_score))

        spread = self._pc.spread(token) if token else None
        imbalance = self._pc.book_imbalance(token) if token else None
        flow_window = self._cfg_float("SIGNAL_FLOW_WINDOW_S", 60.0)
        cvd, volume = self._pc.flow(token, flow_window) if token else (0.0, 0.0)
        cvd_ratio = (cvd / volume) if volume > 0 else None

        tf = (
            self._tf5m.snapshot(side_u)
            if self._tf5m is not None
            else {"available": False}
        )
        macro_snap = (
            self._macro.snapshot().to_dict()
            if self._macro is not None
            else {}
        )
        event = (
            self._events.snapshot()
            if self._events is not None
            else {"event_blackout": False, "event_name": None}
        )

        reasons: list[str] = []
        max_z = self._cfg_float(
            "SIGNAL_T1_MAX_ADVERSE_Z" if is_t1
            else "SIGNAL_MAX_ADVERSE_Z",
            2.5 if is_t1 else 2.0,
        )
        if adverse_z is not None and adverse_z > max_z:
            reasons.append("strike_z")
        min_edge = self._cfg_float(
            "SIGNAL_T1_MIN_EDGE" if is_t1 else "SIGNAL_MIN_EDGE",
            -0.05 if is_t1 else 0.02,
        )
        if edge is not None and edge < min_edge:
            reasons.append("edge")
        max_trend = self._cfg_float(
            "SIGNAL_T1_MAX_TREND_AGAINST" if is_t1
            else "SIGNAL_MAX_TREND_AGAINST",
            0.9 if is_t1 else 0.7,
        )
        if trend_against > max_trend:
            reasons.append("trend_against")
        max_sigma_pct = self._cfg_float(
            "SIGNAL_MAX_SIGMA_1M_PCT", 0.25
        )
        if sigma * 100.0 > max_sigma_pct:
            reasons.append("vol_regime")

        if self._cfg_bool("SIGNAL_MICRO_GATE_ENABLED", True):
            max_spread = self._cfg_float("SIGNAL_MAX_SPREAD", 0.03)
            if spread is not None and spread > max_spread:
                reasons.append("spread_wide")
            min_imbalance = self._cfg_float(
                "SIGNAL_MIN_BOOK_IMBALANCE", -0.60
            )
            if imbalance is not None and imbalance < min_imbalance:
                reasons.append("book_ask_heavy")
            max_sell = self._cfg_float("SIGNAL_MAX_SELL_FLOW", 0.60)
            if cvd_ratio is not None and cvd_ratio < -max_sell:
                reasons.append("sell_flow")

        if self._cfg_bool("SIGNAL_5M_GATE_ENABLED", True) and (
            not is_t1 or self._cfg_bool("SIGNAL_5M_T1_ENABLED", False)
        ):
            confirm = tf.get("confirm")
            min_confirm = self._cfg_float("SIGNAL_5M_MIN_CONFIRM", -0.50)
            if (
                tf.get("available")
                and confirm is not None
                and confirm < min_confirm
            ):
                reasons.append("5m_against")

        if self._cfg_bool("SIGNAL_MACRO_GATE_ENABLED", True):
            funding = macro_snap.get("funding_rate")
            max_funding = self._cfg_float(
                "SIGNAL_MAX_FUNDING_RATE", 0.001
            )
            if funding is not None and abs(float(funding)) > max_funding:
                reasons.append("funding_extreme")
            oi_change = macro_snap.get("oi_change_pct")
            max_oi = self._cfg_float("SIGNAL_MAX_OI_CHANGE_PCT", 3.0)
            if oi_change is not None and abs(float(oi_change)) > max_oi:
                reasons.append("oi_spike")
            dvol = macro_snap.get("dvol")
            dvol_change = macro_snap.get("dvol_change_pct")
            if dvol is not None and float(dvol) > self._cfg_float(
                "SIGNAL_MAX_DVOL", 75.0
            ):
                reasons.append("dvol_extreme")
            elif dvol_change is not None and float(dvol_change) > self._cfg_float(
                "SIGNAL_MAX_DVOL_CHANGE_PCT", 20.0
            ):
                reasons.append("dvol_extreme")

        if self._cfg_bool("SIGNAL_EVENT_GATE_ENABLED", True):
            if event.get("event_blackout"):
                reasons.append(
                    f"event_blackout:{event.get('event_name') or 'event'}"
                )

        would_block = bool(reasons)
        allowed = not would_block if mode == "enforce" else True

        size_mult = 1.0
        if (
            allowed
            and not is_t1
            and self._cfg_bool("SIGNAL_SIZE_SCALING_ENABLED", False)
        ):
            min_mult = max(
                0.0, min(1.0, self._cfg_float("SIGNAL_SIZE_MIN_MULT", 0.25))
            )
            target = max(
                min_edge + 1e-9,
                self._cfg_float("SIGNAL_SIZE_TARGET_EDGE", 0.10),
            )
            edge_now = edge if edge is not None else min_edge
            edge_scale = _clamp(
                (edge_now - min_edge) / (target - min_edge), 0.0, 1.0
            )
            size_mult = min_mult + (1.0 - min_mult) * edge_scale
            size_mult *= 1.0 - 0.35 * min(1.0, trend_against)
            vol_ref = max(max_sigma_pct, 1e-9)
            size_mult *= 1.0 - 0.35 * min(1.0, sigma * 100.0 / vol_ref)
            size_mult = _clamp(size_mult, min_mult, 1.0)

        snapshot = {
            "ts": round(time.time(), 3),
            "kind": kind,
            "side": side_u,
            "mode": mode,
            "allowed": allowed,
            "would_block": would_block,
            "reasons": list(reasons),
            "spot": round(spot, 2) if spot > 0 else None,
            "strike": round(strike, 2) if strike > 0 else None,
            "remaining_s": (
                round(float(remaining_s), 1)
                if remaining_s is not None else None
            ),
            "sigma_1m_pct": round(sigma * 100.0, 4) if sigma > 0 else None,
            "z": round(z, 3) if z is not None else None,
            "adverse_z": (
                round(adverse_z, 3) if adverse_z is not None else None
            ),
            "fair_up": round(fair_up, 4) if fair_up is not None else None,
            "fair_held": (
                round(fair_held, 4) if fair_held is not None else None
            ),
            "token_mid": round(float(mid), 4) if mid > 0 else None,
            "edge": round(edge, 4) if edge is not None else None,
            "trend_score": round(regime.trend_score, 4),
            "trend_against": round(trend_against, 4),
            "rsi": regime.rsi,
            "atr_pct": regime.atr_pct,
            "regime_bars": regime.bars,
            "spread": round(spread, 4) if spread is not None else None,
            "book_imbalance": (
                round(imbalance, 4) if imbalance is not None else None
            ),
            "cvd_ratio": round(cvd_ratio, 4) if cvd_ratio is not None else None,
            "tf5m": tf,
            "macro": macro_snap,
            "event": event,
            "size_multiplier": round(size_mult, 4),
        }
        self._last_snapshot = snapshot
        return SignalDecision(
            allowed=allowed,
            would_block=would_block,
            mode=mode,
            reasons=tuple(reasons),
            size_multiplier=size_mult,
            snapshot=snapshot,
        )

    def set_tf5m(self, feed: Any | None) -> None:
        """Late-bind the 5m confirmation feed (created with the WS feed)."""
        self._tf5m = feed

    def last_snapshot(self) -> dict[str, Any]:
        return dict(self._last_snapshot)
