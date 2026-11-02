"""Read-only 5m market feed for 15m confirmation (2026-11-02).

The bot still trades only the BTC 15m series. This feed discovers the
current BTC 5m Up/Down market, subscribes to its tokens on the shared
market WebSocket, samples their midpoints every ~2 s and exposes a simple
"does the 5m timeframe confirm the held side?" score. It never places
orders.

A missing 5m market or a Gamma/CLOB outage leaves `available=False`; the
signal layer then fails open.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from src.infra.logging import get_logger
from src.market.market_ws import MarketWS, PriceCache
from src.market.pm_client import PolymarketClient

log = get_logger(__name__)


class Signal5mFeed:
    def __init__(
        self,
        *,
        pm: PolymarketClient,
        price_cache: PriceCache,
        market_ws: MarketWS | None = None,
        asset: str = "BTC",
        window_sec: int = 300,
        window_s: float = 300.0,
    ) -> None:
        self._pm = pm
        self._market_ws = market_ws
        self._pc = price_cache
        self._asset = asset.upper()
        self._window_sec = int(window_sec)
        self._window_s = float(window_s)
        self._up_token = ""
        self._down_token = ""
        self._round_ts = 0
        self._samples: deque[tuple[float, float, float]] = deque()
        self._last_refresh = 0.0
        self._last_error = ""
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="signal-5m-feed")
        log.info("signal_5m.started", asset=self._asset)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        log.info("signal_5m.stopped")

    async def _run(self) -> None:
        while self._running:
            try:
                now = time.time()
                if (
                    not self._up_token
                    or now - self._last_refresh >= 10.0
                ):
                    await self._refresh(now)
                await self._sample(now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                log.debug("signal_5m.tick_failed", error=str(exc))
            await asyncio.sleep(2.0)

    async def _refresh(self, now: float) -> None:
        self._last_refresh = now
        markets = await self._pm.get_asset_updown_markets(
            self._asset,
            slug_interval="5m",
            window_sec=self._window_sec,
            limit=3,
        )
        selected = None
        for market in markets:
            try:
                ts = int(str(market.get("_event_slug", "")).split("-")[-1])
            except (TypeError, ValueError):
                continue
            if ts <= now < ts + self._window_sec:
                selected = (ts, market)
                break
        if selected is None:
            return
        round_ts, market = selected
        tokens = market.get("tokens") or []
        if len(tokens) < 2:
            return
        up = str(tokens[0].get("token_id") or "")
        down = str(tokens[1].get("token_id") or "")
        if not up or not down:
            return
        changed = (
            round_ts != self._round_ts
            or up != self._up_token
            or down != self._down_token
        )
        if round_ts != self._round_ts:
            # Drop the previous 5m tokens from the shared price cache so it
            # does not accumulate one pair per 5-minute window forever.
            for old in (self._up_token, self._down_token):
                if old:
                    self._pc.clear_token(old)
            self._round_ts = round_ts
            self._samples.clear()
        self._up_token = up
        self._down_token = down
        if changed:
            log.info(
                "signal_5m.market",
                round_start_ts=round_ts,
                up_token=up[:16],
                down_token=down[:16],
            )

    async def _sample(self, now: float) -> None:
        if not self._up_token or not self._down_token:
            return
        up = down = 0.0
        try:
            # Sequential REST calls (not gather): a cancellation during
            # shutdown must not leave orphaned gather futures behind.
            up = await asyncio.wait_for(
                self._pm.get_midpoint(self._up_token), timeout=3.0
            )
            down = await asyncio.wait_for(
                self._pm.get_midpoint(self._down_token), timeout=3.0
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._last_error = str(exc)
            up = self._pc.get_midpoint(self._up_token)
            down = self._pc.get_midpoint(self._down_token)
        if up <= 0 or down <= 0:
            return
        self._pc.midpoint[self._up_token] = float(up)
        self._pc.midpoint[self._down_token] = float(down)
        self._samples.append((now, float(up), float(down)))
        cutoff = now - max(120.0, self._window_s)
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def snapshot(self, side: str) -> dict[str, Any]:
        side_u = str(side or "").upper()
        if side_u not in ("UP", "DOWN"):
            return {"available": False}
        if len(self._samples) < 2:
            return {"available": False}
        now = self._samples[-1][0]
        latest = self._samples[-1]
        target = now - min(60.0, max(10.0, self._window_s))
        past = next(
            (row for row in self._samples if row[0] >= target),
            self._samples[0],
        )
        up_mid, down_mid = latest[1], latest[2]
        past_up, past_down = past[1], past[2]
        up_change = (up_mid - past_up) / past_up if past_up > 0 else 0.0
        down_change = (
            (down_mid - past_down) / past_down if past_down > 0 else 0.0
        )
        if side_u == "UP":
            side_mid, side_change = up_mid, up_change
            other_change = down_change
        else:
            side_mid, side_change = down_mid, down_change
            other_change = up_change

        def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
            return max(lo, min(hi, value))

        confirm = _clamp(
            (side_mid - 0.5) * 2.0
            + _clamp(side_change * 20.0, -0.5, 0.5)
            - _clamp(max(0.0, other_change) * 10.0, 0.0, 0.5)
        )
        return {
            "available": True,
            "up_mid": round(up_mid, 4),
            "down_mid": round(down_mid, 4),
            "up_change_60": round(up_change, 4),
            "down_change_60": round(down_change, 4),
            "confirm": round(confirm, 4),
            "round_start_ts": self._round_ts,
        }
