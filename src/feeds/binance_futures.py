"""Macro / derivatives signal feed (2026-11-02).

Read-only public data used by the signal gates:

  * Binance USDT-M funding rate and open interest;
  * open-interest change over the last 5 minutes;
  * Deribit BTC DVOL (implied-volatility index) and its 1-hour change.

Everything is cached and refreshed in the background. A source outage
must never block trading: the signal layer treats a missing value as
"unknown" and fails open. This module is deliberately independent of the
execution path.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import httpx

from src.infra.logging import get_logger

log = get_logger(__name__)

BINANCE_FAPI = "https://fapi.binance.com"
DERIBIT_API = "https://www.deribit.com/api/v2"


@dataclass(frozen=True)
class MacroSnapshot:
    funding_rate: float | None = None
    open_interest: float | None = None
    oi_change_pct: float | None = None
    dvol: float | None = None
    dvol_change_pct: float | None = None
    updated_at: float = 0.0
    errors: tuple[str, ...] = ()

    @property
    def available(self) -> bool:
        return any(
            value is not None
            for value in (
                self.funding_rate,
                self.open_interest,
                self.dvol,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "funding_rate": self.funding_rate,
            "open_interest": self.open_interest,
            "oi_change_pct": self.oi_change_pct,
            "dvol": self.dvol,
            "dvol_change_pct": self.dvol_change_pct,
            "updated_at": self.updated_at or None,
            "errors": list(self.errors),
        }


class MacroSignals:
    """Background poller for funding / OI / DVOL."""

    def __init__(
        self,
        *,
        symbol: str = "BTCUSDT",
        poll_s: float = 60.0,
    ) -> None:
        self._symbol = symbol.upper()
        self._poll_s = max(15.0, float(poll_s))
        self._client: httpx.AsyncClient | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._snapshot = MacroSnapshot()
        self._oi_hist: deque[tuple[float, float]] = deque()
        self._dvol_hist: deque[tuple[float, float]] = deque()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(8.0, connect=5.0),
                headers={"User-Agent": "polygtd-signals/1.0"},
            )
        return self._client

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="macro-signals")
        log.info("macro_signals.started", symbol=self._symbol)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        log.info("macro_signals.stopped")

    def snapshot(self) -> MacroSnapshot:
        return self._snapshot

    async def _run(self) -> None:
        while self._running:
            try:
                await self.poll()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("macro_signals.poll_failed", error=str(exc))
            await asyncio.sleep(self._poll_s)

    async def poll(self) -> None:
        errors: list[str] = []
        funding = await self._fetch_funding(errors)
        oi = await self._fetch_oi(errors)
        dvol = await self._fetch_dvol(errors)
        now = time.time()

        oi_change_pct = None
        if oi is not None and oi > 0:
            self._oi_hist.append((now, oi))
            cutoff = now - 3600.0
            while self._oi_hist and self._oi_hist[0][0] < cutoff:
                self._oi_hist.popleft()
            old = next(
                (v for ts, v in self._oi_hist if ts <= now - 300.0),
                None,
            )
            if old is not None and old > 0:
                oi_change_pct = (oi - old) / old * 100.0

        dvol_change_pct = None
        if dvol is not None and dvol > 0:
            self._dvol_hist.append((now, dvol))
            cutoff = now - 7200.0
            while self._dvol_hist and self._dvol_hist[0][0] < cutoff:
                self._dvol_hist.popleft()
            old = next(
                (v for ts, v in self._dvol_hist if ts <= now - 3600.0),
                None,
            )
            if old is not None and old > 0:
                dvol_change_pct = (dvol - old) / old * 100.0

        prev = self._snapshot
        self._snapshot = MacroSnapshot(
            funding_rate=funding if funding is not None else prev.funding_rate,
            open_interest=oi if oi is not None else prev.open_interest,
            oi_change_pct=(
                oi_change_pct
                if oi_change_pct is not None
                else prev.oi_change_pct
            ),
            dvol=dvol if dvol is not None else prev.dvol,
            dvol_change_pct=(
                dvol_change_pct
                if dvol_change_pct is not None
                else prev.dvol_change_pct
            ),
            updated_at=now,
            errors=tuple(errors),
        )
        if errors:
            log.debug("macro_signals.partial", errors=errors)

    async def _fetch_funding(self, errors: list[str]) -> float | None:
        try:
            client = await self._get_client()
            resp = await client.get(
                f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
                params={"symbol": self._symbol},
            )
            resp.raise_for_status()
            data = resp.json()
            raw = data.get("lastFundingRate")
            return float(raw) if raw is not None else None
        except Exception as exc:  # noqa: BLE001
            errors.append(f"funding:{exc}")
            return None

    async def _fetch_oi(self, errors: list[str]) -> float | None:
        try:
            client = await self._get_client()
            resp = await client.get(
                f"{BINANCE_FAPI}/fapi/v1/openInterest",
                params={"symbol": self._symbol},
            )
            resp.raise_for_status()
            data = resp.json()
            raw = data.get("openInterest")
            return float(raw) if raw is not None else None
        except Exception as exc:  # noqa: BLE001
            errors.append(f"oi:{exc}")
            return None

    async def _fetch_dvol(self, errors: list[str]) -> float | None:
        client = await self._get_client()
        now_ms = int(time.time() * 1000)
        try:
            resp = await client.get(
                f"{DERIBIT_API}/public/get_volatility_index_data",
                params={
                    "currency": "BTC",
                    "start_timestamp": now_ms - 2 * 3600 * 1000,
                    "end_timestamp": now_ms,
                    "resolution": "60",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            rows = (data.get("result") or {}).get("data") or []
            if rows:
                last = rows[-1]
                # [timestamp, open, high, low, close]
                return float(last[4])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"dvol:{exc}")
        # Fallback: the DVOL index price endpoint.
        try:
            resp = await client.get(
                f"{DERIBIT_API}/public/get_index_price",
                params={"index_name": "btcdvol_usdc"},
            )
            resp.raise_for_status()
            data = resp.json()
            price = (data.get("result") or {}).get("index_price")
            return float(price) if price is not None else None
        except Exception as exc:  # noqa: BLE001
            errors.append(f"dvol_index:{exc}")
            return None
