"""Minimal router facade for the signal paper bot.

The martingale `ActionRouter` owns TierWatchers and the live OrderManager.
The signal bot has neither.  `KillSwitch.engage()` and `main.py`'s shutdown
path call `router.stop_all_watchers()` / `router.stop_all()`, so this stub
forwards those calls to the paper trader (cancel any resting maker entry).

It deliberately does NOT force-close an open paper position: the trader's
TP/SL/time-exit loop keeps managing it, which is the same "emergency stop
does not market-sell inventory" policy as the production bot.
"""
from __future__ import annotations

from typing import Any


class SignalRouterStub:
    def __init__(self, trader: Any) -> None:
        self._trader = trader

    async def stop_all_watchers(self) -> None:
        try:
            self._trader.cancel_pending("killswitch")
        except Exception:  # noqa: BLE001
            pass

    async def stop_all(self) -> None:
        await self.stop_all_watchers()

    def watcher_for_server_order(self, server_order_id: str) -> None:
        return None

    @property
    def live(self) -> None:
        return None

    @property
    def paper(self) -> Any:
        return self._trader
