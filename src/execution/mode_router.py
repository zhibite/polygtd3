"""Live/paper execution router (2026-11-02).

The strategy engine emits one Action stream. This router decides where
each action goes:

  * `PAPER_TRADING=true`  -> the local PaperBroker (no CLOB orders)
  * `PAPER_TRADING=false` -> the existing live ActionRouter/OrderManager

The mode is read when T1 enters and is then **locked on the round**
(`RoundState.mode`). Toggling the config in the dashboard mid-round
therefore affects the next T1, never the take-profit/stop-loss management
of a position that was already opened live or paper. That is what keeps a
real position from becoming half-managed when the operator flips the
switch.

Both executors are kept live at the same time (the CLOB client/UserWS are
still connected), so a mode switch does not need a process restart.
"""
from __future__ import annotations

from typing import Any

from src.config import strategy as _strategy
from src.infra.logging import get_logger
from src.strategy.engine import Action, ActionCancelTier, ActionOpenT1

log = get_logger(__name__)


class ModeRouter:
    """Action handler that fans out to the live or paper executor."""

    def __init__(self, state: Any, live: Any, paper: Any) -> None:
        self._state = state
        self._live = live
        self._paper = paper
        # The live ActionRouter sets state.router to itself in __init__;
        # the killswitch / round cleanup must see this wrapper instead.
        state.router = self
        self.engine = None

    @staticmethod
    def _configured_mode() -> str:
        try:
            paper = bool(getattr(_strategy(), "PAPER_TRADING", True))
        except Exception:  # noqa: BLE001
            paper = True
        return "paper" if paper else "live"

    def _round_mode(self) -> str:
        rnd = getattr(getattr(self, "engine", None), "_round", None)
        return str(getattr(rnd, "mode", "") or "")

    def _round_ts(self) -> int | None:
        rnd = getattr(getattr(self, "engine", None), "_round", None)
        return getattr(rnd, "round_start_ts", None)

    async def __call__(self, action: Action) -> None:
        eng = getattr(self, "engine", None)
        if eng is not None and getattr(eng, "_round", None) is not None:
            rnd = eng._round
            if isinstance(action, ActionOpenT1):
                mode = self._configured_mode()
                rnd.mode = mode
                log.warning(
                    "execution.mode_selected",
                    mode=mode,
                    round_start_ts=rnd.round_start_ts,
                    side=action.side,
                    token=(action.token or "")[:16],
                )
            else:
                mode = str(getattr(rnd, "mode", "") or "") or self._configured_mode()
        else:
            mode = self._configured_mode()

        # A late cancel after the round object was torn down can belong to
        # either executor. Dispatch to both; cancel-by-unknown-id is a
        # harmless no-op in each.
        if (
            isinstance(action, ActionCancelTier)
            and not self._round_mode()
        ):
            await self._live(action)
            await self._paper(action)
            return

        if mode == "paper":
            await self._paper(action)
        else:
            await self._live(action)

    async def stop_all_watchers(self) -> None:
        """Killswitch / shutdown hook for both executors."""
        try:
            await self._live.stop_all_watchers()
        except Exception as exc:  # noqa: BLE001
            log.warning("router.live_stop_watchers_failed", error=str(exc))
        try:
            await self._paper.cancel_all_open(reason="stop_all_watchers")
        except Exception as exc:  # noqa: BLE001
            log.warning("router.paper_stop_watchers_failed", error=str(exc))

    @property
    def live(self) -> Any:
        return self._live

    @property
    def paper(self) -> Any:
        return self._paper
