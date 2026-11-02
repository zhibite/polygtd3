"""Signal-driven PAPER bot entry point (2026-11-02).

Run with `BOT_MODE=signal` (or `python -m src.signal_main`).  This is a
standalone alternative to the martingale bot:

  * NO CLOB client / OrderManager / Heartbeat / UserWS is created;
  * no order can reach Polymarket — every fill is simulated locally from
    the public market-data feeds;
  * the strategy is `src.strategy.signal_trader.SignalTrader`;
  * the existing FastAPI dashboard is served in-process (status/trades),
    and `/trades` shows the simulated orders and fills.

The bot still reuses WindowManager, MarketWS, BinanceFeed, the SignalEngine
and the 5m confirmation feed, so the live market context is identical to
the production bot.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
from typing import Any

from src.config import binance, general, log_config, strategy
from src.execution.signal_router_stub import SignalRouterStub
from src.feeds.binance_futures import MacroSignals
from src.feeds.binance_spot import (
    BinanceCache,
    BinanceFeed,
    BinanceSpotService,
    RingBuffer,
)
from src.infra.logging import configure_logging, get_logger
from src.market.market_service import MarketService
from src.market.market_ws import MarketWS, PriceCache
from src.market.pm_client import PolymarketClient
from src.market.signal_market import Signal5mFeed
from src.market.window_manager import WindowManager
from src.persistence.db import DB, init_db
from src.strategy import event_calendar
from src.strategy.killswitch import KillSwitch
from src.strategy.signal_trader import SignalTrader
from src.strategy.signals import SignalEngine
from src.web.app import app as web_app
from src.web.app import set_state as set_web_state

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------


class SignalState:
    """Minimal state object consumed by the signal trader and the web app."""

    def __init__(self) -> None:
        self.strategy_mode = "signal"
        self.round_start_ts: int = 0
        self.killswitch = KillSwitch.load()
        self.event_calendar = None
        self.macro_signals: MacroSignals | None = None
        self.signal_5m: Signal5mFeed | None = None
        self.signals: SignalEngine | None = None
        self.binance_cache = BinanceCache()
        self.price_cache = PriceCache()
        self.ring = RingBuffer(retention_s=binance().BUFFER_RETENTION_S)
        self.db_conn = init_db()
        self.db = DB(self.db_conn)
        self.current_market = None
        self.signal_trader: SignalTrader | None = None
        # Deliberately absent in signal mode:
        self.clob = None
        self.order_manager = None
        self.engine = None
        self.paper_broker = None
        self.user_ws = None
        self.heartbeat = None
        self.gtd_expiry = None
        self.binance_feed: BinanceFeed | None = None
        self.market_ws: MarketWS | None = None
        self.wm: WindowManager | None = None
        self.pm_client: PolymarketClient | None = None
        self.market_svc: MarketService | None = None
        self.binance_spot: BinanceSpotService | None = None
        self.router: Any = None
        self.btc_baseline: float = 0.0
        self._shutdown = asyncio.Event()
        self._bg_tasks: set[asyncio.Task] = set()


STATE = SignalState()


# ---------------------------------------------------------------------------
# Window callbacks
# ---------------------------------------------------------------------------


def _on_window_baseline(
    asset: str, price: float, source: str, round_start_ts: int = 0
) -> None:
    if asset != "BTC" or not price or price <= 0:
        return
    STATE.btc_baseline = float(price)
    if STATE.signal_trader is not None:
        STATE.signal_trader.set_baseline(float(price))
    log.info(
        "signal.baseline_applied",
        baseline=price,
        source=source,
        round_start_ts=round_start_ts,
    )


def _on_market_discovered(asset: str, mkt: Any) -> None:
    state = STATE
    state.current_market = mkt
    if state.market_ws is not None:
        try:
            state.market_ws.set_active_tokens(
                {mkt.token_up_id, mkt.token_down_id}
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("signal.market_ws_subscribe_failed", error=str(exc))
    if state.signal_trader is not None:
        try:
            state.signal_trader.attach_market(mkt)
        except Exception as exc:  # noqa: BLE001
            log.warning("signal.attach_market_failed", error=str(exc))
    ctx = state.wm.get("BTC") if state.wm else None
    if (
        ctx is not None
        and getattr(ctx, "skip_until_next_window", False)
        and state.signal_trader is not None
    ):
        state.signal_trader.set_skip_reason("window_precheck")
    spot = float(getattr(mkt, "btc_spot_at_window_open", 0.0) or 0.0)
    if spot > 0:
        state.btc_baseline = spot
        if state.signal_trader is not None:
            state.signal_trader.set_baseline(spot)
    log.info(
        "signal.market_discovered",
        asset=asset,
        slug=getattr(mkt, "slug", "") or "",
        condition_id=(getattr(mkt, "condition_id", "") or "")[:16],
        up=(getattr(mkt, "token_up_id", "") or "")[:16],
        down=(getattr(mkt, "token_down_id", "") or "")[:16],
    )


async def _on_new_round(round_start_ts: int) -> None:
    state = STATE
    state.round_start_ts = int(round_start_ts)
    mkt = None
    if state.wm is not None:
        ctx = state.wm.get("BTC")
        mkt = ctx.market if ctx else None
        if (
            mkt is not None
            and getattr(mkt, "round_start_ts", 0)
            and mkt.round_start_ts != round_start_ts
        ):
            log.warning(
                "signal.stale_market_rejected",
                round_start_ts=round_start_ts,
                market_round_ts=mkt.round_start_ts,
            )
            mkt = None
    state.current_market = mkt
    if state.signal_trader is not None:
        try:
            state.signal_trader.on_new_round(round_start_ts, mkt)
        except Exception:
            log.exception("signal.on_new_round_failed")
    ctx = state.wm.get("BTC") if state.wm else None
    if ctx is not None and getattr(ctx, "skip_until_next_window", False):
        if state.signal_trader is not None:
            state.signal_trader.set_skip_reason("window_precheck")
        log.warning(
            "signal.round_skip",
            round_start_ts=round_start_ts,
            reason="window_precheck",
            note="WindowManager flagged this window unsafe "
                 "(late boot or cross-window BTC gap)",
        )
    if mkt is not None:
        spot = float(getattr(mkt, "btc_spot_at_window_open", 0.0) or 0.0)
        if spot > 0:
            state.btc_baseline = spot
            if state.signal_trader is not None:
                state.signal_trader.set_baseline(spot)
    log.info(
        "signal.new_round",
        round_start_ts=round_start_ts,
        slug=getattr(mkt, "slug", "") if mkt else "",
    )


# ---------------------------------------------------------------------------
# Loops
# ---------------------------------------------------------------------------


async def _midpoint_loop(state: SignalState) -> None:
    """Refresh REST midpoints + top-5 books for the current 15m tokens."""
    last_cid = ""
    while True:
        await asyncio.sleep(2.0)
        pm = state.pm_client
        if pm is None:
            continue
        mkt = state.current_market
        if mkt is None or not mkt.token_up_id or not mkt.token_down_id:
            continue
        cid = getattr(mkt, "condition_id", "") or ""
        if cid != last_cid and last_cid:
            log.info(
                "signal.midpoint_window_changed",
                prev=cid[:16],
                new=cid[:16],
            )
        last_cid = cid
        try:
            up_mid = await pm.get_midpoint(mkt.token_up_id)
            down_mid = await pm.get_midpoint(mkt.token_down_id)
            state.price_cache.midpoint[mkt.token_up_id] = up_mid
            state.price_cache.midpoint[mkt.token_down_id] = down_mid
            for token in (mkt.token_up_id, mkt.token_down_id):
                try:
                    book = await pm.get_order_book(token)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.debug("signal.book_failed", error=str(exc))
                    continue
                if isinstance(book, dict):
                    state.price_cache.set_book(
                        token, book.get("bids"), book.get("asks")
                    )
        except Exception as exc:  # noqa: BLE001
            log.warning("signal.midpoint_poll_failed", error=str(exc))


async def _trader_loop(state: SignalState) -> None:
    """Drive the paper trader on a 250 ms tick."""
    while True:
        trader = state.signal_trader
        if trader is not None:
            try:
                trader.tick()
            except Exception:
                log.exception("signal.trader_tick_failed")
        await asyncio.sleep(0.25)


async def _status_loop(state: SignalState) -> None:
    cfg = log_config()
    while True:
        await asyncio.sleep(max(1.0, float(cfg.HEARTBEAT_S or 5)))
        trader = state.signal_trader
        if trader is None:
            continue
        st = trader.status()
        pos = st.get("position")
        pending = st.get("pending")
        up = state.current_market.token_up_id if state.current_market else ""
        down = state.current_market.token_down_id if state.current_market else ""
        up_mid = state.price_cache.get_midpoint(up) if up else 0.0
        down_mid = state.price_cache.get_midpoint(down) if down else 0.0
        spot = float(getattr(state.binance_cache, "last_price", 0.0) or 0.0)
        base = state.btc_baseline
        drift = abs(spot - base) / base if spot > 0 and base > 0 else None
        sig = st.get("last_signal") or {}
        tf = sig.get("tf5m") if isinstance(sig.get("tf5m"), dict) else {}
        confirm = tf.get("confirm")
        remain_s = float(st.get("remaining_s") or 0.0)
        if remain_s <= 0 and state.wm is not None:
            try:
                remain_s = float(state.wm.seconds_until_next())
            except Exception:  # noqa: BLE001
                remain_s = 0.0
        pos_str = "--"
        if pos:
            pos_str = (
                f"{pos['side']} {pos['qty']:.2f}@{pos['entry_price']:.4f} "
                f"uPnL={pos['upnl_usd']:+.3f}"
            )
        pending_str = ""
        if pending:
            pending_str = (
                f" pending={pending['side']}@{pending['limit_price']:.4f}"
            )
        log.info(
            f"SIGNAL_STATUS  spot={spot:,.1f} base={base:,.1f} "
            f"drift={drift if drift is not None else float('nan'):.4f} "
            f"remain={remain_s:.0f}s "
            f"UP={up_mid:.4f} DOWN={down_mid:.4f} "
            f"entry={'OPEN' if st['entry_window_open'] else 'WAIT'} "
            f"m5={confirm if confirm is not None else '--'} "
            f"pos=[{pos_str}]{pending_str} "
            f"roundPnL={st['round_pnl']:+.3f} trades={st['round_trades']}"
        )


# ---------------------------------------------------------------------------
# Web dashboard
# ---------------------------------------------------------------------------


def _start_dashboard(state: SignalState) -> "asyncio.Task | None":
    cfg = strategy()
    if not cfg.WEB_ENABLED:
        log.info("signal.web_disabled")
        return None
    try:
        import uvicorn

        set_web_state(state)

        class _Server(uvicorn.Server):
            def capture_signals(self):  # type: ignore[override]
                return contextlib.nullcontext()

        server = _Server(
            uvicorn.Config(
                web_app,
                host=cfg.WEB_HOST,
                port=int(cfg.WEB_PORT),
                log_level=cfg.WEB_LOG_LEVEL,
                access_log=False,
                lifespan="off",
            )
        )
        task = asyncio.create_task(server.serve(), name="signal-web")
        task.add_done_callback(
            lambda t: None
            if t.cancelled() or t.exception() is None
            else log.error("signal.web_exited", error=str(t.exception()))
        )
        log.info(
            "signal.web_started",
            host=cfg.WEB_HOST,
            port=int(cfg.WEB_PORT),
            auth=bool(cfg.WEB_PASSWORD),
        )
        if not cfg.WEB_PASSWORD:
            log.warning(
                "signal.web_auth_disabled",
                note="set WEB_PASSWORD to protect the dashboard",
            )
        return task
    except Exception as exc:  # noqa: BLE001
        log.warning("signal.web_start_failed", error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run_signal_bot() -> None:
    configure_logging()
    state = STATE
    cfg = strategy()

    log.info(
        "signal.bot_starting",
        paper_only=True,
        bot_mode=str(getattr(cfg, "BOT_MODE", "signal")),
        note="no CLOB client is created; every fill is simulated locally",
    )
    if getattr(cfg, "PAPER_TRADING", True) is False:
        log.warning(
            "signal.paper_only_forced",
            note="PAPER_TRADING=false is ignored by the signal bot",
        )

    # Runtime config overlay (signal thresholds can be edited there once
    # they are added to the whitelist; unknown keys are ignored).
    try:
        from src.web.runtime_config import apply_persisted_overrides

        apply_persisted_overrides()
    except Exception as exc:  # noqa: BLE001
        log.warning("signal.runtime_config_failed", error=str(exc))

    # Market / feeds
    state.pm_client = PolymarketClient(
        gamma_api=str(getattr(cfg, "GAMMA_API", "https://gamma-api.polymarket.com")),
        polymarket_host=str(
            getattr(cfg, "POLYMARKET_HOST", "https://clob.polymarket.com")
        ),
    )
    state.market_svc = MarketService(
        pm=state.pm_client,
        threshold=0.5,
        slug_interval="15m",
        window_sec=900,
    )
    state.binance_spot = BinanceSpotService()
    state.wm = WindowManager(
        market_svc=state.market_svc,
        symbols=["BTC"],
        on_new_round=_on_new_round,
        on_market_discovered=_on_market_discovered,
        binance_spot=state.binance_spot,
        entry_cfg=None,
        on_window_baseline=_on_window_baseline,
    )

    # Signal layer
    state.event_calendar = event_calendar.from_config()
    state.macro_signals = MacroSignals(
        symbol="BTCUSDT",
        poll_s=float(getattr(cfg, "SIGNAL_MACRO_POLL_S", 60.0) or 60.0),
    )
    state.signals = SignalEngine(
        ring=state.ring,
        price_cache=state.price_cache,
        macro=state.macro_signals,
        events=state.event_calendar,
    )
    state.signal_5m = Signal5mFeed(
        pm=state.pm_client,
        price_cache=state.price_cache,
        market_ws=None,
        asset="BTC",
        window_sec=300,
        window_s=float(getattr(cfg, "SIGNAL_5M_WINDOW_S", 300.0) or 300.0),
    )
    state.signals.set_tf5m(state.signal_5m)

    # Paper trader
    state.signal_trader = SignalTrader(
        state=state,
        price_cache=state.price_cache,
        binance_cache=state.binance_cache,
        signals=state.signals,
        db=state.db,
        killswitch=state.killswitch,
    )
    state.router = SignalRouterStub(state.signal_trader)

    # Feeds
    state.binance_feed = BinanceFeed(
        cache=state.binance_cache,
        ring=state.ring,
        on_connect=None,
    )
    state.market_ws = MarketWS(
        cache=state.price_cache,
        on_connect=None,
        on_disconnect=None,
    )

    await state.binance_feed.start()
    await state.market_ws.start()
    await state.wm.start()
    try:
        await state.macro_signals.start()
        await state.signal_5m.start()
    except Exception as exc:  # noqa: BLE001
        log.warning("signal.feeds_start_failed", error=str(exc))

    loop = asyncio.get_running_loop()
    tasks: list[asyncio.Task] = [
        asyncio.create_task(_trader_loop(state), name="signal-trader"),
        asyncio.create_task(_midpoint_loop(state), name="signal-midpoint"),
        asyncio.create_task(_status_loop(state), name="signal-status"),
    ]
    web_task = _start_dashboard(state)

    def _shutdown_handler() -> None:
        log.info("signal.shutdown_signal_received")
        state._shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except (NotImplementedError, OSError):
            pass

    await state._shutdown.wait()

    log.info("signal.shutdown_initiated")
    for task in tasks:
        task.cancel()
    if web_task is not None:
        web_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await web_task
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    try:
        state.signal_trader.on_shutdown()
    except Exception:
        log.exception("signal.trader_shutdown_failed")

    if state.macro_signals is not None:
        await state.macro_signals.stop()
    if state.signal_5m is not None:
        await state.signal_5m.stop()
    if state.wm is not None:
        await state.wm.stop()
    if state.market_ws is not None:
        await state.market_ws.stop()
    if state.binance_feed is not None:
        await state.binance_feed.stop()
    if state.binance_spot is not None:
        await state.binance_spot.close()
    if state.pm_client is not None:
        await state.pm_client.close()
    state.db_conn.close()
    log.info("signal.shutdown_complete", round_pnl=round(
        state.signal_trader.round_pnl if state.signal_trader else 0.0, 4
    ))


def main() -> None:
    try:
        asyncio.run(run_signal_bot())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
