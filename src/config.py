"""Global configuration parsed from config.toml and .env."""
from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root = parent of the `src` package. config.toml / .env live there.
_ROOT = Path(__file__).parent.parent
_TOML_PATH = _ROOT / "config.toml"


@lru_cache(maxsize=1)
def _toml_data() -> dict:
    """Load config.toml once per process. Returns {} if file missing."""
    if not _TOML_PATH.exists():
        return {}
    with _TOML_PATH.open("rb") as f:
        raw = tomllib.load(f)
    return {k.lower(): v for k, v in raw.items()}


def _toml_section(key: str) -> dict:
    """Return TOML section with env-var override semantics (uppercase keys)."""
    section = _toml_data().get(key.lower(), {})
    return {k.upper(): v for k, v in section.items()}


# ---------------------------------------------------------------------------
# Toml-driven "general" settings
# ---------------------------------------------------------------------------


class GeneralConfig(BaseSettings):
    ROUND_PERIOD_S: int = 900
    WINDOW_PRE_OPEN_S: int = 300

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("GENERAL"), **kwargs})


class BinanceConfig(BaseSettings):
    WS_URL: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    RECONNECT_BASE_S: float = 1.0
    RECONNECT_MAX_S: float = 30.0
    BUFFER_RETENTION_S: int = 1200

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("BINANCE"), **kwargs})


class BaselineConfig(BaseSettings):
    WINDOW_S: int = 300
    MIN_SAMPLES: int = 30

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("BASELINE"), **kwargs})


class LogConfig(BaseSettings):
    LEVEL: str = "INFO"
    HEARTBEAT_S: int = 5

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("LOG"), **kwargs})


# ---------------------------------------------------------------------------
# Env-driven strategy settings
# ---------------------------------------------------------------------------


class StrategySettings(BaseSettings):
    """
    Values come from the process environment, then .env, then .env.docker.

    Priority (low → high):
        code defaults < .env.docker < .env < process environment

    Why .env.docker is read in-process: Coolify/Dockerfile deployments bake
    the non-secret config file into the image, and Coolify injects the
    secrets as process environment variables. Reading both files means the
    same image works for Coolify, docker-compose, and local runs.

    IMPORTANT: DRY_RUN has been removed. Trading is always live.
    """

    model_config = SettingsConfigDict(
        env_file=(str(_ROOT / ".env.docker"), str(_ROOT / ".env")),
        extra="ignore",
    )

    # ── CLOB Credentials ──────────────────────────────────────────────────
    # Option A: provide all three L2 API credentials directly (recommended)
    API_KEY: str = ""
    API_SECRET: str = ""
    API_PASSPHRASE: str = ""

    # Option B: provide private key + wallet address (key is derived at startup)
    WALLET_PRIVATE_KEY: str = ""
    WALLET_ADDRESS: str = ""
    # Signature types:
    #   0 = EOA (standard wallet, gas paid by signer)
    #   1 = POLY_PROXY (Magic Link / email login)
    #   2 = GNOSIS_SAFE (manual Safe Transaction required for pUSD approval)
    SIGNATURE_TYPE: int = 0
    # H-V7-1 fix: deposit / funder address. Required by SIGNATURE_TYPE=1 / 2
    # so the order's `maker` field points at the smart-contract wallet that
    # actually holds the funds (not the EOA signer). Without this, every
    # CLOB /order post is rejected with
    #   "maker address not allowed, please use the deposit wallet flow".
    # EOA flow (SIGNATURE_TYPE=0) also benefits — setting this explicitly
    # makes the maker field deterministic regardless of which signer
    # derived the API key.
    FUNDER_ADDRESS: str = ""

    # ── CLOB Endpoints (usually no need to change) ───────────────────────
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    POLYMARKET_HOST: str = "https://clob.polymarket.com"
    POLYGON_RPC_URL: str = "https://polygon-bor.publicnode.com"

    # ── Trading mode (2026-11-02) ────────────────────────────────────────
    # True  = paper mode: local GTD fill simulation, no real CLOB orders.
    # False = live mode: real CLOB orders.
    # Editable at runtime from /config; the mode is applied to the next T1
    # entry. An already-open round keeps the mode it entered with, so
    # switching never leaves a live/paper position half-managed.
    # Default is paper so a fresh deploy cannot place real orders by
    # accident; switch to live explicitly from the config page.
    PAPER_TRADING: bool = True

    # ── Martingale tiers (USD notional per tier) ─────────────────────────
    # Four-layer budget: T1 entry followed by three averaging orders.
    # Maximum planned exposure per round is 2 + 4 + 8 + 16 = 30 USD.
    # T1..T4 are all GTD BUYs. T1 must buy >= 5 shares (CLOB minimum):
    # with ENTRY_PRICE_CAP=0.40, 2 USD is exactly 5 shares at the cap.
    TIER1_NOTIONAL: float = 2.0
    TIER2_NOTIONAL: float = 4.0
    TIER3_NOTIONAL: float = 8.0
    TIER4_NOTIONAL: float = 16.0

    # ── Strategy thresholds ───────────────────────────────────────────────
    ENTRY_PRICE_CAP: float = 0.40      # max token price to enter
    TIER2_DROP_PCT: float = 0.20       # T2 triggers at baseline * (1 - 20%)
    TIER3_DROP_PCT: float = 0.40       # T3 triggers at baseline * (1 - 40%)
    TIER4_DROP_PCT: float = 0.60       # T4 triggers at baseline * (1 - 60%)
    TP_PROFIT_PCT: float = 0.20         # take profit target as fraction of avg cost
    # N is the number of completed averaging tiers (T2-T4):
    # 0 fills -> 20%, 1 -> 17%, 2 -> 14%, 3 -> 11%.
    TP_DECREMENT_PCT: float = 0.03
    TP_MIN_PCT: float = 0.05             # floor (5%) so TP can never lock in a loss
    BASELINE_DRIFT_LIMIT: float = 0.0015  # 0.15% BTC spot drift cap

    # ── T4 post-entry stop-loss (operator decision 2026-10-31) ────────────
    # Once T4 has filled, the round is at max planned exposure and a
    # continuing collapse is no longer a dip the ladder can average into.
    # Stage 1 sells a fraction at a confirmed drop below T4's fill price;
    # stage 2 exits the remainder on a deeper drop or near expiry.
    STOP_LOSS_ENABLED: bool = True
    # Trigger line = T4 average fill price × (1 − this). 0.45 → 0.55×.
    STOP_LOSS_T4_DROP_PCT: float = 0.45
    # The mid must stay below the stage-1 line for this long before selling
    # (filters single bad prints / thin-book wicks).
    STOP_LOSS_CONFIRM_S: float = 20.0
    STOP_LOSS_STAGE1_SELL_PCT: float = 0.60
    # Stage 2 line = stage-1 line × (1 − this). 0.30 → 0.70× again.
    STOP_LOSS_STAGE2_EXTRA_DROP_PCT: float = 0.30
    # Time floor: with <= STOP_LOSS_TIME_EXIT_S left and mid <= FLOOR,
    # exit the remainder instead of holding a near-worthless lottery.
    STOP_LOSS_TIME_EXIT_S: float = 120.0
    STOP_LOSS_FLOOR: float = 0.05
    # Final exit: last STOP_LOSS_FINAL_EXIT_S seconds and mid below
    # STOP_LOSS_FINAL_MID → sell whatever is left (avoid a binary zero).
    STOP_LOSS_FINAL_EXIT_S: float = 60.0
    STOP_LOSS_FINAL_MID: float = 0.50
    # Backoff after a failed stop POST before retrying.
    STOP_LOSS_RETRY_S: float = 3.0

    # T1 entry time gate: T1 must enter within this many seconds of window open.
    # Prevents chasing at the very end of the 15-min window.
    T1_ENTRY_WINDOW_S: float = 300.0    # 5 minutes

    # ── External signal gates (2026-11-02) ────────────────────────────────
    # The martingale used to average down on a fixed price drop alone. The
    # signal layer vetoes new entries/averaging when the underlying model,
    # trend/volatility regime, CLOB microstructure, 5m timeframe, macro
    # derivatives data or the event calendar says the move is not a
    # mean-reversion opportunity.
    #
    # Modes: enabled=True + log_only=False -> enforce. Set log_only=True to
    # collect the gate metrics without blocking (recommended for the first
    # few days after deploying a new threshold). Set enabled=False to turn
    # the whole layer off. Missing data always fails OPEN: a signal outage
    # must never wedge the ladder.
    SIGNAL_GATES_ENABLED: bool = True
    SIGNAL_GATES_LOG_ONLY: bool = False

    # Digital-option model: P(up) = Phi(ln(S/K) / (sigma * sqrt(tau))).
    # Edge = model probability of the held side - token mid. Only add when
    # the market is cheaper than the model by this much. T1 is allowed a
    # negative edge because the initial entry is a cap-based probe.
    SIGNAL_MIN_EDGE: float = 0.02
    SIGNAL_T1_MIN_EDGE: float = -0.05
    # Adverse distance to the strike, in sigma units. Positive = the
    # underlying has moved against the held side. T4 keeps its existing
    # dedicated gate in addition to this.
    SIGNAL_MAX_ADVERSE_Z: float = 2.0
    SIGNAL_T1_MAX_ADVERSE_Z: float = 2.5
    # Trend/variance regime: trend_score is -1..1; block adding when the
    # trend is strongly against the position.
    SIGNAL_MAX_TREND_AGAINST: float = 0.70
    SIGNAL_T1_MAX_TREND_AGAINST: float = 0.90
    # Slow volatility filter (1m sigma in percent). The existing
    # BTC_VOL_* guard catches a 60s spike; this catches a high-vol regime.
    SIGNAL_MAX_SIGMA_1M_PCT: float = 0.25

    # CLOB microstructure. Missing book/flow data fails open.
    SIGNAL_MICRO_GATE_ENABLED: bool = True
    SIGNAL_MAX_SPREAD: float = 0.03          # (ask-bid)/mid
    SIGNAL_MIN_BOOK_IMBALANCE: float = -0.60  # ask-heavy book blocks
    SIGNAL_MAX_SELL_FLOW: float = 0.60        # |CVD|/volume against a BUY
    SIGNAL_FLOW_WINDOW_S: float = 60.0

    # 5m timeframe confirmation. The bot still trades only BTC 15m; the 5m
    # market is a read-only signal. Missing 5m data fails open.
    SIGNAL_5M_GATE_ENABLED: bool = True
    # T1 also applies the 5m veto. Off by operator decision (2026-11-02):
    # the 300 s T1 entry window is aligned with the 5m market window, so by
    # the time the 15m side is cheap enough to enter, the 5m confirm is
    # usually saturated near -1. T2-T4 keep the 5m confirmation.
    SIGNAL_5M_T1_ENABLED: bool = False
    SIGNAL_5M_MIN_CONFIRM: float = -0.50
    SIGNAL_5M_WINDOW_S: float = 300.0

    # Macro derivatives (Binance USDT-M funding/OI + Deribit DVOL).
    SIGNAL_MACRO_GATE_ENABLED: bool = True
    SIGNAL_MAX_FUNDING_RATE: float = 0.001   # |8h funding| > 0.1%
    SIGNAL_MAX_OI_CHANGE_PCT: float = 3.0    # 5m OI change
    SIGNAL_MAX_DVOL: float = 75.0
    SIGNAL_MAX_DVOL_CHANGE_PCT: float = 20.0
    SIGNAL_MACRO_POLL_S: float = 60.0

    # Event calendar (manual JSON; see data/event_calendar.json).
    SIGNAL_EVENT_GATE_ENABLED: bool = True
    SIGNAL_EVENT_CALENDAR_PATH: str = ""
    SIGNAL_EVENT_PRE_BUFFER_S: float = 300.0
    SIGNAL_EVENT_POST_BUFFER_S: float = 300.0

    # Optional edge/regime-based position scaling. Off by default: the
    # operator's ladder stays 2/4/8/16 USD unless explicitly enabled.
    SIGNAL_SIZE_SCALING_ENABLED: bool = False
    SIGNAL_SIZE_MIN_MULT: float = 0.25
    SIGNAL_SIZE_TARGET_EDGE: float = 0.10

    # Regime recalculation interval (the ring scan is not free).
    SIGNAL_REGIME_REFRESH_S: float = 5.0
    SIGNAL_REGIME_LOOKBACK_S: float = 1200.0

    # ── Trading fees (official formula) ───────────────────────────────────
    # fee = C × feeRate × p × (1 - p)
    #   C = shares, p = fill price, taker-only; makers pay 0.
    # Crypto category: taker 0.07, maker 0, maker rebate 20% (rebates are
    # not credited here — we only avoid overstating PnL). Set 0 to disable
    # fee accounting entirely (the CLOB trade payload's fee_rate_bps field
    # has been observed as 0 even for markets whose /fee-rate reports 1000,
    # so the official category rate is used instead).
    TAKER_FEE_RATE: float = 0.07

    # Boot-time window-age guard: if the process starts more than
    # ENTRY_OPEN_OFFSET_GRACE_SEC into a window, skip that window entirely.
    SKIP_IF_LATE_WINDOW_AT_BOOT: bool = True
    ENTRY_OPEN_OFFSET_GRACE_SEC: float = 180.0

    # Skip a new window when BTC spot jumped more than this percent between
    # the previous window's recorded price and the new window's open.
    # 0 disables the guard.
    CROSS_WINDOW_DRIFT_SKIP_PCT: float = 0.5

    # ── BTC feed health / short-term volatility guards ────────────────────
    # Data freshness: if the Binance WS trade feed has not updated for this
    # many seconds, pause NEW entries/averaging (TP exits stay enabled).
    # 0 disables the guard.
    BTC_STALE_MAX_AGE_S: float = 5.0
    # Volatility blackout: when (max-min)/min over the window exceeds this
    # percentage (a sharp rise or drop), pause new entries/averaging and
    # cancel resting BUY GTDs for a cooldown period. 0 disables.
    BTC_VOL_WINDOW_S: float = 60.0
    BTC_VOL_MAX_MOVE_PCT: float = 0.3
    BTC_VOL_COOLDOWN_S: float = 60.0

    # T4 time-based drift bypass (operator decision 2026-10-31).
    # T4 is the last/largest tier and has no momentum bypass, so a trending
    # window (drift > BASELINE_DRIFT_LIMIT) blocks it exactly when the token
    # crashes into its zone. When the window still has at least this many
    # seconds left, allow T4 at its normal 60%-drop trigger even if the BTC
    # drift gate fails — there is still time for the position to recover.
    # 0 disables (T4 always requires drift within limit).
    T4_DRIFT_BYPASS_MIN_REMAINING_S: float = 540.0

    # ── T4 entry gates (operator decision 2026-10-31) ─────────────────────
    # T4 adds 16 of the 30 USD planned exposure at a price 60% below T1.
    # A fixed price drop alone cannot tell a trend (do not add) from an
    # overreaction (add), so the largest step is additionally conditioned
    # on the underlying's distance from the window strike and on the time
    # left. All gates fail-open when their inputs are unavailable: the
    # BTC feed-health guard already covers a dead feed, and a new gate
    # must never be able to wedge the ladder.
    #
    # Skip T4 when fewer than this many seconds remain in the window:
    # the 300 s GTD and the recovery to TP cannot play out, and round-end
    # cancels the order anyway. 0 disables.
    T4_MIN_REMAINING_S: float = 240.0
    # Distance-to-strike gate. The 15-minute token is a probability that
    # BTC closes on one side of the window-open price. z = adverse-side
    # distance / (sigma_1m * sqrt(remaining_minutes)); "adverse" flips
    # sign by position side. Add only while the adverse z is at most this.
    # Higher = more permissive; 0 = only add when BTC is on the winning
    # side of the strike. 0 does not disable (see the ENABLED flag).
    T4_STRIKE_GATE_ENABLED: bool = True
    T4_MAX_STRIKE_Z: float = 2.0
    # Fallback 1-minute BTC sigma (percent) when the 60 s range sample is
    # too thin. The live estimate is range/2, so a fast spike (possible
    # overreaction) raises sigma and a slow grind keeps it at the floor.
    T4_STRIKE_SIGMA_PCT_PER_MIN: float = 0.05
    # Do not arm T4 once the token mid has already gapped more than this
    # fraction below the T4 trigger: wait for the price to come back into
    # the pre-arm zone instead of catching a falling knife.
    # 0 = arm at any mid at/below the fire line (old behaviour).
    T4_MAX_GAP_BELOW_TRIGGER_PCT: float = 0.05

    # ── T4 decision journal (2026-10-31) ─────────────────────────────────
    # Append-only JSONL of T4 entry/exit events (decisions with z/time/
    # sigma, fills, TP/stop exits, round resolution) used to tune the
    # gates after a few weeks of data. Empty path = <data dir>/
    # t4_journal.jsonl. Not a runtime-config knob (the path is
    # filesystem-bound), but env/.env.docker work.
    T4_JOURNAL_ENABLED: bool = True
    T4_JOURNAL_PATH: str = ""

    # Pause NEW entries/averaging without touching TP or stop-loss
    # management. The backtest page toggles this while it collects data;
    # any open position can still exit normally. (The full emergency stop
    # remains available separately.)
    PAUSE_NEW_ENTRIES: bool = False

    # NOTE: Same-window T1 re-entry after a take-profit is deliberately NOT
    # supported (operator decision 2026-10-31). Once a round has taken
    # profit, `entry_token` is kept until the next window so a fresh
    # martingale ladder cannot start — re-entering late in the same window
    # was judged too risky.

    # ── Order settings ────────────────────────────────────────────────────
    # GTD lifetime in seconds. CLOB enforces a 180-second hard floor
    # (`CLOBClient.GTD_MIN_SEC = 180`); values below that are clamped to
    # 181 by the CLOB client. The default 300 s gives the maker an extra
    # 2 minutes on the book after the pre-arm window opens — covering the
    # full 5-min averaging chain without forcing a re-post in the middle
    # of a fast cascade.
    GTD_LIFETIME_S: int = 300

    # Slippage for FAK sell orders (as a fraction, e.g. 0.02 = 2%)
    SELL_SLIPPAGE_PCT: float = 0.02

    # Startup reconcile: force-cancel every open CLOB order on the account.
    # This frees maker allowance left behind by a crashed process, but it is
    # account-wide — if the same wallet/API key is shared with another bot or
    # manual orders, those will be cancelled too. Set to False when sharing.
    STARTUP_RECONCILE_CANCEL_ALL: bool = True

    # Before POSTing a GTD, query the CLOB's open orders for the same
    # asset + side + price and refuse to place if one already exists.
    # This is the defence against ORPHAN orders: a POST whose response was
    # lost (timeout / connection reset) may have been accepted server-side,
    # so the local OrderManager does not know about it. Without this check
    # the next strategy tick would place a second GTD at the same tier.
    # The query failing is treated as "cannot verify" → refuse to place.
    VERIFY_NO_EXISTING_ORDER: bool = True

    # ── GTD pre-arm margin (核心新参数) ────────────────────────────────────
    # GTD 挂单**时机**提前量，按策略价的比例计算。
    #   BUY  : 触发线 = strategy_price * (1 + PRICE_PRE_MARGIN)
    #           例 PRICE_PRE_MARGIN=0.10, strategy_price=0.40 → mid<=0.44 时挂 0.40
    #   SELL : 触发线 = strategy_price * (1 - PRICE_PRE_MARGIN)
    #           例 PRICE_PRE_MARGIN=0.10, tp_target=0.40 → mid>=0.36 时挂 0.40
    # 设计目的：把 GTD 提前挂在订单簿上，等市价回到策略价成交。
    # 挂单价 = strategy_price（不变），价格提前量只用于触发判断。
    PRICE_PRE_MARGIN: float = 0.10

    # 挂单/撤单之间的滞回缓冲（hysteresis），按触发线的比例计算。
    # 同一 tick 里挂单和撤单用同一条线时，mid 在阈值附近抖动会导致
    # 「挂 3 秒 → 撤 → 再挂」的循环。撤单需要比挂单线多走这么多才执行：
    #   TP 撤单   : mid < fire_line * (1 - h)
    #   TP 区撤买单: mid >= tp_fire_line * (1 + h)
    #   下一层撤单 : mid <= next_fire_line * (1 - h)
    CANCEL_HYSTERESIS_PCT: float = 0.02

    # 止盈是否也受 BTC 漂移门限制。默认 False：漂移门用于过滤"入场"，
    # 不应挡住减仓/离场。生产 2026-10-30：mid 已越过止盈触发线，但 BTC
    # 漂移 FAIL 导致卖单一直没挂出，随后价格回落约 60%。
    # 设为 True 恢复旧行为（TP 也要求 drift 在限内）。
    TP_REQUIRE_DRIFT_GATE: bool = False

    # TierWatcher WS-event 兜底超时（秒）。
    # Watcher 主路径是等 `order.UPDATE` 推过来（~50ms 唤醒），但当 WS
    # 失联 / 漏事件时，必须有一个上限——超过这个时间就必须主动查 REST
    # balance，否则持仓更新会卡住。这不是"主动轮询间隔"——99% 的 tick
    # 是事件驱动，poll_s 只决定"如果 WS 不来，最长等多久"。生产默认
    # 1.5s 给 WS 充分时间重连；测试用 0.5s 是 TierWatcher.__init__ 里
    # `max(0.5, ...)` 的下限——再低也不会生效。
    TIER_WATCHER_POLL_S: float = 1.5

    # ── Signal-driven paper strategy (2026-11-02) ─────────────────────────
    # BOT_MODE=signal runs `src.signal_main` instead of the martingale
    # engine. That bot is PAPER ONLY: it never creates a CLOB client and
    # never posts an order. It uses the public market feeds, the existing
    # signal layer, and a local fill simulator.
    BOT_MODE: str = "martingale"       # "martingale" | "signal"
    SIGNAL_TRADE_ENABLED: bool = True
    SIGNAL_TRADE_STAKE_USD: float = 2.0
    # Only trade the favourite side in this band. The 6-month study showed
    # 0.75-0.90 is the least-bad region; 0.1-0.3 is dominated by spread.
    SIGNAL_TRADE_PRICE_MIN: float = 0.75
    SIGNAL_TRADE_PRICE_MAX: float = 0.90
    # 5m confirmation is the primary positive driver (the digital edge is
    # not: its Brier is worse than the market's and it is anti-predictive).
    SIGNAL_TRADE_M5_MIN_CONFIRM: float = 0.20
    SIGNAL_TRADE_REQUIRE_M5: bool = True
    SIGNAL_TRADE_TP_PCT: float = 0.10
    SIGNAL_TRADE_SL_PCT: float = 0.15
    # Force-close this many seconds before the window ends (avoids binary
    # settlement risk).
    SIGNAL_TRADE_TIME_EXIT_S: float = 60.0
    # Entry allowed from this many seconds after the window open ...
    SIGNAL_TRADE_ENTRY_START_S: float = 60.0
    # ... until this many seconds before the window end.
    SIGNAL_TRADE_ENTRY_END_BEFORE_END_S: float = 180.0
    SIGNAL_TRADE_MAX_ENTRIES: int = 1
    SIGNAL_TRADE_ALLOW_REENTRY: bool = False
    # Maker entry/exit avoids the taker fee; the paper simulator fills a
    # resting limit when a trade prints through it or the mid crosses.
    SIGNAL_TRADE_ENTRY_MAKER: bool = True
    SIGNAL_TRADE_EXIT_MAKER: bool = True
    # Fallback half-spread used when the live book has no bid/ask.
    SIGNAL_TRADE_MAKER_OFFSET: float = 0.005
    SIGNAL_TRADE_ENTRY_TIMEOUT_S: float = 5.0
    SIGNAL_TRADE_MAX_ATTEMPTS: int = 3
    # Extra filters on top of the SignalEngine reasons. Missing data
    # fails open (the engine's values are None), except the 5m driver.
    SIGNAL_TRADE_MAX_SPREAD: float = 0.03
    SIGNAL_TRADE_MIN_IMBALANCE: float = -0.60
    SIGNAL_TRADE_MAX_SELL_FLOW: float = 0.60
    SIGNAL_TRADE_MAX_TREND_AGAINST: float = 0.70
    SIGNAL_TRADE_MAX_SIGMA_PCT: float = 0.25
    # The digital edge is deliberately ignored by default (set a value in
    # [-1, 1] to enable it as a filter). The 6-month calibration study
    # found higher edge = lower actual win rate.
    SIGNAL_TRADE_MIN_EDGE: float = -1.0
    SIGNAL_TRADE_USE_SIGNAL_GATES: bool = True
    # SignalEngine reasons that must NOT block a signal-trader entry.
    # `edge` and `strike_z` are model-derived and anti-predictive in the
    # backtest; `5m_against` is replaced by the positive M5 threshold.
    SIGNAL_TRADE_IGNORE_REASONS: str = "edge,strike_z,5m_against"
    # Optional: exit when the held side's 5m confirmation flips negative.
    SIGNAL_TRADE_EXIT_ON_FLIP: bool = False
    SIGNAL_TRADE_FLIP_CONFIRM: float = 0.0
    # Forward-research snapshot JSONL (default <data dir>/signal_snapshots.jsonl).
    SIGNAL_TRADE_SNAPSHOT_S: float = 2.0
    SIGNAL_TRADE_SNAPSHOT_PATH: str = ""

    # ── Web dashboard ─────────────────────────────────────────────────────
    # FastAPI app served in-process on WEB_PORT (default 8010). Set
    # WEB_PASSWORD (secret — Coolify panel / process env) to require HTTP
    # Basic auth; leaving it empty exposes the dashboard publicly.
    WEB_ENABLED: bool = True
    WEB_HOST: str = "0.0.0.0"
    WEB_PORT: int = 8010
    WEB_LOG_LEVEL: str = "warning"
    WEB_USERNAME: str = "admin"
    WEB_PASSWORD: str = ""
    # Dashboard login session lifetime (seconds). Sessions are kept in
    # memory, so a restart/redeploy forces a fresh login.
    WEB_SESSION_TTL_S: int = 7 * 24 * 3600

    # ── Derived helpers ───────────────────────────────────────────────────

    @property
    def max_tier(self) -> int:
        return 4

    @property
    def tier_notionals(self) -> list[float]:
        return [
            self.TIER1_NOTIONAL,
            self.TIER2_NOTIONAL,
            self.TIER3_NOTIONAL,
            self.TIER4_NOTIONAL,
        ]

    @property
    def tier_drop_pcts(self) -> list[float | None]:
        return [
            None,
            self.TIER2_DROP_PCT,
            self.TIER3_DROP_PCT,
            self.TIER4_DROP_PCT,
        ]

    @property
    def total_max_notional(self) -> float:
        """Maximum total USD exposure across all tiers."""
        return sum(self.tier_notionals)

    @property
    def has_credentials(self) -> bool:
        """True if L2 API credentials are available."""
        return bool(self.API_KEY and self.API_SECRET)


# ---------------------------------------------------------------------------
# Research / backtest settings
# ---------------------------------------------------------------------------


class BacktestSettings(BaseSettings):
    """In-process historical backtest (dashboard 回测 page)."""

    model_config = SettingsConfigDict(
        env_file=(str(_ROOT / ".env.docker"), str(_ROOT / ".env")),
        extra="ignore",
    )

    BACKTEST_ENABLED: bool = True
    # Start a collection/replay job on every process start. Cached
    # windows are skipped, so this is incremental after the first run.
    BACKTEST_COLLECT_ON_START: bool = True
    BACKTEST_COLLECT_DAYS: int = 7
    BACKTEST_MAX_DAYS: int = 30
    # Seconds between future.news / data-API requests; keep it polite.
    BACKTEST_RATE_LIMIT_S: float = 0.15
    # Run the A/B variants (T4 off/2u, stop 30%, z=1) in every report.
    BACKTEST_AUTO_VARIANTS: bool = True
    BACKTEST_DB_PATH: str = ""
    BACKTEST_REPORT_PATH: str = ""


# ---------------------------------------------------------------------------
# Singleton access
# ---------------------------------------------------------------------------

_general: GeneralConfig | None = None
_binance: BinanceConfig | None = None
_baseline: BaselineConfig | None = None
_log: LogConfig | None = None
_strategy: StrategySettings | None = None
_backtest: BacktestSettings | None = None


def general() -> GeneralConfig:
    global _general
    if _general is None:
        _general = GeneralConfig()
    return _general


def binance() -> BinanceConfig:
    global _binance
    if _binance is None:
        _binance = BinanceConfig()
    return _binance


def baseline_cfg() -> BaselineConfig:
    global _baseline
    if _baseline is None:
        _baseline = BaselineConfig()
    return _baseline


def log_config() -> LogConfig:
    global _log
    if _log is None:
        _log = LogConfig()
    return _log




def strategy() -> StrategySettings:
    global _strategy
    if _strategy is None:
        _strategy = StrategySettings()
    return _strategy


def backtest() -> BacktestSettings:
    global _backtest
    if _backtest is None:
        _backtest = BacktestSettings()
    return _backtest
