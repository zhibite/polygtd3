# 外部信号闸门（Signal Gates）逻辑说明

> 对应代码版本：`bd9a1f5`（2026-11-02）。
> 参数默认值来自 `src/config.py`；带 ★ 的参数可在 dashboard「配置」页修改，下一 tick 生效。

## 1. 定位

信号闸门是入场/补仓前的**最后一道否决**，不改变原有的价格、漂移、时间窗逻辑：

- 只决定「要不要开新仓 / 补仓」，**不参与任何卖出**。止盈、止损、尾盘清仓、结算路径都不调用它。
- 默认 **fail-open**：任何上游数据缺失、计算异常、5m 市场找不到，都视为「不拦截」，避免信号源故障把策略卡死。
- 两处接入：
  - T1 入场：`src/strategy/engine.py` → `_check_t1_entry()`（约 1700 行）
  - T2–T4 补仓：`_check_average_down()`（约 2067 行）
- 空仓或暂停时，`_monitor_signal()` 仍会计算便宜一侧的快照，只刷新 dashboard，不下单、不写 `engine.signal_gate` 日志。

## 2. 接入顺序

### 2.1 T1 入场

```
PAUSE_NEW_ENTRIES / BTC 源过期 / 急涨急跌
  → 已有仓位 / POST 失败 backoff
  → 预挂线：mid > ENTRY_PRICE_CAP × (1 + PRICE_PRE_MARGIN) 则不触发
  → T1 入场时间窗：elapsed > T1_ENTRY_WINDOW_S 则关闭
  → 价格上限：mid > ENTRY_PRICE_CAP 则拒绝
  → BTC 漂移门 GateKeeper.can_enter()
  → 仓位 / stake 上限
  → ★ 信号闸门（本文）
  → 通过：ActionOpenT1
```

买入价是 `ENTRY_PRICE_CAP`（默认 0.40）；`PRICE_PRE_MARGIN=0.10` 时，mid 跌到 0.44 就开始预挂。

### 2.2 T2–T4 补仓

```
tier 价格触发（相对 T1 均价的跌幅）
  → T4 专属门（剩余时间 / 行权价 z / 低于触发价幅度）
  → BTC 漂移门 can_double_down()（T4 有基于剩余时间的 bypass）
  → ★ 信号闸门
  → 预算 / 最小份数 / stake 上限
  → 挂 GTD
```

补仓被信号否决时直接 `return`，**不会跳过去补更深一层**（不会出现 T2 被挡却补 T3 的盲目加仓）。

## 3. 模式

| `SIGNAL_GATES_ENABLED` | `SIGNAL_GATES_LOG_ONLY` | `mode` | `allowed` | 行为 |
|---|---|---|---|---|
| true | false | `enforce` | 仅当没有 reasons | 拦截 |
| true | true | `log` | 恒为 true | 只记录，不拦截 |
| false | — | `off` | 恒为 true | 整层关闭 |

- `would_block` 在三种模式下都按规则计算，dashboard 和 journal 都能看到「本来会不会被拦」。
- 生产启动日志 `signals.configured enabled=True log_only=False` 表示 enforce。
- 建议新阈值先用 log-only 跑几天，再用 journal 回放评估。

## 4. 数据源

| 指标 | 来源 | 更新方式 |
|---|---|---|
| BTC 现价 / 1m 波动 / 趋势 | Binance 现货成交（`RingBuffer`，保留 1200s） | WS 实时 |
| token mid / spread / 盘口深度 | Polymarket CLOB 共享 `PriceCache`（MarketWS） | WS + REST 兜底 |
| 60s 主动买卖流（CVD） | CLOB 成交的 `side` 字段 | WS 实时 |
| 5m 确认 | BTC 5m Up/Down 市场（只读） | REST midpoint 每 ~2s，市场每 10s 重新发现 |
| funding / OI / DVOL | Binance USDT-M + Deribit | 后台轮询，`SIGNAL_MACRO_POLL_S=60` |
| 事件黑名单 | `data/event_calendar.json`（或 `SIGNAL_EVENT_CALENDAR_PATH`） | 每 15s 检查 mtime，变化才重读 |

### 4.1 5m 确认源

`src/market/signal_market.py`：

- 只做信号，不下单；不再订阅 5m 的 WebSocket（`market_ws=None`），全部走 REST midpoint。
- 每 10s 通过 Gamma 找当前 BTC 5m 市场（slug `btc-updown-5m-<ts>`），按 Gamma `outcomes` 映射 Up/Down token，不假设 token 顺序。
- 每 ~2s 采样一次 Up/Down midpoint，写入共享 `PriceCache`；5m 窗口切换时清掉旧 token 和样本。
- `snapshot(side)` 需要至少 2 个样本，否则 `available=False`（fail-open）。
- T1 是否使用这个门由 `SIGNAL_5M_T1_ENABLED` 控制（**默认关闭**，2026-11-02 操作员决定）；T2–T4 始终使用，但整体还受 `SIGNAL_5M_GATE_ENABLED` 总开关约束。
- `confirm` 计算：
  ```
  confirm = clamp( (side_mid - 0.5) × 2
                 + clamp(side_change_60 × 20, -0.5, +0.5)
                 - clamp(max(0, other_change_60) × 10, 0, +0.5), -1, +1 )
  ```
  `side_mid` 是持仓方向的 5m midpoint，`side_change_60` 是该 token 60 秒涨跌幅，`other_change_60` 是对手方的 60 秒涨幅（只惩罚对手方上涨）。

## 5. 指标计算

### 5.1 1 分钟波动率 σ

两条来源取较大值：

- `engine._btc_sigma_1m()`：最近 60s 的 `(high-low)/low/2`，下限 `T4_STRIKE_SIGMA_PCT_PER_MIN=0.05`（%/分钟）。
- `signals._refresh_regime()`：把 ring buffer 聚合成 1 分钟 bar，最近最多 20 根，用 1 分钟对数收益率的样本标准差；下限同上。

σ 每 `SIGNAL_REGIME_REFRESH_S=5s` 重算一次。

### 5.2 趋势

用同样的 1 分钟 bar：

```
trend_raw  = (EMA5 - EMA15) / EMA15
normalizer = max(σ, floor) × sqrt(5)
trend_score = clamp(trend_raw / normalizer, -1, +1)

side_sign    = +1 (UP) / -1 (DOWN)
trend_against = max(0, -(side_sign × trend_score))
```

`trend_against > 0` 表示趋势与持仓方向相反。

### 5.3 数字期权模型与 edge

把标的当作行权价 = 窗口开盘价 K 的数字期权：

```
z          = ln(spot / K) / (σ × sqrt(τ分钟))
fair_up    = Φ(z)                      # 标准正态 CDF
fair_held  = fair_up (UP) / 1 - fair_up (DOWN)
edge       = fair_held - token_mid
adverse_z  = -z (UP) / +z (DOWN)       # 正值 = 标的已朝不利方向走
```

- `spot` = Binance 最新价，`K` = GateKeeper baseline（窗口开盘价），`τ` = 距窗口结束的剩余秒数（最少按 1 秒算）。
- 输入不完整（spot/K/σ/τ 任一缺失）时，`edge`、`adverse_z` 为 `None`，对应规则不触发（fail-open）。

### 5.4 盘口微观结构

| 指标 | 定义 |
|---|---|
| `spread` | `(best_ask - best_bid) / mid`，无 BBO 时为 `None` |
| `book_imbalance` | `(bid_depth - ask_depth) / (bid_depth + ask_depth)`，top 5 档 |
| `cvd_ratio` | 最近 60s 的 `Σ signed_size / Σ |size|`；BUY 为正、SELL 为负 |

### 5.5 宏观/衍生品

- `funding_rate`：Binance BTCUSDT 永续最新 funding（8 小时费率）。
- `oi_change_pct`：当前 OI 相对约 5 分钟前样本的变化百分比（60s 轮询）。
- `dvol`：Deribit BTC DVOL；`dvol_change_pct`：相对约 1 小时前的变化百分比。
- 取不到值时对应字段为 `None`，规则跳过。

### 5.6 事件黑名单

```
data/event_calendar.json
{
  "events": [
    {"name": "FOMC", "start": "2026-11-05T18:00:00Z",
                      "end":   "2026-11-05T19:00:00Z"}
  ]
}
```

`start`/`end` 支持 ISO 时间或 `start_ts`/`end_ts` Unix 秒；黑名单窗口 = `[start - PRE_BUFFER_S, end + POST_BUFFER_S]`，默认前后各 300s。文件缺失或格式错误 = 没有事件。

## 6. 判定规则

每条规则独立追加 reason；在 `enforce` 模式下**任意一个 reason 即拦截**。

| reason | 触发条件 | T1 阈值 | T2–T4 阈值 | 参数 |
|---|---|---|---|---|
| `strike_z` | `adverse_z > max` | 2.5 | 2.0 | `SIGNAL_T1_MAX_ADVERSE_Z` / `SIGNAL_MAX_ADVERSE_Z` ★ |
| `edge` | `edge < min` | -0.05 | 0.02 | `SIGNAL_T1_MIN_EDGE` / `SIGNAL_MIN_EDGE` ★ |
| `trend_against` | `trend_against > max` | 0.90 | 0.70 | `SIGNAL_T1_MAX_TREND_AGAINST` / `SIGNAL_MAX_TREND_AGAINST` ★ |
| `vol_regime` | `σ × 100 > max`（σ 为小数） | 0.25 | 0.25 | `SIGNAL_MAX_SIGMA_1M_PCT` ★ |
| `spread_wide` | `spread > max` | 0.03 | 0.03 | `SIGNAL_MAX_SPREAD` ★ |
| `book_ask_heavy` | `book_imbalance < min` | -0.60 | -0.60 | `SIGNAL_MIN_BOOK_IMBALANCE` ★ |
| `sell_flow` | `cvd_ratio < -max` | 0.60 | 0.60 | `SIGNAL_MAX_SELL_FLOW` ★ |
| `5m_against` | `confirm < min`（且 5m 数据可用） | -0.50 | -0.50 | `SIGNAL_5M_MIN_CONFIRM` ★ |
| `funding_extreme` | `abs(funding) > max` | 0.001 | 0.001 | `SIGNAL_MAX_FUNDING_RATE` ★ |
| `oi_spike` | `abs(oi_change_pct) > max` | 3.0% | 3.0% | `SIGNAL_MAX_OI_CHANGE_PCT` ★ |
| `dvol_extreme` | `dvol > 75` 或 `dvol_change_pct > 20%` | — | — | `SIGNAL_MAX_DVOL` / `SIGNAL_MAX_DVOL_CHANGE_PCT` ★ |
| `event_blackout:<name>` | 当前处于事件黑名单窗口 | — | — | `SIGNAL_EVENT_GATE_ENABLED` ★ |

子开关（可单独关闭某一组）：`SIGNAL_MICRO_GATE_ENABLED`、`SIGNAL_5M_GATE_ENABLED`、`SIGNAL_MACRO_GATE_ENABLED`、`SIGNAL_EVENT_GATE_ENABLED`（均默认开启）。

`5m_against` 的适用范围：T1 仅在 `SIGNAL_5M_T1_ENABLED=true` 时生效（**默认 false**）；T2–T4 始终生效。关闭 `SIGNAL_5M_T1_ENABLED` 不影响 `SIGNAL_5M_GATE_ENABLED` 对补仓的约束。

固定值、不在 dashboard 白名单内、需要改环境变量的参数：`SIGNAL_FLOW_WINDOW_S`（60s）、`SIGNAL_5M_WINDOW_S`（300s）、`SIGNAL_MACRO_POLL_S`（60s）、`SIGNAL_EVENT_CALENDAR_PATH`、`SIGNAL_EVENT_PRE/POST_BUFFER_S`、`SIGNAL_REGIME_*`、`SIGNAL_SIZE_TARGET_EDGE`。

## 7. 仓位缩放（默认关闭）

`SIGNAL_SIZE_SCALING_ENABLED=false` 时 `size_multiplier=1.0`。开启后**只作用于 T2–T4**（T1 不缩放）：

```
min_mult   = clamp(SIGNAL_SIZE_MIN_MULT, 0, 1)          # 0.25
target     = max(min_edge + 1e-9, SIGNAL_SIZE_TARGET_EDGE)  # 0.10
edge_scale = clamp((edge - min_edge) / (target - min_edge), 0, 1)
mult       = min_mult + (1 - min_mult) × edge_scale
mult      *= 1 - 0.35 × min(1, trend_against)
mult      *= 1 - 0.35 × min(1, σ% / SIGNAL_MAX_SIGMA_1M_PCT)
mult       = clamp(mult, min_mult, 1)
```

`order_notional = (tier.notional - cost_filled) × mult`。注意 log-only 模式下 `allowed=True`，若缩放开着，会照样按信号缩量。

## 8. 日志、journal 与 dashboard

### 8.1 日志

- `engine.signal_gate`（INFO）：每次评估输出一行，字段包括
  `kind, side, mid, mode, allowed, would_block, reasons, edge, adverse_z,
  fair_held, trend_against, sigma_1m_pct, spread, book_imbalance, cvd_ratio,
  tf5m_confirm, funding_rate, oi_change_pct, dvol, event_name, size_multiplier`。
- 记录节流（`engine._log_signal_decision`）：
  - **拦截**的判定：每次评估都写（没有节流，量大）。
  - **放行**的判定：每个 `(kind, side)` 最多 30 秒一行。
- `engine.t1_skip_signal` / `engine.tN_skip_signal`：进入拦截分支后的摘要，按 `(reason, side/tier)` 每 5 秒最多一行。
- `_monitor_signal()` 只更新内存快照，**不写日志、不写 journal**。

### 8.2 Journal

- `src/infra/t4_journal.py` 把 `engine.signal_` 前缀的事件追加到 `/app/data/t4_journal.jsonl`，包含完整快照字段和 `ts_ms`。
- 文件在 Coolify 持久卷上，append-only，**没有轮转**；enforce 高峰期每个 T1 窗口会产生数百到上千条记录（每条约 300–400 字节）。
- 适合用 Python/jq 直接按 `kind`、`side`、`reasons`、`tf5m_confirm` 聚合，做阈值回放。

### 8.3 Dashboard

`/api/status` 的 `signals` 字段来自 `engine.signal_snapshot()`（最近一次评估），面板展示：

- 结论（拦截/放行）、模式、kind/side、reasons
- 数字期权模型：fair / mid / edge / adverse z / σ
- 趋势/波动：trend_score、trend_against、RSI、ATR、bar 数
- 盘口：spread、imbalance、CVD 占比
- 5m 确认：UP/DOWN midpoint、60s 变化、confirm
- 宏观与事件：funding、OI 变化、DVOL 及变化、事件名/结束时间

## 9. 最近的生产观察（2026-11-02）

数据来自 deploy `bd9a1f5` 后的日志和 `/app/data/t4_journal.jsonl`，仅作当前状态记录，不是规则本身：

- 05:53–06:23 共 2 个完整 T1 窗口：信号闸门评估 **644 次，放行 0 次**；另有 20 条 `t1_skip_price_cap`。
- 信号拦截 46 条节流日志：`5m_against` **46/46（100%）**，`spread_wide` 24/46（52%）。
- 信号层启用以来（journal 约 1 小时）：T1 评估 **1276 次，放行 2 次（0.16%）**；1274 次拦截全部带 `5m_against`，577 次同时带 `spread_wide`。两次放行发生在本次部署之前。
- 结构原因：策略只在便宜一侧 `mid ≤ 0.40` 时开 T1；等价格跌到这个区间，5 分钟窗口往往已走了 2–5 分钟，5m 市场对同一侧的报价通常已经很低，`confirm` 大量饱和在 -1 附近，于是被 `SIGNAL_5M_MIN_CONFIRM=-0.50` 拦下。T1 默认入场窗口（300s）与 5m 窗口几乎完全对齐，窗口后段的 5m 价格接近结算价，是这种「饱和」的主要来源。
- `spread_wide` 的默认阈值 0.03 也偏紧：生产盘口常见 0.031–0.032。

调参建议（不改代码，dashboard 即时生效）：

1. 先把 `只记录不拦截` 打开跑 1–2 天，用 journal 统计各 reason 的占比和「本来会开的单」。
2. 若确认主要是 5m 门槛过严：当前默认策略是 **T1 不使用 5m**（`SIGNAL_5M_T1_ENABLED=false`），T2–T4 补仓仍保留；这比整体关闭 5m 门更精确。若要进一步放开补仓，再考虑关闭全局 5m 门或调整 `5m 最小确认分`——注意 confirm 常饱和在 -1，只有设为 -1.0 才会全部放行。
3. `spread` 默认 0.03 偏紧，但单独放宽不会增加轮次级机会；建议在 5m 策略确定后再观察。
4. 不要同时放宽多个阈值；每次只改一个，观察 24 小时。
5. `PAUSE_NEW_ENTRIES`、live/paper 切换与信号闸门相互独立，互不覆盖。

日志阅读提醒：

- `engine.t1_cap_rejected_summary duration_s=...` 统计的是「从第一次超过预挂线到本轮结束」的时长，**不是**「整轮都在价格上限之上」，不要用它判断价格上限的实际拦截时长。
- 没有看到 `allowed=True` 的日志不代表没有评估：放行日志被节流到 30 秒一条，拦截日志才是逐次记录。

## 10. 回测中的信号逻辑

`src/backtest/model.py::_signal_ok()` 只回放信号层的子集：

- 数字期权模型（fair / edge / adverse_z）
- 趋势门
- 波动率门

**不包含**盘口微观结构、5m 确认、宏观和事件黑名单（回测没有这些数据）。因此回测结果偏乐观，不能用来判断生产环境是否会被 5m 门槛拦住。`src/backtest/runner.py` 会额外跑 `signals_off` 和 `signals_strict` 两个对照变体。

## 11. 代码索引

| 文件 | 内容 |
|---|---|
| `src/strategy/signals.py` | `SignalEngine.evaluate()`：全部指标与 reason |
| `src/market/signal_market.py` | `Signal5mFeed`：5m 市场发现、采样、confirm |
| `src/feeds/binance_futures.py` | `MacroSignals`：funding / OI / DVOL |
| `src/strategy/event_calendar.py` | `EventCalendar`：事件黑名单 |
| `src/strategy/engine.py` | T1 / 补仓接入、`_monitor_signal()`、日志节流 |
| `src/main.py` | SignalEngine / 5m / macro / 事件源的装配（约 2767–2846 行） |
| `src/config.py` | 全部 `SIGNAL_*` 默认值（约 185–254 行） |
| `src/web/runtime_config.py` | dashboard 可编辑参数白名单与持久化 |
| `src/infra/t4_journal.py` | journal 事件过滤（`engine.signal_` 前缀） |
| `src/backtest/model.py` | 回测中的信号子集 `_signal_ok()` |
| `tests/test_signals_2026_11_02.py` | 信号层单元测试与回测对照 |
