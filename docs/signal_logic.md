# polygtd3 信号逻辑与使用说明

> 对应代码：2026-11-02，`BOT_MODE=signal` 的纸盘版本。  
> 相关文档：[signal_trader.md](signal_trader.md)（部署/实现）、[signal-gates.md](signal-gates.md)（旧马丁闸门）、[polymarket-pnl-logic.md](polymarket-pnl-logic.md)（UP/DOWN token PnL）。

## 1. 总览

当前 polygtd3 跑的是**信号驱动纸盘**。核心是 `SignalEngine`：每次决策先对 UP、DOWN 各算一份信号快照，然后由 `SignalTrader` 决定是否开仓、开哪边、何时退出。

```
Binance 现货 WS/RingBuffer ─┐
CLOB 行情 WS/REST ──────────┤→ SignalEngine.evaluate() → snapshot + reasons
5m Up/Down 只读 feed ───────┤                              │
宏观 funding/OI/DVOL ───────┤                              ├→ 旧马丁：否决层（p1/p2）
事件日历 ───────────────────┘                              └→ 当前 SignalTrader：驱动层（polygtd3）
```

旧马丁的 `StrategyEngine` 仍保留在仓库里，但 `BOT_MODE=signal` 不会执行它。signal 模式不创建 CLOB 客户端，不可能发送真实订单。

## 2. 数据来源与刷新

| 数据 | 来源 | 刷新 |
|---|---|---|
| BTC 现货价格 | Binance 现货 WS 成交 | 实时 |
| BTC 最近 1200s 样本 | `RingBuffer` | 实时 |
| 15m UP/DOWN mid | CLOB REST `/midpoint` | 每 2s |
| 15m 盘口 bid/ask/深度 | CLOB MarketWS + REST `/book` top-5 | 实时 / 2s |
| 15m 成交流 CVD | MarketWS `last_trade_price` / `price_change` 的 side/size | 实时 |
| 5m UP/DOWN mid + confirm | `Signal5mFeed` 只读 REST | 每 2s 采样，每 10s 重新发现市场 |
| funding / OI | Binance USDT-M | 每 60s |
| DVOL | Deribit | 每 60s |
| 事件黑名单 | `data/event_calendar.json` | 每 15s 检查 mtime |
| 窗口开盘 BTC 价 | Binance 1m K 线回溯 | 每轮开盘 |

## 3. 每个信号怎么算

代码：[../src/strategy/signals.py](../src/strategy/signals.py)

### 3.1 数字期权模型：z / fair / edge

把 15m 市场看成一个行权价 = 窗口开盘 BTC 价的数字期权：

```
z          = ln(spot / strike) / (σ_1m × √剩余分钟数)
fair_up    = Φ(z)                    # 标准正态 CDF
fair_held  = fair_up (UP) / 1 - fair_up (DOWN)
edge       = fair_held - token_mid
adverse_z  = z (DOWN) / -z (UP)      # 正数 = 标的朝不利方向走
```

- `spot`：Binance 最新价
- `strike`：窗口开盘 BTC 价（GateKeeper baseline）
- `σ_1m`：1 分钟波动率估计
- `edge > 0`：模型认为该 token 比市场价便宜

### 3.2 1 分钟波动率 σ

取两个来源的较大值：

- 最近 60 秒 BTC 高低振幅 / 2
- 最近 20 根 1 分钟 bar 的对数收益率标准差

下限 `T4_STRIKE_SIGMA_PCT_PER_MIN = 0.05%/分钟`。

### 3.3 趋势 / 波动 regime

用最多 20 根 1 分钟 bar：

```
trend_raw     = (EMA5 - EMA15) / EMA15
normalizer    = max(σ, floor) × √5
trend_score   = clamp(trend_raw / normalizer, -1, 1)
side_sign     = +1 (UP) / -1 (DOWN)
trend_against = max(0, -(side_sign × trend_score))
```

`trend_against > 0` 表示趋势与持仓方向相反。

RSI、ATR 也会计算，但**只用于展示，目前不参与入场判断**。

### 3.4 盘口微观结构

| 指标 | 定义 |
|---|---|
| `spread` | `(best_ask - best_bid) / mid` |
| `book_imbalance` | `(bid_depth - ask_depth) / (bid_depth + ask_depth)`，top 5 档 |
| `cvd_ratio` | 最近 60 秒 `Σ signed_size / Σ |size|`；主动买为正、主动卖为负 |

### 3.5 5m 确认 confirm

代码：[../src/market/signal_market.py](../src/market/signal_market.py)

```
confirm = clamp(
    (side_mid - 0.5) × 2
  + clamp(side_change_60 × 20, -0.5, +0.5)
  - clamp(max(0, other_change_60) × 10, 0, +0.5),
  -1, +1
)
```

- `side_mid`：持仓方向 5m token 的 mid
- `side_change_60`：该 token 60 秒涨跌幅
- `other_change_60`：对手方 60 秒涨跌幅（只惩罚对手方上涨）
- 至少需要 2 个样本；数据缺失时 confirm 为 `None`

### 3.6 宏观

- `funding_rate`：Binance BTCUSDT 永续最新 8h funding
- `oi_change_pct`：当前 OI 相对约 5 分钟前的变化
- `dvol`：Deribit BTC DVOL
- `dvol_change_pct`：相对约 1 小时前的变化

### 3.7 事件黑名单

```
黑名单窗口 = [event.start - PRE_BUFFER_S, event.end + POST_BUFFER_S]
```

默认前后各 300 秒。文件缺失 = 没有事件。

### 3.8 最终 reasons

`SignalEngine.evaluate()` 会把命中的规则追加到 `reasons`：

```
strike_z, edge, trend_against, vol_regime,
spread_wide, book_ask_heavy, sell_flow,
5m_against, funding_extreme, oi_spike,
dvol_extreme, event_blackout:<name>
```

旧马丁模式下任意 reason 会拦截；当前 signal trader 只拦截其中一部分（见第 4 节）。

## 4. 当前 polygtd3 怎么用这些信号

代码：[../src/strategy/signal_trader.py](../src/strategy/signal_trader.py)

### 4.1 入场流程

1. **只在空仓且入场时间窗口内**  
   窗口开启后 60s 到结束前 180s。每轮最多 1 次入场。

2. **只评估价格在 0.75–0.90 的一侧**  
   UP/DOWN 分别看 mid；不在这个区间直接跳过。  
   这是为了避免 0.1–0.3 的高 spread 区和 0.9 以上的低赔率区。

3. **计算 `SignalEngine.evaluate(kind="T1", ...)`**  
   得到 snapshot 和 reasons。

4. **5m confirm 是正向驱动**  
   默认要求 `tf5m.confirm >= 0.20`。  
   如果 5m 数据不可用，**不入场**（`SIGNAL_TRADE_REQUIRE_M5=true`）。

5. **信号过滤**  
   如果 `SIGNAL_TRADE_USE_SIGNAL_GATES=true`：
   - 先看 `decision.reasons`，但忽略 `edge`、`strike_z`、`5m_against`
   - 再直接检查：  
     `spread <= 0.03`、`imbalance >= -0.60`、`cvd_ratio >= -0.60`、  
     `trend_against <= 0.70`、`sigma_1m_pct <= 0.25`
   - 数据为 `None` 时**跳过该条过滤**（fail-open）

6. **数字 edge 默认不参与判断**  
   `SIGNAL_TRADE_MIN_EDGE=-1` 表示忽略。  
   原因：6 个月校准显示模型 Brier 0.191，市场 0.175，edge 越高实际胜率反而越低。  
   如果把它设成 `0.10` 之类，它会变成一个过滤条件，但目前不建议。

7. **选边**  
   在通过条件的候选里选 5m confirm 最高的一侧。

8. **maker 入场**  
   挂在 best bid；5 秒未成交撤单重试，最多 3 次。  
   纸盘模型在成交价穿过后成交，手续费为 0（maker）。

9. **出场**
   - 止盈：+10%，maker
   - 止损：-15%，taker 按 best bid
   - 尾盘：窗口结束前 60 秒强制平仓，taker
   - 可选：5m confirm 翻负提前退出（默认关闭）

### 4.2 默认参数

| 参数 | 当前默认值 |
|---|---:|
| 价格带 `SIGNAL_TRADE_PRICE_MIN/MAX` | 0.75–0.90 |
| 5m 最小 confirm `SIGNAL_TRADE_M5_MIN_CONFIRM` | 0.20 |
| 是否必须 5m `SIGNAL_TRADE_REQUIRE_M5` | true |
| 单次仓位 `SIGNAL_TRADE_STAKE_USD` | 2 USD |
| 止盈 `SIGNAL_TRADE_TP_PCT` | +10% |
| 止损 `SIGNAL_TRADE_SL_PCT` | -15% |
| 尾盘强平 `SIGNAL_TRADE_TIME_EXIT_S` | 结束前 60s |
| 每轮入场 `SIGNAL_TRADE_MAX_ENTRIES` | 最多 1 次 |
| 入场方式 `SIGNAL_TRADE_ENTRY_MAKER` | maker，5s 超时，最多 3 次 |
| 出场 maker `SIGNAL_TRADE_EXIT_MAKER` | true |
| 数字 edge `SIGNAL_TRADE_MIN_EDGE` | -1（忽略） |
| 被忽略 reason `SIGNAL_TRADE_IGNORE_REASONS` | `edge,strike_z,5m_against` |
| 快照间隔 `SIGNAL_TRADE_SNAPSHOT_S` | 2s |

### 4.3 一个实际例子（2026-11-02 14:32 UTC）

日志：

```
side=UP mid=0.805 m5=1.0 edge=-0.0489 spread=None
```

对应判断：

- 价格 0.805，在 0.75–0.90 内 ✅
- 5m confirm = 1.0 ≥ 0.20 ✅
- edge = -0.0489，但默认忽略 ✅
- spread = None，该条过滤跳过 ✅
- 其他 reason 没有触发 ✅
- 选择 UP，maker 挂在 0.80
- 成交后 TP 0.88，约 53 秒后止盈，+0.20 USD

### 4.4 数据缺失时

- **5m 缺失**：不进场（因为它是正向驱动）
- **spread / imbalance / CVD 缺失**：跳过对应过滤，继续
- **BTC 行情超过 5s 没更新**：不进场
- **BTC 60 秒振幅超过 0.3%**：不进场
- **WindowManager 标记晚启动 / 跨窗跳空**：本轮不进场（`skip_reason=window_precheck`）

## 5. 旧马丁的用法（p1/p2 参考）

旧 `StrategyEngine` 把同一份 SignalEngine 当作**否决层**：

| 规则 | T1 阈值 | T2–T4 阈值 |
|---|---:|---:|
| `adverse_z` | 2.5 | 2.0 |
| `edge` | -0.05 | 0.02 |
| `trend_against` | 0.90 | 0.70 |
| `sigma_1m_pct` | 0.25 | 0.25 |
| `spread` | 0.03 | 0.03 |
| `book_imbalance` | -0.60 | -0.60 |
| `cvd_ratio` | -0.60 | -0.60 |
| `5m_against` | 默认关闭（`SIGNAL_5M_T1_ENABLED=false`） | 开启，阈值 -0.50 |
| funding / OI / DVOL / 事件 | 同上 | 同上 |

- `SIGNAL_GATES_ENABLED=true` + `LOG_ONLY=false`：enforce，任意 reason 拦截
- `LOG_ONLY=true`：只记录不拦截
- `ENABLED=false`：整层关闭
- 数据缺失：对应规则跳过（fail-open）
- T2–T4 被信号否决时直接 `return`，不会跳过去补更深一层

这就是为什么 p1（5m 门开）和 p2（5m 门关）在 11:15 那轮出现分歧：p1 的 T2 被 `5m_against` 拦下，p2 继续补了 T2/T3。

## 6. 怎么观察、怎么调

### 6.1 看板

- `/`：信号/风控表、当前持仓、本轮 PnL、挂单
- `/trades`：纸盘成交历史（mode=`paper`，tier=`SIG/TP/SL/TIME`）
- `/api/status`：`signal_trade` 字段包含当前规则和本轮统计

### 6.2 日志

```
signal_trader.entry_pending
signal_trader.entry_timeout
signal_trader.position_opened
signal_trader.position_closed
SIGNAL_STATUS ...   每 5 秒
```

### 6.3 快照

```
/app/data/signal_snapshots.jsonl
```

每 2 秒一行，包含 UP/DOWN 的 mid/bid/ask、fair/edge/z、趋势、波动、spread、imbalance、CVD、5m confirm、宏观、事件、当前仓位。用于跑一段时间后做前向分析。

### 6.4 调参

在 `/config` 的「信号交易」分组，或环境变量：

- 想更严格：提高 `SIGNAL_TRADE_M5_MIN_CONFIRM`、收窄价格带、降低 `SIGNAL_TRADE_MAX_SPREAD`、降低 `SIGNAL_TRADE_MAX_TREND_AGAINST`
- 想多交易：放宽价格带、降低 5m 阈值
- 只想收集信号不下单：`SIGNAL_TRADE_ENABLED=false`（快照仍会写）
- 想不用信号过滤、只看价格带和 5m：`SIGNAL_TRADE_USE_SIGNAL_GATES=false`
- 想启用数字 edge 过滤：设置 `SIGNAL_TRADE_MIN_EDGE`（目前不建议，回测显示反向）

### 6.5 几个容易混淆的开关

- **`SIGNAL_TRADE_USE_SIGNAL_GATES`** 才是当前 signal trader 的过滤总开关
- `SIGNAL_GATES_ENABLED` / `SIGNAL_GATES_LOG_ONLY` 是旧马丁层的模式，不会关闭 signal trader 自己的过滤
- **`SIGNAL_TRADE_REQUIRE_M5` / `SIGNAL_TRADE_M5_MIN_CONFIRM`** 控制当前 5m 驱动；`SIGNAL_5M_T1_ENABLED` 只影响旧马丁 T1 的 `5m_against` reason，不会关闭当前 signal trader 的 5m 要求
- `SIGNAL_MICRO_GATE_ENABLED=false` 只影响旧引擎是否生成 micro reason；当前 signal trader 的直接 spread/imbalance/CVD 过滤仍受 `SIGNAL_TRADE_USE_SIGNAL_GATES` 控制
- `SIGNAL_SIZE_SCALING_ENABLED` 只作用于旧马丁 T2–T4；当前 signal trader 固定 2 USD，不用 `size_multiplier`

## 7. 研究背景与风险

6 个月（2026-05 至 2026-10，17,635 个 BTC 15m 市场）的精细回测显示：

- 完整马丁 2/4/8/16：-25,173 USD
- T1-only：-6,226 USD
- 数字 edge 的 Brier 0.191，市场 Brier 0.175，且 edge 越高实际胜率越低
- 任意时点入场 + 10%/10% 止盈止损：edge 选边 -0.279/次，5m 确认选边 -0.141/次，买热门侧 -0.098/次（都是负的）

因此当前版本：

- 用 **5m confirm** 作为正向驱动
- 用 **热门侧价格带 0.75–0.90** 避开高 spread / 低赔率
- **忽略数字 edge**（它是反向的）
- 用 maker 入场 / 止盈，降低手续费
- 仍然只是**纸盘研究版本**，不是已经证明盈利的实盘策略

2026-11-02 当天前两笔纸盘交易都是 5m confirm 很高、价格在 0.75–0.90 时触发，maker 入场后 +10% 止盈，合计 +0.40 USD；两笔样本不足以判断策略有效性。

## 8. 相关代码

| 文件 | 内容 |
|---|---|
| [../src/strategy/signals.py](../src/strategy/signals.py) | SignalEngine：全部指标与 reasons |
| [../src/strategy/signal_trader.py](../src/strategy/signal_trader.py) | 当前信号纸盘：入场/出场/纸盘成交/快照 |
| [../src/signal_main.py](../src/signal_main.py) | signal 模式入口与装配 |
| [../src/market/signal_market.py](../src/market/signal_market.py) | 5m 市场发现、采样、confirm |
| [../src/market/market_ws.py](../src/market/market_ws.py) | PriceCache：mid/bid/ask/depth/CVD |
| [../src/feeds/binance_futures.py](../src/feeds/binance_futures.py) | funding / OI / DVOL |
| [../src/strategy/event_calendar.py](../src/strategy/event_calendar.py) | 事件黑名单 |
| [../src/config.py](../src/config.py) | 全部 `SIGNAL_TRADE_*` 默认值 |
| [../src/web/runtime_config.py](../src/web/runtime_config.py) | `/config` 信号交易分组 |
| [../.env.signal.example](../.env.signal.example) | 新实例环境变量示例 |
