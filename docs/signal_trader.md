# 信号驱动纸盘交易（2026-11-02）

`BOT_MODE=signal` 启动一套**完全独立于马丁**的纸盘策略：

- 不创建 `CLOBClient`、`OrderManager`、`Heartbeat`、`UserWS`；
- 不可能向 Polymarket 发送真实订单；
- 成交全部由本地行情模拟；
- 复用现有 `WindowManager` / `MarketWS` / `BinanceFeed` / `SignalEngine` / 5m 确认；
- 复用现有 SQLite（`rounds/orders/fills`）和 FastAPI 看板。

马丁代码仍保留在仓库里（`BOT_MODE=martingale` 或默认就是原行为），但
signal 模式不会执行它。

## 1. 启动

```bash
BOT_MODE=signal python -m src.main
# 或
python -m src.signal_main
```

新实例建议使用独立 Coolify 应用 / 独立数据卷：

```bash
BOT_MODE=signal
PAPER_TRADING=true
WEB_PORT=8020
```

不需要 API Key / 私钥。`.env.signal.example` 有完整示例。

## 2. 策略规则

入口在 `src/strategy/signal_trader.py`。默认规则：

- **入场时间**：窗口开启后 60s 到结束前 180s；
- **只买热门侧**：mid 在 `[SIGNAL_TRADE_PRICE_MIN, SIGNAL_TRADE_PRICE_MAX]`
  （默认 0.75–0.90）；
- **5m 确认是正向驱动**：`confirm >= SIGNAL_TRADE_M5_MIN_CONFIRM`
  （默认 0.20），数据缺失时不入场（可关闭该要求）；
- **其他信号作为过滤**：spread / 盘口失衡 / CVD / 趋势 / 波动 /
  funding / OI / DVOL / 事件黑名单；
- **数字 edge 默认不参与判断**：6 个月校准显示它比市场差且反向预测，
  `SIGNAL_TRADE_MIN_EDGE=-1` 表示忽略；`edge`/`strike_z`/`5m_against`
  三个 reason 默认被忽略；
- **maker 入场**：挂在 best bid；纸盘模型在成交价穿过后成交；
- **出场**：
  - 止盈 `+SIGNAL_TRADE_TP_PCT`（默认 10%，maker）；
  - 止损 `-SIGNAL_TRADE_SL_PCT`（默认 15%，taker at bid）；
  - 窗口结束前 `SIGNAL_TRADE_TIME_EXIT_S`（默认 60s）强制平仓；
  - 可选 5m 翻转退出（默认关闭）；
- **每轮最多一次入场**（`SIGNAL_TRADE_MAX_ENTRIES=1`，
  `SIGNAL_TRADE_ALLOW_REENTRY=false`）。

## 3. 代码结构

| 文件 | 作用 |
|---|---|
| `src/signal_main.py` | 独立入口：装配 feeds / WindowManager / SignalEngine / 纸盘 trader / Web |
| `src/strategy/signal_trader.py` | 入场/出场、纸盘成交、DB 写入、快照 JSONL |
| `src/execution/signal_router_stub.py` | 给 killswitch 用的 router 桩（只撤待成交入场） |
| `src/config.py` | `BOT_MODE` 和 `SIGNAL_TRADE_*` 参数 |
| `src/web/status.py` | `build_signal_status()`，把信号持仓映射到现有看板 |
| `src/web/runtime_config.py` | `/config` 新增「信号交易」分组 |
| `.env.signal.example` | 新实例环境变量示例 |

## 4. 看板与日志

- `/`：控制台（BTC 15m 价格、窗口、信号、当前信号持仓、挂单）；
- `/trades`：模拟成交与轮次（mode=`paper`，tier=`SIG/TP/SL/TIME`）；
- `/config`：在线修改信号交易参数（分组「信号交易」）；
- `/api/status`：`strategy="signal"`，含 `signal_trade` 规则摘要；
- `/api/health`：Coolify 健康检查。

日志：

- `signal_trader.new_round`
- `signal_trader.entry_pending` / `entry_skipped`
- `signal_trader.position_opened` / `position_closed`
- `SIGNAL_STATUS ...` 每 5 秒一行，包含 spot/base/drift/remain/UP/DOWN/
  entry 状态/仓位/roundPnL。

快照：

```
/app/data/signal_snapshots.jsonl
```

每 `SIGNAL_TRADE_SNAPSHOT_S`（默认 2s）一行，包含 UP/DOWN 的
mid/bid/ask、模型 fair/edge/z、趋势、波动、spread/imbalance/CVD、
5m confirm、宏观、事件、当前仓位/挂单。用于跑 2–4 周后的前向评估。
文件没有轮转，2s 约 20–30MB/天；长期运行请加日志轮转或调大间隔。

## 5. 安全边界

- 该进程没有 CLOB 客户端，**无法下真实订单**；
- 紧急停止只停止新入场并撤销 pending maker 入场；已有纸盘仓位仍按
  TP/SL/尾盘规则管理（与生产“停止开新仓、不强制平仓”一致）；
- `PAPER_TRADING=false` 会被忽略并记录警告；这是刻意的。

## 6. 研究背景

6 个月（2026-05 至 2026-10，17,635 个 BTC 15m 市场）的精细回测显示：

- 完整马丁 2/4/8/16：-25,173 USD；
- T1-only：-6,226 USD；
- 数字 edge 的 Brier 0.191，市场 Brier 0.175，且 edge 越高实际胜率越低；
- 任意时点入场 + 10%/10% 止盈止损：edge 选边 -0.279/次，5m 确认选边
  -0.141/次，买热门侧 -0.098/次（都是负的）。

因此本版本的默认选择是“热门侧 + 5m 确认 + maker + 小止盈 + 严格止损”，
并且把数字 edge 当作被忽略的参考值。它仍然是研究/纸盘版本，不是已经
证明盈利的实盘策略。
