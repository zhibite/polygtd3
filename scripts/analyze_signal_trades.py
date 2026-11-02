#!/usr/bin/env python3
"""Analyze signal snapshots + paper trades.

Inputs
------
1. ``signal_snapshots.jsonl`` (written by SignalTrader every 2 s):
   one JSON object per line with ``up`` / ``down`` mid/bid/ask and the full
   SignalEngine snapshot.
2. ``polymarket.db`` (rounds / orders / fills).

Outputs
-------
* Markdown report (actual trades + opportunity replay tables).
* CSV of actual trades.
* CSV of replayed opportunities.

The "opportunity replay" is the important part when there are only a few
real trades: for every 15m window it finds the first snapshot that would
have passed the same entry filters as the live SignalTrader, then simulates
the +TP / -SL / time-exit using the *subsequent* snapshots of that window.
It is an estimate: maker fills, no queue/depth/slippage, one opportunity
per round.  It is not a substitute for live fills.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _f(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _iso(ts_ms: int | float | None) -> str:
    if not ts_ms:
        return ""
    try:
        return datetime.fromtimestamp(
            float(ts_ms) / 1000.0, timezone.utc
        ).strftime("%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return ""


def _pct(value: float | None) -> str:
    return "--" if value is None else f"{value * 100:.1f}%"


def _num(value: float | None, digits: int = 3) -> str:
    return "--" if value is None else f"{value:.{digits}f}"


def _bucket(
    value: float | None,
    edges: list[float],
    labels: list[str],
) -> str:
    if value is None:
        return "n/a"
    for edge, label in zip(edges, labels):
        if value < edge:
            return label
    return labels[-1]


def taker_fee(shares: float, price: float, rate: float) -> float:
    return max(0.0, shares) * max(0.0, rate) * max(0.0, price) * max(
        0.0, 1.0 - price
    )


# ---------------------------------------------------------------------------
# Snapshot loading
# ---------------------------------------------------------------------------


def load_snapshots(path: str) -> tuple[dict[int, list[dict]], int, int]:
    rounds: dict[int, list[dict]] = defaultdict(list)
    rows = bad = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue
            rt = row.get("round_start_ts")
            if not rt:
                continue
            if not isinstance(row.get("up"), dict):
                continue
            if not isinstance(row.get("down"), dict):
                continue
            try:
                row["_ts"] = int(row.get("ts_ms") or 0)
                row["_round"] = int(rt)
            except (TypeError, ValueError):
                continue
            if row["_ts"] <= 0:
                continue
            rounds[row["_round"]].append(row)
            rows += 1
    for rt in rounds:
        rounds[rt].sort(key=lambda r: r["_ts"])
    return rounds, rows, bad


def side_info(row: dict, side: str) -> tuple[dict, dict]:
    d = row.get(side.lower()) or {}
    if not isinstance(d, dict):
        d = {}
    snap = d.get("snapshot") or {}
    if not isinstance(snap, dict):
        snap = {}
    return d, snap


def features(row: dict, side: str) -> dict[str, Any]:
    d, s = side_info(row, side)
    tf = s.get("tf5m") if isinstance(s.get("tf5m"), dict) else {}
    return {
        "mid": _f(d.get("mid")),
        "bid": _f(d.get("bid")),
        "ask": _f(d.get("ask")),
        "m5": _f(tf.get("confirm")),
        "edge": _f(s.get("edge")),
        "adverse_z": _f(s.get("adverse_z")),
        "fair_held": _f(s.get("fair_held")),
        "trend_against": _f(s.get("trend_against")),
        "sigma_pct": _f(s.get("sigma_1m_pct")),
        "spread": _f(s.get("spread")),
        "imbalance": _f(s.get("book_imbalance")),
        "cvd": _f(s.get("cvd_ratio")),
        "token_mid": _f(s.get("token_mid")),
        "reasons": list(s.get("reasons") or []),
        "spot": _f(s.get("spot")),
        "strike": _f(s.get("strike")),
        "remaining_s": _f(s.get("remaining_s")),
    }


# ---------------------------------------------------------------------------
# Entry filter (mirrors SignalTrader._try_entry)
# ---------------------------------------------------------------------------


def passes_filters(
    row: dict,
    side: str,
    args: argparse.Namespace,
) -> tuple[bool, str]:
    d, s = side_info(row, side)
    mid = _f(d.get("mid"))
    if mid is None or mid < args.price_min or mid > args.price_max:
        return False, "price"
    tf = s.get("tf5m") if isinstance(s.get("tf5m"), dict) else {}
    m5 = _f(tf.get("confirm"))
    if args.require_m5 and m5 is None:
        return False, "m5_missing"
    if m5 is not None and m5 < args.m5_min:
        return False, "m5_low"
    if args.use_gates:
        ignore = set(args.ignore_reasons)
        reasons = [r for r in (s.get("reasons") or []) if r not in ignore]
        if reasons:
            return False, "gate:" + ",".join(sorted(reasons))
        spread = _f(s.get("spread"))
        if spread is not None and spread > args.max_spread:
            return False, "spread"
        imbalance = _f(s.get("book_imbalance"))
        if imbalance is not None and imbalance < args.min_imbalance:
            return False, "imbalance"
        cvd = _f(s.get("cvd_ratio"))
        if cvd is not None and cvd < -args.max_sell_flow:
            return False, "cvd"
        trend = _f(s.get("trend_against"))
        if trend is not None and trend > args.max_trend:
            return False, "trend"
        sigma = _f(s.get("sigma_1m_pct"))
        if sigma is not None and sigma > args.max_sigma:
            return False, "sigma"
    edge = _f(s.get("edge"))
    if args.min_edge > -0.99 and (edge is None or edge < args.min_edge):
        return False, "edge"
    return True, ""


def entry_window_ok(row: dict, args: argparse.Namespace) -> bool:
    if "entry_window_open" in row:
        return bool(row.get("entry_window_open"))
    remaining = _f(row.get("remaining_s"))
    if remaining is None:
        return True
    elapsed = 900.0 - remaining
    return args.entry_start_s <= elapsed <= 900.0 - args.entry_end_s


# ---------------------------------------------------------------------------
# Opportunity replay
# ---------------------------------------------------------------------------


def simulate_exit(
    rows: list[dict],
    start_idx: int,
    side: str,
    entry_px: float,
    round_start_ts: int,
    args: argparse.Namespace,
) -> tuple[str, float, int, str]:
    tp = min(0.99, entry_px * (1.0 + args.tp))
    sl = max(0.01, entry_px * (1.0 - args.sl))
    end_ms = round_start_ts * 1000 + 900_000
    time_line = end_ms - int(args.time_exit * 1000)
    last_px = entry_px
    last_ts = rows[start_idx]["_ts"]
    for row in rows[start_idx + 1:]:
        d, _ = side_info(row, side)
        bid = _f(d.get("bid"))
        mid = _f(d.get("mid"))
        px = bid if bid and bid > 0 else mid
        if px is None or px <= 0:
            continue
        last_px, last_ts = px, row["_ts"]
        if px >= tp:
            return "TP", tp, row["_ts"], "MAKER"
        if px <= sl:
            return "SL", px, row["_ts"], "TAKER"
        if row["_ts"] >= time_line:
            return "TIME", px, row["_ts"], "TAKER"
    return "NO_EXIT", last_px, last_ts, "TAKER"


def pnl_usd(
    entry_px: float,
    exit_px: float,
    role: str,
    stake: float,
    fee_rate: float,
) -> float:
    if entry_px <= 0:
        return 0.0
    shares = stake / entry_px
    entry_fee = 0.0  # maker entry
    exit_fee = (
        taker_fee(shares, exit_px, fee_rate) if role == "TAKER" else 0.0
    )
    return (exit_px - entry_px) * shares - entry_fee - exit_fee


def find_opportunities(
    rounds: dict[int, list[dict]],
    args: argparse.Namespace,
    *,
    first_only: bool = True,
) -> list[dict]:
    out: list[dict] = []
    for rt, rows in sorted(rounds.items()):
        if not rows:
            continue
        for idx, row in enumerate(rows):
            if not entry_window_ok(row, args):
                continue
            candidates: list[tuple[str, float]] = []
            for side in ("UP", "DOWN"):
                ok, _why = passes_filters(row, side, args)
                if not ok:
                    continue
                d, s = side_info(row, side)
                mid = _f(d.get("mid"))
                bid = _f(d.get("bid"))
                if mid is None or mid <= 0:
                    continue
                entry_px = bid if bid and bid > 0 else mid
                if entry_px <= 0 or entry_px >= 0.999:
                    continue
                tf = s.get("tf5m") if isinstance(s.get("tf5m"), dict) else {}
                m5 = _f(tf.get("confirm"), -99.0)
                candidates.append((side, m5 or -99.0))
            if not candidates:
                continue
            # One opportunity per window: highest 5m confirm, like live.
            candidates.sort(key=lambda x: x[1], reverse=True)
            side = candidates[0][0]
            d, s = side_info(row, side)
            bid = _f(d.get("bid"))
            mid = _f(d.get("mid"))
            entry_px = bid if bid and bid > 0 else mid
            if entry_px is None or entry_px <= 0:
                continue
            exit_reason, exit_px, exit_ts, role = simulate_exit(
                rows, idx, side, entry_px, rt, args
            )
            feat = features(row, side)
            hold_s = max(0.0, (exit_ts - row["_ts"]) / 1000.0)
            pnl = pnl_usd(entry_px, exit_px, role, args.stake, args.fee_rate)
            out.append({
                "round_start_ts": rt,
                "entry_ts": row["_ts"],
                "entry_time": _iso(row["_ts"]),
                "exit_ts": exit_ts,
                "exit_time": _iso(exit_ts),
                "side": side,
                "entry_px": entry_px,
                "exit_px": exit_px,
                "exit_reason": exit_reason,
                "role": role,
                "pnl_usd": pnl,
                "hold_s": hold_s,
                "elapsed_s": 900.0 - (feat.get("remaining_s") or 0.0),
                **feat,
            })
            if first_only:
                break  # one opportunity per round
    return out


# ---------------------------------------------------------------------------
# Actual trades from SQLite
# ---------------------------------------------------------------------------


def load_actual_trades(
    db_path: str,
    rounds: dict[int, list[dict]],
    args: argparse.Namespace,
) -> list[dict]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        round_tokens: dict[int, tuple[str, str]] = {}
        for rt, up, down in conn.execute(
            "select round_start_ts, up_token, down_token from rounds"
        ):
            round_tokens[int(rt)] = (str(up or ""), str(down or ""))
        tier_by_order = {
            str(cid): str(tier)
            for cid, tier in conn.execute(
                "select client_order_id, tier_name from orders"
            )
        }
        by_round: dict[int, list[tuple]] = defaultdict(list)
        for row in conn.execute(
            "select round_start_ts, side, price, size, fee, ts_ms, "
            "token_id, order_id from fills order by ts_ms"
        ):
            try:
                rt = int(row[0])
            except (TypeError, ValueError):
                continue
            by_round[rt].append(row)
    finally:
        conn.close()

    out: list[dict] = []
    for rt, fills in sorted(by_round.items()):
        buys = [r for r in fills if str(r[1]).upper() == "BUY"]
        sells = [r for r in fills if str(r[1]).upper() == "SELL"]
        if not buys:
            continue
        buy_qty = sum(_f(r[3], 0.0) or 0.0 for r in buys)
        if buy_qty <= 0:
            continue
        entry_px = sum(
            (_f(r[2], 0.0) or 0.0) * (_f(r[3], 0.0) or 0.0)
            for r in buys
        ) / buy_qty
        buy_fee = sum(_f(r[4], 0.0) or 0.0 for r in buys)
        entry_ts = min(int(r[5]) for r in buys)
        token = str(buys[0][6] or "")
        up_tok, down_tok = round_tokens.get(rt, ("", ""))
        side = "UP" if token and token == up_tok else (
            "DOWN" if token and token == down_tok else ""
        )
        sell_qty = sum(_f(r[3], 0.0) or 0.0 for r in sells)
        if sells:
            exit_px = sum(
                (_f(r[2], 0.0) or 0.0) * (_f(r[3], 0.0) or 0.0)
                for r in sells
            ) / sell_qty if sell_qty > 0 else 0.0
            sell_fee = sum(_f(r[4], 0.0) or 0.0 for r in sells)
            exit_ts = max(int(r[5]) for r in sells)
            exit_reason = tier_by_order.get(
                str(sells[-1][7] or ""), "SELL"
            )
            pnl = (
                exit_px * sell_qty - sell_fee
                - (entry_px * buy_qty + buy_fee)
            )
        else:
            exit_px = entry_px
            exit_ts = entry_ts
            exit_reason = "OPEN"
            pnl = 0.0
        flag = ""
        if exit_reason == "SL" and exit_px > entry_px + 1e-9:
            # A stop below the entry cannot legitimately fill above the
            # entry; this is the 2026-11-02 stale-last-trade false stop.
            flag = "SL>entry"
        elif exit_reason == "TP" and exit_px < entry_px - 1e-9:
            flag = "TP<entry"
        elif exit_reason == "OPEN":
            flag = "open"
        feat: dict[str, Any] = {}
        if side:
            rows = rounds.get(rt) or []
            latest = None
            for row in rows:
                if row["_ts"] <= entry_ts:
                    latest = row
                else:
                    break
            if latest is not None:
                feat = features(latest, side)
        out.append({
            "round_start_ts": rt,
            "entry_ts": entry_ts,
            "entry_time": _iso(entry_ts),
            "exit_ts": exit_ts,
            "exit_time": _iso(exit_ts),
            "side": side,
            "entry_px": entry_px,
            "exit_px": exit_px,
            "exit_reason": exit_reason,
            "role": "",
            "flag": flag,
            "pnl_usd": pnl,
            "hold_s": max(0.0, (exit_ts - entry_ts) / 1000.0),
            "elapsed_s": 900.0 - (feat.get("remaining_s") or 0.0),
            **feat,
        })
    return out


# ---------------------------------------------------------------------------
# Aggregation / Markdown
# ---------------------------------------------------------------------------


def _metric(records: list[dict]) -> dict[str, Any]:
    n = len(records)
    if n == 0:
        return {
            "n": 0, "wins": 0, "losses": 0, "win_rate": None,
            "pnl": 0.0, "avg": None, "avg_win": None, "avg_loss": None,
            "pf": None, "tp": 0, "sl": 0, "time": 0, "no_exit": 0,
            "avg_hold": None, "avg_entry": None, "avg_m5": None,
            "avg_spread": None,
        }
    pnls = [r["pnl_usd"] for r in records]
    wins = [p for p in pnls if p > 1e-9]
    losses = [p for p in pnls if p < -1e-9]
    reasons = [str(r.get("exit_reason") or "") for r in records]
    holds = [r["hold_s"] for r in records if r.get("hold_s") is not None]
    entries = [r["entry_px"] for r in records if r.get("entry_px")]
    m5s = [r["m5"] for r in records if r.get("m5") is not None]
    spreads = [r["spread"] for r in records if r.get("spread") is not None]
    return {
        "n": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / n,
        "pnl": sum(pnls),
        "avg": sum(pnls) / n,
        "avg_win": sum(wins) / len(wins) if wins else None,
        "avg_loss": sum(losses) / len(losses) if losses else None,
        "pf": (
            sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0
            else None
        ),
        "tp": reasons.count("TP"),
        "sl": reasons.count("SL"),
        "time": reasons.count("TIME"),
        "no_exit": reasons.count("NO_EXIT"),
        "avg_hold": sum(holds) / len(holds) if holds else None,
        "avg_entry": sum(entries) / len(entries) if entries else None,
        "avg_m5": sum(m5s) / len(m5s) if m5s else None,
        "avg_spread": sum(spreads) / len(spreads) if spreads else None,
    }


def aggregate(
    records: list[dict],
    key_fn: Callable[[dict], str],
) -> list[tuple[str, dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        groups[key_fn(r)].append(r)
    return sorted(
        ((name, _metric(group)) for name, group in groups.items()),
        key=lambda kv: kv[0],
    )


def md_metrics_table(
    title: str,
    groups: list[tuple[str, dict]],
) -> str:
    lines = [
        f"### {title}",
        "",
        "| 分组 | 笔数 | 胜率 | 总 PnL | 单次均值 | 平均盈利 | 平均亏损 | PF | TP/SL/TIME | 平均持仓 | 平均入场价 | 平均 5m | 平均 spread |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    for name, m in groups:
        lines.append(
            f"| {name} | {m['n']} | {_pct(m['win_rate'])} | "
            f"{m['pnl']:+.3f} | {_num(m['avg'])} | "
            f"{_num(m['avg_win'])} | {_num(m['avg_loss'])} | "
            f"{_num(m['pf'], 2)} | "
            f"{m['tp']}/{m['sl']}/{m['time']} | "
            f"{_num(m['avg_hold'], 1)}s | {_num(m['avg_entry'], 4)} | "
            f"{_num(m['avg_m5'], 2)} | {_num(m['avg_spread'], 4)} |"
        )
    lines.append("")
    return "\n".join(lines)


def md_detail_table(records: list[dict]) -> str:
    lines = [
        "| 时间 (UTC) | 轮次 | 方向 | 入场 | 出场 | 原因 | 标记 | PnL | 持仓 | m5 | edge | spread | trend |",
        "|---|---:|---|---:|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in sorted(records, key=lambda x: x["entry_ts"]):
        lines.append(
            f"| {r['entry_time']} | {r['round_start_ts']} | {r['side'] or '--'} | "
            f"{_num(r['entry_px'], 4)} | {_num(r['exit_px'], 4)} | "
            f"{r['exit_reason']} | {r.get('flag') or ''} | "
            f"{r['pnl_usd']:+.3f} | "
            f"{_num(r['hold_s'], 0)}s | {_num(r.get('m5'), 2)} | "
            f"{_num(r.get('edge'), 3)} | {_num(r.get('spread'), 4)} | "
            f"{_num(r.get('trend_against'), 2)} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_csv(path: str, records: list[dict]) -> None:
    if not records:
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write("")
        return
    keys = [
        "round_start_ts", "entry_time", "exit_time", "side",
        "entry_px", "exit_px", "exit_reason", "role", "flag", "pnl_usd",
        "hold_s", "elapsed_s", "mid", "bid", "ask", "m5", "edge",
        "adverse_z", "fair_held", "trend_against", "sigma_pct",
        "spread", "imbalance", "cvd", "reasons",
    ]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for r in sorted(records, key=lambda x: x["entry_ts"]):
            row = dict(r)
            row["reasons"] = "|".join(r.get("reasons") or [])
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", default="/app/data/signal_snapshots.jsonl")
    ap.add_argument("--db", default="/app/data/polymarket.db")
    ap.add_argument("--out", default="/app/data/signal_analysis.md")
    ap.add_argument("--trades-csv", default="/app/data/signal_trades.csv")
    ap.add_argument("--opps-csv", default="/app/data/signal_opportunities.csv")
    ap.add_argument(
        "--candidates-csv", default="/app/data/signal_candidates.csv"
    )
    ap.add_argument("--price-min", type=float, default=0.75)
    ap.add_argument("--price-max", type=float, default=0.90)
    ap.add_argument("--m5-min", type=float, default=0.20)
    ap.add_argument("--require-m5", action="store_true", default=True)
    ap.add_argument("--no-require-m5", dest="require_m5", action="store_false")
    ap.add_argument("--tp", type=float, default=0.10)
    ap.add_argument("--sl", type=float, default=0.15)
    ap.add_argument("--time-exit", type=float, default=60.0)
    ap.add_argument("--entry-start", type=float, default=60.0)
    ap.add_argument("--entry-end", type=float, default=180.0)
    ap.add_argument("--stake", type=float, default=2.0)
    ap.add_argument("--fee-rate", type=float, default=0.07)
    ap.add_argument("--use-gates", action="store_true", default=True)
    ap.add_argument("--no-gates", dest="use_gates", action="store_false")
    ap.add_argument(
        "--ignore-reasons",
        default="edge,strike_z,5m_against",
    )
    ap.add_argument("--max-spread", type=float, default=0.03)
    ap.add_argument("--min-imbalance", type=float, default=-0.60)
    ap.add_argument("--max-sell-flow", type=float, default=0.60)
    ap.add_argument("--max-trend", type=float, default=0.70)
    ap.add_argument("--max-sigma", type=float, default=0.25)
    ap.add_argument("--min-edge", type=float, default=-1.0)
    args = ap.parse_args()
    args.ignore_reasons = [
        x.strip() for x in str(args.ignore_reasons).split(",") if x.strip()
    ]

    if not os.path.exists(args.jsonl):
        raise SystemExit(f"snapshot file not found: {args.jsonl}")
    rounds, snap_rows, bad = load_snapshots(args.jsonl)
    actual = load_actual_trades(args.db, rounds, args) if os.path.exists(args.db) else []
    opps_first = find_opportunities(rounds, args, first_only=True)
    opps_all = find_opportunities(rounds, args, first_only=False)

    write_csv(args.trades_csv, actual)
    write_csv(args.opps_csv, opps_first)
    write_csv(args.candidates_csv, opps_all)

    m5_edges = [0.0, 0.2, 0.4, 0.6, 0.8]
    m5_labels = ["<0", "0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", ">=0.8"]
    price_edges = [0.80, 0.85]
    price_labels = ["0.75-0.80", "0.80-0.85", "0.85-0.90"]
    spread_edges = [0.01, 0.02, 0.03]
    spread_labels = ["<0.01", "0.01-0.02", "0.02-0.03", ">=0.03"]
    trend_edges = [0.2, 0.5, 0.7]
    trend_labels = ["0-0.2", "0.2-0.5", "0.5-0.7", ">=0.7"]
    edge_edges = [-0.1, 0.0, 0.1]
    edge_labels = ["<-0.1", "-0.1-0", "0-0.1", ">=0.1"]

    def key_m5(r: dict) -> str:
        return _bucket(r.get("m5"), m5_edges, m5_labels)

    def key_price(r: dict) -> str:
        return _bucket(r.get("entry_px"), price_edges, price_labels)

    def key_spread(r: dict) -> str:
        return _bucket(r.get("spread"), spread_edges, spread_labels)

    def key_trend(r: dict) -> str:
        return _bucket(r.get("trend_against"), trend_edges, trend_labels)

    def key_edge(r: dict) -> str:
        return _bucket(r.get("edge"), edge_edges, edge_labels)

    def key_side(r: dict) -> str:
        return r.get("side") or "n/a"

    def key_time(r: dict) -> str:
        elapsed = r.get("elapsed_s")
        if elapsed is None:
            return "n/a"
        return "early<300s" if elapsed < 300 else (
            "mid300-600s" if elapsed < 600 else "late>600s"
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "# 信号快照 + 成交分析",
        "",
        f"- 生成时间：{now}",
        f"- 快照文件：`{args.jsonl}`（有效快照行 {snap_rows}，坏行 {bad}）",
        f"- 数据库：`{args.db}`",
        f"- 轮次数：{len(rounds)}；实际成交轮次：{len(actual)}；"
        f"每轮首个回放：{len(opps_first)}；全部候选快照：{len(opps_all)}",
        f"- 参数：价格 {args.price_min}-{args.price_max}，5m≥{args.m5_min}，"
        f"TP {args.tp:.0%}，SL {args.sl:.0%}，尾盘 {args.time_exit:.0f}s，"
        f"仓位 {args.stake:.2f} USD",
        "",
        "> 回放规则：按 best bid maker 入场，TP 用 bid≥目标价，"
        "SL/尾盘用 bid 立即成交；不含排队/深度/滑点，maker 成交偏乐观。"
        "每轮首个回放接近实盘每轮一次的限制；全部候选快照表允许同一轮"
        "多次重叠，仅用于扩大样本和信号质量分析，不是可执行交易序列。",
        "",
        "## 1. 实际纸盘成交",
        "",
    ]
    if actual:
        lines.append(md_detail_table(actual))
        m = _metric(actual)
        lines += [
            f"实际成交合计：{m['n']} 笔，胜率 {_pct(m['win_rate'])}，"
            f"总 PnL {m['pnl']:+.3f} USD，单次均值 {_num(m['avg'])} USD。",
            "",
        ]
    else:
        lines += ["暂无实际纸盘成交。", ""]

    lines += [
        "## 2. 机会回放（每轮首个，接近实盘）",
        "",
    ]
    if opps_first:
        lines.append(md_detail_table(opps_first))
        m = _metric(opps_first)
        lines += [
            f"每轮首个回放合计：{m['n']} 笔，胜率 {_pct(m['win_rate'])}，"
            f"总 PnL {m['pnl']:+.3f} USD，单次均值 {_num(m['avg'])} USD，"
            f"TP/SL/TIME = {m['tp']}/{m['sl']}/{m['time']}。",
            "",
        ]
        lines += [
            md_metrics_table("按 5m confirm 分桶", aggregate(opps_first, key_m5)),
            md_metrics_table("按入场价格分桶", aggregate(opps_first, key_price)),
            md_metrics_table("按 spread 分桶", aggregate(opps_first, key_spread)),
            md_metrics_table("按 trend_against 分桶", aggregate(opps_first, key_trend)),
            md_metrics_table("按 edge 分桶", aggregate(opps_first, key_edge)),
            md_metrics_table("按方向分桶", aggregate(opps_first, key_side)),
            md_metrics_table("按入场时间分桶", aggregate(opps_first, key_time)),
        ]
    else:
        lines += ["当前快照里没有满足条件的回放机会。", ""]

    lines += [
        "## 3. 候选快照（全部，重叠样本）",
        "",
    ]
    if opps_all:
        m = _metric(opps_all)
        lines += [
            f"全部候选快照合计：{m['n']} 笔，胜率 {_pct(m['win_rate'])}，"
            f"总 PnL {m['pnl']:+.3f} USD，单次均值 {_num(m['avg'])} USD，"
            f"TP/SL/TIME = {m['tp']}/{m['sl']}/{m['time']}。",
            "",
        ]
        lines += [
            md_metrics_table("按 5m confirm 分桶", aggregate(opps_all, key_m5)),
            md_metrics_table("按入场价格分桶", aggregate(opps_all, key_price)),
            md_metrics_table("按 spread 分桶", aggregate(opps_all, key_spread)),
            md_metrics_table("按 trend_against 分桶", aggregate(opps_all, key_trend)),
            md_metrics_table("按 edge 分桶", aggregate(opps_all, key_edge)),
            md_metrics_table("按方向分桶", aggregate(opps_all, key_side)),
            md_metrics_table("按入场时间分桶", aggregate(opps_all, key_time)),
        ]
    else:
        lines += ["当前快照里没有满足条件的候选快照。", ""]

    lines += [
        "## 4. 说明",
        "",
        "- 实际成交来自 `fills` 表，按轮次聚合买单/卖单；PnL 含记录的手续费。",
        "- `flag=SL>entry` 表示止损出场价高于入场价，通常是过期 last_trade "
        "造成的误触发；信号纸盘的 SL/TP 判断已改为只使用 best bid/mid。",
        "- 每轮首个回放接近实盘“每轮最多一次入场”的限制，适合看实际策略近似结果。",
        "- 全部候选快照表对每个满足条件的 2 秒快照都回放，同一轮可能重叠多次；"
        "它不是可执行交易序列，只用于扩大样本、观察信号分桶。",
        "- 回放的价格来自快照的 best bid/mid；真实 maker 成交可能更差。",
        "- 样本量小的时候，分桶结果只能看方向，不能当作参数优化的唯一依据。",
        "- 当前 signal 模式的数字 edge 默认忽略，因此 edge 分桶仅作参考。",
        "",
    ]
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"report: {args.out}")
    print(f"trades csv: {args.trades_csv}")
    print(f"opportunities csv: {args.opps_csv}")
    print(f"candidates csv: {args.candidates_csv}")
    print(
        f"snapshots={snap_rows} rounds={len(rounds)} "
        f"actual={len(actual)} first={len(opps_first)} "
        f"all_candidates={len(opps_all)}"
    )


if __name__ == "__main__":
    main()
