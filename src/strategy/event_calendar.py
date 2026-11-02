"""Manual event blackout calendar (2026-11-02).

The martingale should not open or average into a known macro event
(CPI, FOMC, NFP, ETF/regulatory decisions, token unlocks, …). The file is
operator-maintained so there is no external API dependency and no
surprise outage in the hot path:

    data/event_calendar.json
    {
      "events": [
        {"name": "FOMC",
         "start": "2026-11-05T18:00:00Z",
         "end":   "2026-11-05T19:00:00Z"}
      ]
    }

`start_ts` / `end_ts` (Unix seconds) are also accepted. The file is
re-read when its mtime changes. A missing/invalid file means "no events".
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import strategy as _strategy
from src.infra.logging import get_logger

log = get_logger(__name__)

_DATA_DIR = Path(
    os.getenv("POLYMARKET_DATA_DIR")
    or (Path(__file__).resolve().parents[2] / "data")
)


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


@dataclass
class EventCalendar:
    path: Path = field(default_factory=lambda: _DATA_DIR / "event_calendar.json")
    pre_buffer_s: float = 300.0
    post_buffer_s: float = 300.0
    _events: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _mtime: float = 0.0
    _last_check: float = 0.0

    def __post_init__(self) -> None:
        self.refresh(force=True)

    def refresh(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_check < 15.0:
            return
        self._last_check = now
        try:
            if not self.path.exists():
                self._events = []
                self._mtime = 0.0
                return
            mtime = self.path.stat().st_mtime
            if not force and mtime == self._mtime:
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            entries = raw.get("events", []) if isinstance(raw, dict) else []
            events: list[dict[str, Any]] = []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                start = _parse_ts(entry.get("start", entry.get("start_ts")))
                end = _parse_ts(entry.get("end", entry.get("end_ts")))
                if start is None or end is None or end <= start:
                    continue
                events.append({
                    "name": str(entry.get("name") or "event"),
                    "start": float(start),
                    "end": float(end),
                })
            self._events = events
            self._mtime = mtime
            log.info(
                "event_calendar.loaded",
                path=str(self.path),
                events=len(events),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "event_calendar.load_failed",
                path=str(self.path),
                error=str(exc),
            )

    def active(self, now: float | None = None) -> dict[str, Any] | None:
        """Return the active event, including pre/post blackout buffers."""
        self.refresh()
        ts = time.time() if now is None else float(now)
        for event in self._events:
            if (
                event["start"] - self.pre_buffer_s
                <= ts
                <= event["end"] + self.post_buffer_s
            ):
                return event
        return None

    def snapshot(self, now: float | None = None) -> dict[str, Any]:
        event = self.active(now)
        return {
            "event_blackout": event is not None,
            "event_name": event["name"] if event else None,
            "event_end": event["end"] if event else None,
        }


def from_config() -> EventCalendar:
    cfg = _strategy()
    raw = str(getattr(cfg, "SIGNAL_EVENT_CALENDAR_PATH", "") or "").strip()
    path = Path(raw) if raw else (_DATA_DIR / "event_calendar.json")
    return EventCalendar(
        path=path,
        pre_buffer_s=float(
            getattr(cfg, "SIGNAL_EVENT_PRE_BUFFER_S", 300.0) or 0.0
        ),
        post_buffer_s=float(
            getattr(cfg, "SIGNAL_EVENT_POST_BUFFER_S", 300.0) or 0.0
        ),
    )
