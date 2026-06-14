import time, threading, json
from pathlib import Path
from dataclasses import dataclass
from typing import Callable


def _cron_field(field: str, lo: int, hi: int) -> list[int]:
    if field == "*":
        return list(range(lo, hi + 1))
    if field.startswith("*/"):
        return list(range(lo, hi + 1, int(field[2:])))
    if "-" in field:
        a, b = field.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in field:
        return [int(x) for x in field.split(",")]
    return [int(field)]


class CronExpr:
    """5-bit cron expression (minute hour day month day-of-week)."""

    def __init__(self, expr: str):
        f = expr.strip().split()
        if len(f) != 5:
            raise ValueError(f"cron needs 5 fields: {expr}")
        self.minutes = _cron_field(f[0], 0, 59)
        self.hours = _cron_field(f[1], 0, 23)
        self.days = _cron_field(f[2], 1, 31)
        self.months = _cron_field(f[3], 1, 12)
        # cron DOW: 0=Sun,1=Mon..6=Sat,7=Sun. Python tm_wday: 0=Mon..6=Sun
        raw = _cron_field(f[4], 0, 7)
        self.dow = [(x + 6) % 7 for x in raw]  # 0(Sun)→6, 1(Mon)→0, ..., 6(Sat)→5

    def next_after(self, after: float) -> float:
        t = time.localtime(after)
        y, mo, d, h, mi = t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min
        for _ in range(525600):
            mi += 1
            if mi >= 60:
                mi = -1; h += 1
            if h >= 24:
                h = -1; d += 1
            if d > 31:
                d = 1; mo += 1
            if mo > 12:
                mo = 1; y += 1
            if mo not in self.months:
                mi = h = -1; d = 1; mo += 1; continue
            if d not in self.days:
                mi = h = -1; d += 1; continue
            if h not in self.hours:
                mi = -1; h += 1; continue
            if mi not in self.minutes:
                continue
            try:
                tm = time.mktime((y, mo, d, h, mi, 0, 0, 0, -1))
                if time.localtime(tm).tm_wday in self.dow:
                    return tm
            except (ValueError, OverflowError):
                pass
        return float('inf')


@dataclass
class TaskDef:
    name: str
    schedule: str  # cron expr or "interval:N"
    callback: Callable
    last_run: float = 0.0

    @property
    def is_interval(self) -> bool:
        return self.schedule.startswith("interval:")

    def due(self, now: float) -> bool:
        if self.is_interval:
            secs = int(self.schedule.split(":", 1)[1])
            return now - self.last_run >= secs
        return CronExpr(self.schedule).next_after(self.last_run) <= now if self.last_run > 0 else True


class Scheduler:
    """Interval + cron scheduler with persistence."""

    def __init__(self, persist_path: str | None = None, auto_reload: bool = True):
        self._tasks: list[TaskDef] = []
        self._running = False
        self._lock = threading.Lock()
        self._persist_path = Path(persist_path) if persist_path else None
        self._auto_reload = auto_reload
        self._last_mtime = 0.0
        self._programmatic_tasks: set[str] = set()
        self._load()

    # --- public API ---

    def add_task(self, name: str, schedule: str, fn: Callable) -> str:
        """Register a named task. schedule is a cron expr or 'interval:N'."""
        with self._lock:
            self._programmatic_tasks.add(name)
            for t in self._tasks:
                if t.name == name:
                    t.schedule = schedule
                    t.callback = fn
                    self._save()
                    return name
            self._tasks.append(TaskDef(name, schedule, fn))
            self._save()
        return name

    def remove_task(self, name: str) -> bool:
        with self._lock:
            before = len(self._tasks)
            self._tasks[:] = [t for t in self._tasks if t.name != name]
            if len(self._tasks) != before:
                self._save()
                return True
        return False

    def list_tasks(self) -> list[dict]:
        with self._lock:
            return [{"name": t.name, "schedule": t.schedule,
                     "last_run": t.last_run} for t in self._tasks]

    def every(self, interval_seconds: int):
        """Backward-compat decorator — names are auto-generated."""
        def deco(fn):
            name = fn.__name__ or f"anon_{len(self._tasks)}"
            self.add_task(name, f"interval:{interval_seconds}", fn)
            return fn
        return deco

    def start(self):
        self._running = True
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def stop(self):
        self._running = False

    # --- internals ---

    def _loop(self):
        while self._running:
            now = time.time()
            self._reload_if_changed()
            with self._lock:
                for task in self._tasks:
                    if task.due(now):
                        task.last_run = now
                        self._save()
                        try:
                            task.callback()
                        except Exception:
                            pass
            time.sleep(1)

    def _save(self):
        if not self._persist_path:
            return
        data = [{"name": t.name, "schedule": t.schedule, "last_run": t.last_run}
                for t in self._tasks]
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        self._persist_path.write_text(json.dumps(data, indent=2))
        self._last_mtime = self._persist_path.stat().st_mtime

    def _reload_if_changed(self):
        if not self._persist_path or not self._persist_path.exists() or not self._auto_reload:
            return
        try:
            mtime = self._persist_path.stat().st_mtime
        except OSError:
            return
        if mtime <= self._last_mtime:
            return
        try:
            data = json.loads(self._persist_path.read_text())
        except (json.JSONDecodeError, OSError):
            return
        existing: dict[str, TaskDef] = {t.name: t for t in self._tasks}
        # Add new tasks from file, preserve existing
        for entry in data:
            if entry["name"] not in existing:
                self._tasks.append(TaskDef(
                    name=entry["name"],
                    schedule=entry["schedule"],
                    callback=lambda: None,
                    last_run=entry.get("last_run", 0),
                ))
            else:
                # Update schedule if changed
                existing[entry["name"]].schedule = entry["schedule"]
        # Remove tasks no longer in file, keep programmatic ones
        file_names = {e["name"] for e in data}
        self._tasks[:] = [t for t in self._tasks
                          if t.name in file_names or t.name in self._programmatic_tasks]
        self._last_mtime = mtime

    def _load(self):
        if not self._persist_path or not self._persist_path.exists():
            return
        try:
            data = json.loads(self._persist_path.read_text())
        except (json.JSONDecodeError, OSError):
            return
        known = {t.name: t for t in self._tasks}
        for entry in data:
            if entry["name"] not in known:
                self._tasks.append(TaskDef(
                    name=entry["name"],
                    schedule=entry["schedule"],
                    callback=lambda: None,
                    last_run=entry.get("last_run", 0),
                ))
