import json, os, re, time, threading
from pathlib import Path


class StateStore:

    @staticmethod
    def _sanitize(name: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", name)
    """Three-zone storage: working (current snapshot), episodic (events),
    baseline (trends), alerts (history)."""

    def __init__(self, data_dir: str):
        self.root = Path(data_dir)
        self._baseline_locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()
        for d in ("working", "episodic", "baseline", "alerts"):
            (self.root / d).mkdir(parents=True, exist_ok=True)

    # -- working zone: single latest.json --

    def update_working(self, data: dict):
        (self.root / "working" / "latest.json").write_text(
            json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    def update_working_field(self, key: str, value):
        p = self.root / "working" / "latest.json"
        data = json.loads(p.read_text()) if p.exists() else {}
        data[key] = value
        p.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def load_working(self) -> dict:
        p = self.root / "working" / "latest.json"
        return json.loads(p.read_text()) if p.exists() else {}

    # -- episodic zone: per-day JSONL files --

    def append_episodic(self, event: dict):
        if "ts" not in event:
            event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        p = self.root / "episodic" / f"{time.strftime('%Y-%m-%d')}.jsonl"
        with p.open("a") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def load_episodic(self, days: int = 7) -> list:
        events = []
        for i in range(days):
            date = time.strftime("%Y-%m-%d", time.localtime(time.time() - i * 86400))
            p = self.root / "episodic" / f"{date}.jsonl"
            if p.exists():
                for line in p.read_text().splitlines():
                    if line.strip():
                        events.append(json.loads(line))
        return events

    def count_today_episodic(self) -> int:
        p = self.root / "episodic" / f"{time.strftime('%Y-%m-%d')}.jsonl"
        if not p.exists():
            return 0
        return sum(1 for line in p.read_text().splitlines() if line.strip())

    # -- baseline zone: per-metric sliding window --

    def _get_baseline_lock(self, metric: str) -> threading.Lock:
        with self._locks_lock:
            if metric not in self._baseline_locks:
                self._baseline_locks[metric] = threading.Lock()
            return self._baseline_locks[metric]

    def update_baseline(self, metric: str, value: float):
        safe = self._sanitize(metric)
        p = self.root / "baseline" / f"{safe}.json"
        lock = self._get_baseline_lock(safe)
        with lock:
            if value == -1:
                p.write_text(json.dumps({"values": []}))
                return
            try:
                data = json.loads(p.read_text()) if p.exists() else {"values": []}
            except json.JSONDecodeError:
                data = {"values": []}
            data["values"].append(value)
            data["values"] = data["values"][-168:]
            p.write_text(json.dumps(data))

    def replace_baseline(self, metric: str, values: list):
        safe = self._sanitize(metric)
        p = self.root / "baseline" / f"{safe}.json"
        lock = self._get_baseline_lock(safe)
        with lock:
            p.write_text(json.dumps({"values": values[-168:]}))

    def load_baseline(self, metric: str) -> list:
        safe = self._sanitize(metric)
        p = self.root / "baseline" / f"{safe}.json"
        lock = self._get_baseline_lock(safe)
        with lock:
            if not p.exists():
                return []
            try:
                return json.loads(p.read_text()).get("values", [])
            except json.JSONDecodeError:
                return []

    # -- alerts zone --

    def append_alert(self, alert: dict):
        if "ts" not in alert:
            alert["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        p = self.root / "alerts" / "history.jsonl"
        with p.open("a") as f:
            f.write(json.dumps(alert, ensure_ascii=False) + "\n")

    def load_alerts(self) -> list:
        p = self.root / "alerts" / "history.jsonl"
        if not p.exists():
            return []
        return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]
