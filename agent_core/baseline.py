import math
from opentelemetry import trace
from .state_store import StateStore

_tracer = trace.get_tracer("agent_core.baseline")


class BaselineEngine:
    """Track baseline ranges for numeric metrics using sliding window.
    Auto-resets window on regime shifts via accelerated decay."""

    def __init__(self, store: StateStore, reset_after: int = 5,
                 decay_factor: float = 0.99, shift_threshold: int = 20):
        self.store = store
        self.window_size = 168
        self.reset_after = reset_after
        self.decay_factor = decay_factor
        self.shift_threshold = shift_threshold
        self._consecutive_anomalies: dict[str, int] = {}
        self._shift_counters: dict[str, int] = {}
        self._shift_direction: dict[str, str] = {}

    def record(self, metric: str, value: float):
        with _tracer.start_as_current_span("baseline.record") as span:
            span.set_attribute("metric", metric)
            span.set_attribute("value", value)
            result = self.analyze(metric, value)

            key = f"anomaly_count:{metric}"
            is_anomaly = result["status"] == "anomaly"

            if is_anomaly:
                count = self._consecutive_anomalies.get(key, 0) + 1
                self._consecutive_anomalies[key] = count
            else:
                self._consecutive_anomalies[key] = 0
                self._shift_counters.pop(metric, None)
                self._shift_direction.pop(metric, None)

            self._track_regime_shift(metric, value, result)

            shift_counter = self._shift_counters.get(metric, 0)
            if shift_counter >= self.shift_threshold:
                self._accelerated_decay(metric, value)
                self._shift_counters[metric] = 0
                self._shift_direction.pop(metric, None)

            self.store.update_baseline(metric, value)
            span.set_attribute("status", result["status"])
            if "deviation" in result:
                span.set_attribute("deviation", result["deviation"])

    def _track_regime_shift(self, metric, value, result):
        if "deviation" not in result or result.get("status") == "learning":
            return
        dev = result["deviation"]
        if abs(dev) < 0.5:
            return

        direction = "above" if dev > 0 else "below"
        prev_dir = self._shift_direction.get(metric)
        if prev_dir == direction:
            self._shift_counters[metric] = self._shift_counters.get(metric, 0) + 1
        else:
            self._shift_counters[metric] = 1
            self._shift_direction[metric] = direction

    def _accelerated_decay(self, metric, new_value):
        values = self.store.load_baseline(metric)
        if len(values) < self.shift_threshold:
            return
        fast_decay = 0.85
        weights = [fast_decay ** (len(values) - i) for i in range(1, len(values) + 1)]
        threshold_w = sum(weights) * 0.005
        keep = []
        active_weight = 1.0
        for v in reversed(values):
            keep.append(v)
            active_weight *= fast_decay
            if active_weight < threshold_w:
                break
        keep.reverse()
        if len(keep) < 10:
            keep = values[-10:]
        self.store.replace_baseline(metric, keep)

    def analyze(self, metric: str, value: float) -> dict:
        with _tracer.start_as_current_span("baseline.analyze") as span:
            span.set_attribute("metric", metric)
            values = self.store.load_baseline(metric)
            span.set_attribute("sample_count", len(values))
            if len(values) < 10:
                span.set_attribute("status", "learning")
                return {"status": "learning", "needed": 10 - len(values)}

            wm = self._weighted_median(values, self.decay_factor)
            wdev = self._weighted_stdev(values, self.decay_factor, wm)
            p99 = sorted(values)[int(len(values) * 0.99)]
            deviation = (value - wm) / max(wdev, 0.01)
            span.set_attribute("median", round(wm, 2))
            span.set_attribute("p99", p99)
            span.set_attribute("deviation", round(deviation, 2))
            span.set_attribute("status", "ok" if abs(deviation) < 3 else "anomaly")

            return {
                "status": "ok" if abs(deviation) < 3 else "anomaly",
                "value": value, "median": round(wm, 2),
                "p99": p99, "deviation": round(deviation, 2),
            }

    def summary(self, metrics: dict) -> list:
        violations = []
        for name, value in metrics.items():
            result = self.analyze(name, value)
            if result["status"] == "anomaly":
                result["metric"] = name
                violations.append(result)
        return violations

    def _weighted_median(self, values, decay_factor):
        n = len(values)
        weights = [decay_factor ** (n - i) for i in range(1, n + 1)]
        sorted_pairs = sorted(zip(values, weights), key=lambda x: x[0])
        total_weight = sum(weights)
        half = total_weight / 2
        cumulative = 0.0
        for v, w in sorted_pairs:
            cumulative += w
            if cumulative >= half:
                return v
        return sorted_pairs[-1][0]

    def _weighted_stdev(self, values, decay_factor, mean):
        n = len(values)
        weights = [decay_factor ** (n - i) for i in range(1, n + 1)]
        total_weight = sum(weights)
        variance = sum(w * (v - mean) ** 2 for v, w in zip(values, weights)) / total_weight
        return math.sqrt(variance)
