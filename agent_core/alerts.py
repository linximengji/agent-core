import time


class AlertManager:
    """Alert with severity levels and cooling."""

    def __init__(self, store, cooldown: int = 1800):
        self.store = store
        self.cooldown = cooldown
        self._last_sent = {}

    def fire(self, severity: str, service: str, message: str):
        if severity not in ("INFO", "WARN", "CRITICAL"):
            severity = "WARN"
        key = f"{service}:{message[:60]}"
        now = time.time()
        last = self._last_sent.get(key, 0)
        # CRITICAL uses a shorter cooldown (5 min) instead of bypassing entirely
        effective_cd = 300 if severity == "CRITICAL" else self.cooldown
        if now - last < effective_cd:
            return
        self._last_sent[key] = now
        record = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "severity": severity,
            "service": service,
            "message": message,
            "acknowledged": False,
        }
        self.store.append_alert(record)
        return record
