import asyncio, signal, sys, json
from pathlib import Path


class BaseDaemon:
    """Async daemon base: main loop, check registration, lifecycle."""

    def __init__(self, name: str, config: dict, store, heartbeat_path: str | None = None):
        self.name = name
        self.config = config
        self.store = store
        self.running = False
        self.check_interval = config.get("check_interval", 60)
        self._checks = {}
        self._on_shutdown = []
        self._on_check_complete = []
        self._stop_marker = Path(config.get("stop_marker", "/tmp/stop_daemon"))
        self._heartbeat_path = Path(heartbeat_path) if heartbeat_path else None

    def register_check(self, name: str, check_fn):
        self._checks[name] = check_fn

    def on_shutdown(self, cb):
        self._on_shutdown.append(cb)

    def on_check_complete(self, cb):
        self._on_check_complete.append(cb)
        return cb

    async def run(self):
        self.running = True

        try:
            while self.running:
                if self._check_stop():
                    break
                await self._run_checks()
                # Responsive sleep: check stop marker every 5s
                _stop_seen = False
                for _ in range(self.check_interval // 5):
                    if self._check_stop():
                        _stop_seen = True
                        break
                    await asyncio.sleep(5)
                if _stop_seen:
                    break
                remaining = self.check_interval % 5
                if remaining:
                    await asyncio.sleep(remaining)
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    def _check_stop(self) -> bool:
        if self._stop_marker.exists():
            self._stop_marker.unlink(missing_ok=True)
            return True
        return False

    async def _run_checks(self):
        results = {"daemon": self.name, "ts": __import__("time").strftime("%Y-%m-%dT%H:%M:%S")}
        for name, fn in self._checks.items():
            try:
                results[name] = await fn()
            except Exception as e:
                results[name] = {"error": str(e)}
                self.store.append_episodic({
                    "type": "check_error", "service": name, "error": str(e)
                })
        self.store.update_working(results)
        # Write heartbeat BEFORE on_check_complete (repair can block 30s+)
        if self._heartbeat_path:
            self._heartbeat_path.write_text(str(__import__("time").time()))
        for cb in self._on_check_complete:
            try:
                await cb(results)
            except Exception:
                pass

    async def _shutdown(self):
        self.running = False
        for cb in self._on_shutdown:
            try:
                cb()
            except Exception:
                pass
        self.store.update_working({"daemon": self.name, "status": "stopped"})
