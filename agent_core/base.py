import asyncio, signal, sys, json
from pathlib import Path


class BaseDaemon:
    """Async daemon base: main loop, check registration, lifecycle."""

    def __init__(self, name: str, config: dict, store):
        self.name = name
        self.config = config
        self.store = store
        self.running = False
        self.check_interval = config.get("check_interval", 60)
        self._checks = {}
        self._on_shutdown = []
        self._on_check_complete = []
        self._stop_marker = Path(config.get("stop_marker", "/tmp/stop_daemon"))

    def register_check(self, name: str, check_fn):
        self._checks[name] = check_fn

    def on_shutdown(self, cb):
        self._on_shutdown.append(cb)

    def on_check_complete(self, cb):
        self._on_check_complete.append(cb)
        return cb

    async def run(self):
        self.running = True
        self.store.update_working({"daemon": self.name, "status": "running"})

        try:
            while self.running:
                if self._stop_marker.exists():
                    self._stop_marker.unlink(missing_ok=True)
                    break
                await self._run_checks()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

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
