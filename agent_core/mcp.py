from mcp.server.fastmcp import FastMCP
import time


def build_mcp(name: str, store) -> FastMCP:
    """Create a FastMCP server exposing daemon state.
    Subclasses should register additional tools after creation."""
    mcp = FastMCP(name)

    @mcp.tool()
    async def status() -> dict:
        """Return latest daemon working state."""
        return store.load_working()

    @mcp.tool()
    async def recent_events(hours: int = 24) -> list:
        """Return episodic events from the last N hours."""
        all_events = store.load_episodic(days=1)
        cutoff = time.time() - hours * 3600
        return [e for e in all_events if parse_ts(e.get("ts", "")) >= cutoff]

    return mcp


def parse_ts(ts: str) -> float:
    try:
        return time.mktime(time.strptime(ts.split(".")[0][:19], "%Y-%m-%dT%H:%M:%S"))
    except (ValueError, IndexError):
        return 0
