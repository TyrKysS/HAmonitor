"""
HA Monitor — logs entity state changes to a daily-rotating CSV file.

CSV columns:
  timestamp       — ISO-8601 datetime of the change
  entity_id       — unique HA identifier  (e.g. sensor.living_room_temp)
  friendly_name   — human-readable name    (e.g. Living Room Temperature)
  domain          — entity domain           (e.g. sensor)
  previous_state  — state before the change
  new_state       — state after the change
  unit            — unit of measurement (if available, else empty)
  annotation      — intentionally empty; filled by the user after export
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

# ---------------------------------------------------------------------------
# Configuration from environment (injected by run.sh)
# ---------------------------------------------------------------------------

HA_TOKEN: str = os.environ.get("HA_TOKEN", "")
HA_WS_URL: str = "ws://supervisor/core/websocket"
OUTPUT_DIR: Path = Path(os.environ.get("MONITOR_OUTPUT_DIR", "/share/ha_monitor"))
LOG_STDOUT: bool = os.environ.get("MONITOR_LOG_STDOUT", "true").lower() == "true"

# Domains to watch — read from env as JSON array string produced by bashio
_raw_domains = os.environ.get("MONITOR_DOMAINS", '["sensor","binary_sensor"]')
try:
    WATCHED_DOMAINS: set[str] = set(json.loads(_raw_domains))
except json.JSONDecodeError:
    # bashio may emit them space-separated without brackets
    WATCHED_DOMAINS = set(_raw_domains.replace("[", "").replace("]", "").replace('"', "").split())

CSV_HEADER = [
    "timestamp",
    "entity_id",
    "friendly_name",
    "domain",
    "previous_state",
    "new_state",
    "unit",
    "annotation",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {message}"
    print(line, flush=True)


def today_csv_path() -> Path:
    """Return the path for today's CSV file (rotates at midnight UTC)."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUTPUT_DIR / f"ha_monitor_{date_str}.csv"


def ensure_csv_header(path: Path) -> None:
    """Create the CSV with a header row if it does not yet exist."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)


def append_row(row: dict) -> None:
    """Append one change record to today's CSV."""
    path = today_csv_path()
    ensure_csv_header(path)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writerow(row)


def extract_state_info(state_obj: dict | None) -> tuple[str, str, str]:
    """Return (state_value, friendly_name, unit) from a HA state object."""
    if state_obj is None:
        return "unknown", "", ""
    state_val = state_obj.get("state", "unknown")
    attrs = state_obj.get("attributes", {})
    friendly = attrs.get("friendly_name", state_obj.get("entity_id", ""))
    unit = attrs.get("unit_of_measurement", "")
    return state_val, friendly, unit


def describe_change(friendly: str, entity_id: str, prev: str, new: str, unit: str) -> str:
    """Return a human-readable one-line description of the state change."""
    unit_str = f" {unit}" if unit else ""
    return (
        f"{friendly} ({entity_id}): {prev}{unit_str} → {new}{unit_str}"
    )


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------

class HAMonitor:
    def __init__(self) -> None:
        self._msg_id = 1

    def _next_id(self) -> int:
        i = self._msg_id
        self._msg_id += 1
        return i

    async def run(self) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        log(f"Connecting to {HA_WS_URL}")
        log(f"Watching domains: {sorted(WATCHED_DOMAINS)}")
        log(f"Output directory: {OUTPUT_DIR}")

        backoff = 5
        while True:
            try:
                await self._connect()
            except Exception as exc:
                log(f"Connection error: {exc}. Reconnecting in {backoff}s…")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)
            else:
                backoff = 5

    async def _connect(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(HA_WS_URL) as ws:
                # Step 1 — receive auth_required
                msg = await ws.receive_json()
                if msg.get("type") != "auth_required":
                    raise RuntimeError(f"Unexpected initial message: {msg}")

                # Step 2 — authenticate
                await ws.send_json({"type": "auth", "access_token": HA_TOKEN})
                msg = await ws.receive_json()
                if msg.get("type") != "auth_ok":
                    raise RuntimeError(f"Authentication failed: {msg}")
                log("Authenticated with Home Assistant.")

                # Step 3 — subscribe to state_changed events
                sub_id = self._next_id()
                await ws.send_json({
                    "id": sub_id,
                    "type": "subscribe_events",
                    "event_type": "state_changed",
                })
                msg = await ws.receive_json()
                if not msg.get("success"):
                    raise RuntimeError(f"Subscription failed: {msg}")
                log("Subscribed to state_changed events. Monitoring started.")

                # Step 4 — event loop
                async for raw in ws:
                    if raw.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(json.loads(raw.data))
                    elif raw.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise ConnectionError("WebSocket closed unexpectedly.")

    async def _handle_message(self, msg: dict) -> None:
        if msg.get("type") != "event":
            return

        event = msg.get("event", {})
        if event.get("event_type") != "state_changed":
            return

        data = event.get("data", {})
        entity_id: str = data.get("entity_id", "")
        domain = entity_id.split(".")[0] if "." in entity_id else ""

        if domain not in WATCHED_DOMAINS:
            return

        old_state_obj = data.get("old_state")
        new_state_obj = data.get("new_state")

        prev_val, friendly, unit = extract_state_info(old_state_obj)
        new_val, friendly_new, unit_new = extract_state_info(new_state_obj)

        # Prefer the new state's friendly name (entity may have been renamed)
        if friendly_new:
            friendly = friendly_new
        if unit_new:
            unit = unit_new

        # Skip if no actual state change (attribute-only updates)
        if prev_val == new_val:
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        row = {
            "timestamp": timestamp,
            "entity_id": entity_id,
            "friendly_name": friendly,
            "domain": domain,
            "previous_state": prev_val,
            "new_state": new_val,
            "unit": unit,
            "annotation": "",
        }

        append_row(row)

        if LOG_STDOUT:
            log(describe_change(friendly, entity_id, prev_val, new_val, unit))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not HA_TOKEN:
        log("ERROR: HA_TOKEN is not set. Cannot authenticate with Home Assistant.")
        sys.exit(1)

    monitor = HAMonitor()
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        log("Stopped by user.")
