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
  score           — significance score 0–10 (0 = no change, 10 = maximum)
  score_label     — human-readable tier: Negligible/Low/Moderate/Significant/High/Critical
  score_reason    — short explanation of why this score was assigned
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
SCORE_MIN_THRESHOLD: int = int(os.environ.get("MONITOR_SCORE_MIN_THRESHOLD", "0"))

OLLAMA_ENABLED: bool = os.environ.get("MONITOR_OLLAMA_ENABLED", "false").lower() == "true"
OLLAMA_URL: str = os.environ.get("MONITOR_OLLAMA_URL", "http://localhost:11434").strip().rstrip("/")
OLLAMA_MODEL: str = os.environ.get("MONITOR_OLLAMA_MODEL", "llama3.2:3b").strip()
OLLAMA_SCORE_THRESHOLD: int = int(os.environ.get("MONITOR_OLLAMA_SCORE_THRESHOLD", "7").strip())
OLLAMA_LANGUAGE: str = os.environ.get("MONITOR_OLLAMA_LANGUAGE", "cs").strip()

HA_REST_URL: str = "http://supervisor/core/api"

_raw_automations = os.environ.get("MONITOR_AUTOMATIONS", "[]")
try:
    AUTOMATIONS: list[dict] = json.loads(_raw_automations) if _raw_automations.strip() else []
except json.JSONDecodeError:
    AUTOMATIONS = []

LLM_ACTIONS_ENABLED: bool = os.environ.get("MONITOR_LLM_ACTIONS_ENABLED", "false").lower() == "true"
_raw_llm_domains = os.environ.get("MONITOR_LLM_ACTIONS_DOMAINS", '["climate","input_boolean","switch","input_number"]')
try:
    LLM_ACTIONS_DOMAINS: set[str] = set(json.loads(_raw_llm_domains))
except json.JSONDecodeError:
    LLM_ACTIONS_DOMAINS = {"climate", "input_boolean", "switch", "input_number"}

_raw_domains = os.environ.get("MONITOR_DOMAINS", '["sensor","binary_sensor"]')
try:
    WATCHED_DOMAINS: set[str] = set(json.loads(_raw_domains))
except json.JSONDecodeError:
    WATCHED_DOMAINS = set(_raw_domains.replace("[", "").replace("]", "").replace('"', "").split())

CSV_HEADER = [
    "timestamp",
    "entity_id",
    "friendly_name",
    "domain",
    "previous_state",
    "new_state",
    "unit",
    "score",
    "score_label",
    "score_reason",
    "annotation",
    "llm_explanation",
]

# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

_SCORE_LABELS = {
    0: "Negligible",
    1: "Low",
    2: "Low",
    3: "Moderate",
    4: "Moderate",
    5: "Significant",
    6: "Significant",
    7: "High",
    8: "High",
    9: "Critical",
    10: "Critical",
}

# Keywords checked against the lower-cased entity_id
_PRESENCE_KEYWORDS = ("presence", "occupancy", "person", "people")
_MOTION_KEYWORDS = ("motion", "movement", "pir", "vibration")
_DOOR_KEYWORDS = ("door", "gate", "hatch", "entry", "entrance")
_WINDOW_KEYWORDS = ("window",)
_SMOKE_KEYWORDS = ("smoke", "fire", "co2_alarm", "gas")
_FLOOD_KEYWORDS = ("flood", "leak", "water_sensor")
_TEMP_UNITS = {"°c", "°f", "c", "f", "celsius", "fahrenheit"}
_HUMID_KEYWORDS = ("humidity", "humid")
_CO2_KEYWORDS = ("co2", "co_2", "carbon_dioxide", "voc", "pm2", "pm10", "air_quality")
_ILLUMINANCE_KEYWORDS = ("illuminance", "lux", "light_level")


def _score_label(score: int) -> str:
    return _SCORE_LABELS.get(max(0, min(10, score)), "Unknown")


def _score_temperature_delta(delta: float, unit: str) -> tuple[int, str]:
    """Score a numeric temperature change."""
    # Convert Fahrenheit delta to Celsius equivalent for uniform thresholds
    if unit.lower() in ("°f", "f", "fahrenheit"):
        delta_c = delta / 1.8
    else:
        delta_c = delta

    if delta_c < 0.5:
        score = 0
    elif delta_c < 1.0:
        score = 1
    elif delta_c < 2.0:
        score = 2
    elif delta_c < 5.0:
        score = 4
    else:
        score = 6

    return score, f"Δtemp={delta:.2g}{unit}"


def _score_humidity_delta(delta: float) -> tuple[int, str]:
    if delta < 1.0:
        return 0, f"Δhum={delta:.1f}%"
    if delta < 3.0:
        return 1, f"Δhum={delta:.1f}%"
    if delta < 7.0:
        return 3, f"Δhum={delta:.1f}%"
    return 5, f"Δhum={delta:.1f}%"


def _score_co2_delta(delta: float, unit: str) -> tuple[int, str]:
    """Score air-quality sensor changes (CO2 in ppm, VOC, etc.)."""
    if delta < 50:
        return 1, f"Δair={delta:.0f}{unit}"
    if delta < 200:
        return 3, f"Δair={delta:.0f}{unit}"
    if delta < 500:
        return 5, f"Δair={delta:.0f}{unit}"
    return 7, f"Δair={delta:.0f}{unit}"


def score_change(
    entity_id: str,
    domain: str,
    prev_val: str,
    new_val: str,
    unit: str,
) -> tuple[int, str]:
    """Return (score 0–10, reason) for a state transition.

    Rules are evaluated top-to-bottom; first match wins.
    """
    eid = entity_id.lower()
    unit_stripped = unit.strip()
    unit_low = unit_stripped.lower()
    new_low = new_val.lower()

    # --- Safety/security events — always top priority ---

    if domain == "alarm_control_panel":
        return 10, f"alarm: {prev_val} → {new_val}"

    if any(kw in eid for kw in _SMOKE_KEYWORDS):
        if new_low in ("detected", "on", "true", "1"):
            return 10, "smoke/fire/gas detected"
        return 7, "smoke/fire/gas cleared"

    if any(kw in eid for kw in _FLOOD_KEYWORDS):
        if new_low in ("detected", "on", "true", "1"):
            return 10, "flood/leak detected"
        return 7, "flood/leak cleared"

    # --- Presence & motion ---

    if any(kw in eid for kw in _PRESENCE_KEYWORDS):
        return 10, "presence/occupancy change"

    if any(kw in eid for kw in _MOTION_KEYWORDS):
        if new_low in ("detected", "on", "true", "1"):
            return 10, "motion detected"
        return 7, "motion cleared"

    # --- Access points ---

    if domain == "lock":
        if new_low == "unlocked":
            return 9, "lock opened"
        return 7, "lock secured"

    if any(kw in eid for kw in _DOOR_KEYWORDS):
        if new_low in ("open", "on", "true", "1"):
            return 8, "door/gate opened"
        return 6, "door/gate closed"

    if any(kw in eid for kw in _WINDOW_KEYWORDS):
        if new_low in ("open", "on", "true", "1"):
            return 7, "window opened"
        return 5, "window closed"

    # --- Climate / comfort sensors (numeric) ---

    try:
        prev_num = float(prev_val)
        new_num = float(new_val)
        delta = abs(new_num - prev_num)

        if unit_low in _TEMP_UNITS:
            return _score_temperature_delta(delta, unit_stripped)

        if unit_low == "%" and any(kw in eid for kw in _HUMID_KEYWORDS):
            return _score_humidity_delta(delta)

        if any(kw in eid for kw in _CO2_KEYWORDS):
            return _score_co2_delta(delta, unit_stripped)

        if any(kw in eid for kw in _ILLUMINANCE_KEYWORDS):
            if delta < 20:
                return 0, f"Δlux={delta:.0f}"
            if delta < 100:
                return 1, f"Δlux={delta:.0f}"
            return 3, f"Δlux={delta:.0f}"

        # Generic numeric — score proportional to relative change
        if prev_num != 0:
            rel = delta / abs(prev_num)
            if rel < 0.01:
                return 0, f"Δ={delta:.3g} {unit_stripped}".strip()
            if rel < 0.05:
                return 1, f"Δ={delta:.3g} {unit_stripped}".strip()
            if rel < 0.15:
                return 2, f"Δ={delta:.3g} {unit_stripped}".strip()
            return 3, f"Δ={delta:.3g} {unit_stripped}".strip()
        else:
            # Previous was 0 — any change is notable
            return 3, f"Δ={delta:.3g} {unit_stripped}".strip()

    except (ValueError, TypeError):
        pass

    # --- Non-numeric / binary / enumerated states ---

    if domain in ("light", "switch", "input_boolean"):
        if new_low in ("on", "true", "1"):
            return 5, f"{domain} turned on"
        return 4, f"{domain} turned off"

    if domain == "climate":
        return 7, f"climate mode: {prev_val} → {new_val}"

    if domain == "cover":
        if new_low in ("open", "opening"):
            return 5, "cover opening"
        return 4, "cover closing/closed"

    if domain == "media_player":
        if new_low == "playing":
            return 4, "playback started"
        if new_low in ("idle", "off"):
            return 3, "playback stopped"
        return 3, f"media: {prev_val} → {new_val}"

    if domain == "input_select":
        return 3, f"option: {prev_val} → {new_val}"

    if domain == "binary_sensor":
        if new_low in ("on", "true", "detected", "1"):
            return 5, f"binary on: {new_val}"
        return 4, f"binary off: {new_val}"

    return 3, f"state: {prev_val} → {new_val}"


# ---------------------------------------------------------------------------
# Ollama LLM explanation
# ---------------------------------------------------------------------------

async def _fetch_ollama_models() -> list[str]:
    """Return model names available on the configured Ollama instance."""
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{OLLAMA_URL}/api/tags") as resp:
                if resp.status != 200:
                    log(f"Ollama /api/tags returned HTTP {resp.status}")
                    return []
                data = await resp.json()
                return [m["name"] for m in data.get("models", [])]
    except asyncio.TimeoutError:
        log("Ollama /api/tags timed out — is the URL correct?")
        return []
    except Exception as exc:
        log(f"Ollama /api/tags error: {exc}")
        return []


async def _call_ollama(
    entity_id: str,
    friendly: str,
    domain: str,
    prev_val: str,
    new_val: str,
    unit: str,
    score: int,
    label: str,
    reason: str,
) -> str:
    """Ask Ollama to produce a one-sentence human-readable description of the event.

    Returns an empty string on any error or timeout so callers can proceed safely.
    """
    unit_str = f" {unit}" if unit else ""

    if OLLAMA_LANGUAGE == "cs":
        prompt = (
            f"Entita: {friendly} ({entity_id})\n"
            f"Doména: {domain}\n"
            f"Předchozí stav: {prev_val}{unit_str}\n"
            f"Nový stav: {new_val}{unit_str}\n"
            f"Hodnocení: {score}/10 ({label}) – {reason}\n\n"
            "Napiš jednu větu v češtině, která přirozeně popisuje, co se právě stalo. "
            "Použij konkrétní hodnoty stavů. Odpovídej pouze touto větou, bez dalšího textu."
        )
    else:
        prompt = (
            f"Entity: {friendly} ({entity_id})\n"
            f"Domain: {domain}\n"
            f"Previous state: {prev_val}{unit_str}\n"
            f"New state: {new_val}{unit_str}\n"
            f"Score: {score}/10 ({label}) – {reason}\n\n"
            "Write one sentence in English that naturally describes what just happened. "
            "Use the concrete state values. Reply with this sentence only, no other text."
        )

    timeout = aiohttp.ClientTimeout(total=120)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    log(f"Ollama HTTP {resp.status} for {entity_id}: {body}")
                    return ""
                data = await resp.json()
                explanation = data.get("response", "").strip()
                return explanation.replace("\n", " ").replace("\r", "")[:500]
    except asyncio.TimeoutError:
        log(f"Ollama timeout for {entity_id}")
        return ""
    except Exception as exc:
        log(f"Ollama error for {entity_id}: {exc}")
        return ""


async def _call_ha_service(service: str, target: str, extra_data: str) -> None:
    """Call a HA service via the Supervisor REST API."""
    if "." not in service:
        log(f"Automation: invalid action_service '{service}' (expected 'domain.service')")
        return
    domain, service_name = service.split(".", 1)
    url = f"{HA_REST_URL}/services/{domain}/{service_name}"

    body: dict = {}
    if target:
        body["entity_id"] = target
    if extra_data:
        try:
            body.update(json.loads(extra_data))
        except json.JSONDecodeError:
            log(f"Automation: invalid action_data JSON for {service}: {extra_data!r}")
            return

    headers = {"Authorization": f"Bearer {HA_TOKEN}"}
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=body, headers=headers) as resp:
                if resp.status in (200, 201):
                    log(f"Automation: {service} on '{target or '(no target)'}' → OK")
                else:
                    body_text = (await resp.text())[:200]
                    log(f"Automation: {service} failed HTTP {resp.status}: {body_text}")
    except asyncio.TimeoutError:
        log(f"Automation: timeout calling {service}")
    except Exception as exc:
        log(f"Automation: error calling {service}: {exc}")


async def _fetch_ha_states(domains: set[str]) -> list[dict]:
    """Return HA states filtered to the given domains."""
    headers = {"Authorization": f"Bearer {HA_TOKEN}"}
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{HA_REST_URL}/states", headers=headers) as resp:
                if resp.status != 200:
                    log(f"LLM actions: /api/states returned HTTP {resp.status}")
                    return []
                all_states = await resp.json()
                return [
                    s for s in all_states
                    if s.get("entity_id", "").split(".")[0] in domains
                ]
    except asyncio.TimeoutError:
        log("LLM actions: /api/states timed out")
        return []
    except Exception as exc:
        log(f"LLM actions: /api/states error: {exc}")
        return []


async def _ask_llm_for_actions(
    entity_id: str,
    friendly: str,
    prev_val: str,
    new_val: str,
    score_reason: str,
    available: list[dict],
) -> list[dict]:
    """Ask Ollama which entities (if any) should be controlled in response to this event.

    Returns a list of dicts with keys: entity_id, service, service_data (optional).
    Returns [] on any error or when no action is needed.
    """
    valid_ids = {s["entity_id"] for s in available}

    lines = []
    for s in available:
        fn = s.get("attributes", {}).get("friendly_name") or s["entity_id"]
        lines.append(f"  {s['entity_id']} | {fn} | stav: {s['state']}")
    entity_list = "\n".join(lines)

    if OLLAMA_LANGUAGE == "cs":
        prompt = (
            f"Událost v Home Assistant:\n"
            f"  Entita: {friendly} ({entity_id})\n"
            f"  Změna stavu: '{prev_val}' → '{new_val}'\n"
            f"  Důvod: {score_reason}\n\n"
            f"Dostupné ovladatelné entity:\n{entity_list}\n\n"
            "Úkol: Pokud tato událost vyžaduje ovládání některé entity (např. vypnutí termostatu "
            "nebo vytápění po otevření okna), odpověz POUZE platným JSON polem. Každá akce má klíče "
            "\"entity_id\" a \"service\" (ve formátu \"doména.služba\"). Příklad:\n"
            '[{"entity_id": "climate.obyvak", "service": "climate.turn_off"}]\n'
            "Pokud akce není potřeba, odpověz: []\n"
            "Odpovídej VÝHRADNĚ JSON bez jakéhokoliv dalšího textu."
        )
    else:
        prompt = (
            f"Home Assistant event:\n"
            f"  Entity: {friendly} ({entity_id})\n"
            f"  State change: '{prev_val}' → '{new_val}'\n"
            f"  Reason: {score_reason}\n\n"
            f"Controllable entities:\n{entity_list}\n\n"
            "Task: If this event requires controlling any entity (e.g. turning off a thermostat "
            "or heating after a window opens), reply ONLY with a valid JSON array. Each action has "
            "keys \"entity_id\" and \"service\" (format \"domain.service\"). Example:\n"
            '[{"entity_id": "climate.living_room", "service": "climate.turn_off"}]\n'
            "If no action is needed, reply: []\n"
            "Reply EXCLUSIVELY with JSON, no other text."
        )

    timeout = aiohttp.ClientTimeout(total=120)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            ) as resp:
                if resp.status != 200:
                    log(f"LLM actions: Ollama HTTP {resp.status}")
                    return []
                data = await resp.json()
                raw = data.get("response", "").strip()
                # Strip optional markdown fences
                if "```" in raw:
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                    raw = raw.strip()
                actions = json.loads(raw)
                # Validate: only allow entity_ids that actually exist in HA
                validated = [
                    a for a in actions
                    if isinstance(a, dict)
                    and a.get("entity_id") in valid_ids
                    and isinstance(a.get("service"), str)
                    and "." in a["service"]
                ]
                if len(validated) < len(actions):
                    log(f"LLM actions: {len(actions) - len(validated)} action(s) dropped (unknown entity_id)")
                return validated
    except (json.JSONDecodeError, ValueError) as exc:
        log(f"LLM actions: could not parse Ollama response as JSON: {exc}")
        return []
    except asyncio.TimeoutError:
        log(f"LLM actions: Ollama timeout for {entity_id}")
        return []
    except Exception as exc:
        log(f"LLM actions: error for {entity_id}: {exc}")
        return []


async def _ollama_background_task(
    entity_id: str,
    friendly: str,
    domain: str,
    prev_val: str,
    new_val: str,
    unit: str,
    score: int,
    label: str,
    reason: str,
    timestamp: str,
) -> None:
    """Fire-and-forget wrapper: calls Ollama for explanation and optional LLM-driven actions."""
    explanation = await _call_ollama(
        entity_id, friendly, domain, prev_val, new_val, unit, score, label, reason
    )
    if explanation:
        log(f"[LLM] {timestamp} {friendly}: {explanation}")

    if LLM_ACTIONS_ENABLED:
        available = await _fetch_ha_states(LLM_ACTIONS_DOMAINS)
        if not available:
            log("LLM actions: no controllable entities found, skipping")
            return
        actions = await _ask_llm_for_actions(
            entity_id, friendly, prev_val, new_val, reason, available
        )
        if not actions:
            log(f"[LLM] No actions decided for {entity_id}")
            return
        for action in actions:
            target = action["entity_id"]
            svc = action["service"]
            extra = json.dumps(action.get("service_data", {})) if action.get("service_data") else ""
            log(f"[LLM] Action: {svc} on {target}")
            await _call_ha_service(svc, target, extra)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}", flush=True)


def today_csv_path() -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUTPUT_DIR / f"ha_monitor_{date_str}.csv"


def ensure_csv_header(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADER)


def append_row(row: dict) -> None:
    path = today_csv_path()
    ensure_csv_header(path)
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CSV_HEADER).writerow(row)


def extract_state_info(state_obj: dict | None) -> tuple[str, str, str]:
    if state_obj is None:
        return "unknown", "", ""
    state_val = state_obj.get("state", "unknown")
    attrs = state_obj.get("attributes", {})
    friendly = attrs.get("friendly_name", state_obj.get("entity_id", ""))
    unit = attrs.get("unit_of_measurement", "")
    return state_val, friendly, unit


def describe_change(
    friendly: str,
    entity_id: str,
    prev: str,
    new: str,
    unit: str,
    score: int,
    label: str,
    reason: str,
) -> str:
    unit_str = f" {unit}" if unit else ""
    return (
        f"[{score:2d}/{label:11s}] {friendly} ({entity_id}): "
        f"{prev}{unit_str} → {new}{unit_str}  ({reason})"
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
        log(f"Score threshold: {SCORE_MIN_THRESHOLD} (recording changes with score >= {SCORE_MIN_THRESHOLD})")
        if AUTOMATIONS:
            log(f"Automations loaded: {len(AUTOMATIONS)} rule(s)")
            for r in AUTOMATIONS:
                log(f"  rule: entity contains '{r.get('trigger_keyword')}' + state='{r.get('trigger_state')}' → {r.get('action_service')} on '{r.get('action_target', '')}'")
        else:
            log("No automations configured.")

        if OLLAMA_ENABLED:
            log(f"Ollama enabled: {OLLAMA_URL}  model={OLLAMA_MODEL}  threshold={OLLAMA_SCORE_THRESHOLD}  lang={OLLAMA_LANGUAGE}")
            if LLM_ACTIONS_ENABLED:
                log(f"LLM actions enabled — controllable domains: {sorted(LLM_ACTIONS_DOMAINS)}")
            else:
                log("LLM actions disabled (set llm_actions_enabled: true to let the LLM control entities)")
            available = await _fetch_ollama_models()
            if available:
                log(f"Ollama available models: {', '.join(available)}")
                if OLLAMA_MODEL not in available:
                    log(f"WARNING: configured model '{OLLAMA_MODEL}' not found — LLM explanations will fail until the model is pulled or the config is updated")
            else:
                log("WARNING: could not reach Ollama — LLM explanations will be skipped until the connection is restored")
        else:
            log("Ollama disabled (set ollama_enabled: true to activate LLM explanations)")

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
                msg = await ws.receive_json()
                if msg.get("type") != "auth_required":
                    raise RuntimeError(f"Unexpected initial message: {msg}")

                await ws.send_json({"type": "auth", "access_token": HA_TOKEN})
                msg = await ws.receive_json()
                if msg.get("type") != "auth_ok":
                    raise RuntimeError(f"Authentication failed: {msg}")
                log("Authenticated with Home Assistant.")

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

        if friendly_new:
            friendly = friendly_new
        if unit_new:
            unit = unit_new

        if prev_val == new_val:
            return

        score, reason = score_change(entity_id, domain, prev_val, new_val, unit)
        label = _score_label(score)

        if score < SCORE_MIN_THRESHOLD:
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
            "score": score,
            "score_label": label,
            "score_reason": reason,
            "annotation": "",
            "llm_explanation": "",
        }

        append_row(row)

        if LOG_STDOUT:
            log(describe_change(friendly, entity_id, prev_val, new_val, unit, score, label, reason))

        for rule in AUTOMATIONS:
            kw = rule.get("trigger_keyword", "").lower()
            ts = rule.get("trigger_state", "").lower()
            if kw and kw in entity_id.lower() and ts and new_val.lower() == ts:
                svc = rule.get("action_service", "")
                if svc:
                    log(f"Automation triggered: '{kw}'='{ts}' → {svc}")
                    asyncio.create_task(_call_ha_service(
                        svc,
                        rule.get("action_target", ""),
                        rule.get("action_data", ""),
                    ))

        if OLLAMA_ENABLED and score >= OLLAMA_SCORE_THRESHOLD:
            asyncio.create_task(_ollama_background_task(
                entity_id, friendly, domain, prev_val, new_val, unit, score, label, reason, timestamp
            ))


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
