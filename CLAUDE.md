# CLAUDE.md

## Project overview
HA Monitor is a Home Assistant add-on that listens to `state_changed` events over the Home Assistant WebSocket API and writes state transitions to daily CSV files.

## Purpose
- Monitor selected Home Assistant entity domains (for example: `sensor`, `binary_sensor`, `switch`)
- Persist meaningful state transitions in `/share/ha_monitor` (by default)
- Produce CSV output that is easy to export and annotate

## Runtime architecture
1. `run.sh` reads add-on options via `bashio`, exports environment variables, and starts Python.
2. `monitor.py` connects to `ws://supervisor/core/websocket`.
3. The monitor authenticates with `SUPERVISOR_TOKEN` (mapped to `HA_TOKEN`).
4. It subscribes to `state_changed` events and filters by configured domains.
5. Real state changes are appended to a daily-rotating CSV (`ha_monitor_YYYY-MM-DD.csv`).

## Key files
- `monitor.py` — main event loop, filtering, CSV writing, reconnect logic
- `run.sh` — entrypoint for Home Assistant add-on runtime
- `config.yaml` — add-on metadata, options, and schema
- `Dockerfile` — image build and runtime setup
- `build.yaml` — architecture-specific base images
- `repository.yaml` — add-on repository metadata

## CSV schema
Columns written by `monitor.py`:
- `timestamp`
- `entity_id`
- `friendly_name`
- `domain`
- `previous_state`
- `new_state`
- `unit`
- `score` — significance score 0–10 assigned by the scoring engine
- `score_label` — human-readable tier (Negligible / Low / Moderate / Significant / High / Critical)
- `score_reason` — one-line explanation of how the score was derived
- `annotation` (intentionally left empty for manual annotation later)

## Scoring engine
`score_change()` in `monitor.py` assigns scores based on domain, entity keywords, and numeric delta:

| Score | Label       | Examples                                              |
|-------|-------------|-------------------------------------------------------|
| 0     | Negligible  | Temperature delta < 0.5 °C, humidity delta < 1 %     |
| 1–2   | Low         | Small temperature / humidity / illuminance changes    |
| 3–4   | Moderate    | Generic numeric change, media player, input_select    |
| 5–6   | Significant | Light/switch on-off, binary sensor, cover, window     |
| 7–8   | High        | Lock secured, climate mode, door opened, motion clear |
| 9–10  | Critical    | Motion detected, presence, lock opened, alarm, smoke  |

Rules are keyword-based (entity_id) + domain-based + unit-based. First matching rule wins.

## Configuration
Configured in `config.yaml` options:
- `domains` (list of domains to monitor)
- `output_dir` (default: `/share/ha_monitor`)
- `log_to_stdout` (boolean)
- `score_min_threshold` (integer 0–10, default `0`; events with score below this are silently dropped)

Environment variables consumed by `monitor.py`:
- `HA_TOKEN`
- `MONITOR_DOMAINS`
- `MONITOR_OUTPUT_DIR`
- `MONITOR_LOG_STDOUT`
- `MONITOR_SCORE_MIN_THRESHOLD`

## Implementation notes
- Ignore attribute-only updates (`prev_val == new_val`).
- Friendly name and unit should prefer values from `new_state`.
- CSV rotation is by UTC date.
- Reconnection uses exponential backoff up to 120 seconds.

## Guidance for future changes
- Keep add-on options and Python env var handling in sync (`config.yaml` ↔ `run.sh` ↔ `monitor.py`).
- Preserve CSV column compatibility unless a migration plan is explicitly introduced.
- Avoid logging secrets/tokens.
- Keep Home Assistant API behavior defensive (unexpected WS messages, malformed payloads).

## Validation checklist
After changes:
1. Confirm Python syntax is valid.
2. Confirm add-on config schema still matches options.
3. If CSV behavior changed, verify header + appended rows format remains correct.
