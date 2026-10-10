# NEM dashboard and collector — system map

How the system runs now. Rewrite the affected section whenever something changes; do not append. History and reasoning live in `SESSIONS.md`; rules that must not break live in tests. Last verified against the machine: 8 Oct 2026.

## Machine and code

Everything runs on daves_mini (.71, `ssh davidleitch@192.168.68.71`), in tmux session `services`.

| Checkout | Remote | What runs from it |
|---|---|---|
| `~/aemo-redesign` (branch `web-dashboard-redesign`) | `davidleitch1/aemo-energy-dashboard2` | `src/aemo_dashboard/web/app.py` only — the live web app file |
| `~/aemo_production/aemo-energy-dashboard2` (branch `main`) | same repo, different branch | every `aemo_dashboard.*` module the web app imports (evening_peak, penetration, generation_comparison, shared/...), the iOS API, the standalone gauge, spot prices |
| `~/aemo_production/aemo-data-updater` | own repo | the collector, alert plugins, gap repair, weekly integrity, duid_mapping refresh |
| `~/aemo_production/outage_monitor` (+ entry `~/aemo_production/run_outage_monitor.py`) | local git only, no remote (since 10 Oct 2026) | PASA / High Impact Outages collector feeding the PASA tab |

**Import trap.** The dashboard2 venv has `aemo_dashboard` installed editable from `aemo-energy-dashboard2/src`, so `app.py` in aemo-redesign imports its helper modules from the *other* checkout. A fix to a shared module goes in `aemo-energy-dashboard2`; a fix to `app.py` goes in `aemo-redesign`. `shared/fuel_categories.py` exists in both and must be kept identical.

Python for everything dashboard-side: `~/aemo_production/aemo-energy-dashboard2/.venv/bin/python`. Collector side: `~/aemo_production/aemo-data-updater/.venv/bin/python`. Env files: `.env` in each of those two checkouts (SMTP, Twilio, `AEMO_DUCKDB_PATH`).

## Services (tmux `services`)

| Win | Name | Port | Runs | DB |
|---|---|---|---|---|
| 0 | collector | — | `python -m aemo_updater.collectors.unified_collector_duckdb` in aemo-data-updater | writes `aemo_test.duckdb` |
| 1 | itk-dashboard | 5008 | `uvicorn app:app --workers 4` from `aemo-redesign/src/aemo_dashboard/web` | readonly (hard-coded `DB_PATH` in app.py) |
| 2 | renewable-gauge | 5009 | `renewable_gauge_stacked.py` in dashboard2 | readonly |
| 3 | spot-prices | 8081 | Panel app in dashboard2 | readonly |
| 4 | iea-global | 5202 | `iea_dashboard/run.sh` | own data |
| 5 | isso-comments | 8080 | Isso | — |
| 7 | outage-monitor | — | `run_outage_monitor.py` | — |
| 9 | battery-monitor | — | `battery_monitor.py` (records, low-SOC alerts) | readonly |
| 10 | cloudflared | — | tunnel for all public hostnames | — |
| 11 | nemgenapi | 8002 (localhost) | iOS app API, `PYTHONPATH=src uvicorn aemo_dashboard.api.main:app` in dashboard2 | readonly (default in `api/db.py`) |
| 12 | demand-forecast | 5201 | aemo-demand-forecast | — |
| 13 | nembids | 8095 | `~/nem_bids_app` | `bids_readonly.duckdb` + readonly |
| 14 | intl-energy | 8098 | `~/intl_energy` | own data |

Windows 6 (status text) and 8 (karen gallery) are not NEM services.

**Launch lines live in two scripts and must match:** `~/tmux_files/start_services.sh` (boot, via `@reboot` cron) and `~/tmux_files/restart_dashboards.sh` (nightly 00:15; restarts windows 1–4 only). Process signatures for the half-hourly health check are in `~/tmux_files/monitor_services.py`. No uvicorn runs with `--reload`: a code change needs `C-c` in the window and the launch line re-sent.

**Deploying a web change:** run tests (`python -m pytest tests/web -q` in aemo-redesign), start a second uvicorn from the same directory on port 5099, check it, kill it, restart window 1. For a dashboard2 module change, restart window 1 *and* window 11 (and 2/3 if they use it).

## Data flow

AEMO NEMweb → collector (every 4.5 min) → `~/aemo_production/data/aemo_test.duckdb` → at the end of each cycle the collector copies the whole file to `aemo_readonly.duckdb` (copy + atomic rename) → every app reads the readonly copy.

- **Never write to `aemo_readonly.duckdb`.** The next cycle overwrites it. Edits (including `duid_mapping` and view definitions, which live in the DB) go into `aemo_test.duckdb`, between cycles: the collector holds an exclusive lock during a cycle, so writers retry `duckdb.connect` on `IOException`.
- No app may open `aemo_test.duckdb`; it contends with the collector's lock.
- Bids go to a separate `bids.duckdb` with its own `bids_readonly.duckdb` copy.

## Tables that matter and where their truth comes from

| Table | Grain | Notes |
|---|---|---|
| `scada5`, `scada30` | per DUID | Generation MW. Battery BDUs are signed (charge negative). **Pump loads report consumption as positive MW.** |
| `prices5`, `prices30` | per region | `prices30` holds raw 5-min rows Oct-2021 → ~Jun-2024; resample before any frequency statistic in that window. |
| `demand30` | per region | `demand` = AEMO `OPERATIONAL_DEMAND` (excludes grid battery charging and pump load; includes battery discharge as supply). `demand_less_snsg` alongside. |
| `rooftop30` | per region | Holds sub-regions QLDC/QLDN/QLDS/TASN/TASS before 2026: always filter to the five parent regions. |
| `bdu5` | per region | Battery charge/discharge and stored energy. |
| `duid_mapping` | per DUID | region, site name, owner, capacity_mw, storage_mwh, fuel. **Exists only in the DB** — no file behind it. |

Generation views (`generation_by_fuel_5min/30min`, `duid_info`, ...) are joins of scada × `duid_mapping` and drop rows whose fuel is NULL.

**`duid_mapping` maintenance.**
- New DUIDs: the collector inserts a guessed row on first sight (fuel from the DUID string, blank region, 0 MW) and never retries a low-confidence guess.
- Weekly fix-up: `scripts/refresh_duid_mapping.py` (cron Sunday 05:10) pulls the AEMO Registration and Exemption List and MMSDM `DUDETAIL`/`DUDETAILSUMMARY`, inserts active unmapped DUIDs, fills blank fields, and emails conflicts without overwriting them. History and edit logs: `~/aemo_production/data/duid_mapping_history/`. Pre-audit copy of the table: `duid_mapping_bak_20261008`.
- Conventions: non-battery MW = Gen Info AC nameplate (else registered capacity); battery MW = registered max capacity; storage = registered max storage; region from MMS (the registration list has errors).
- **Pump loads** (PUMP1, PUMP2 = Wivenhoe; SNOWYP = Tumut 3; SHPUMP = Shoalhaven; KIDSPHL1/2 = Kidston) carry fuel NULL so they are not counted as hydro. Queries that join `duid_mapping` directly must keep `fuel IS NOT NULL`. `fuel_categories.PUMPED_HYDRO_DUIDS` lists the six pumped-storage *generators*; no live code applies it.
- `~/aemo_production/data/duid_exceptions.json` only silences new-DUID alert emails.

Other hand-maintained data: `~/aemo_production/data/futures.csv` (merge NEM-Review exports with `update_futures.py`; the launchd job `com.aemo.update-futures` is dead).

## Scheduled jobs (crontab on .71)

| When | Job |
|---|---|
| @reboot | `start_services.sh` |
| 00:15 daily | `restart_dashboards.sh` (windows 1–4) |
| :30 hourly | `monitor_services.py` (service health) |
| every 30 min | `itk-infrastructure/clients/ops/run_job.sh health` |
| 11:00 daily | STTM gas prices |
| 12:00 / 19:00 or 19:30 | midday / evening reports; `itk_mail send_daily.py` sends them 5 min later |
| Sun 03:00 | IEA dashboard backup |
| Sun 04:17 | `weekly_integrity.py` (gap survey + repair, email) |
| Sun 05:10 | `refresh_duid_mapping.py` (email) |
| Mon 06:40, 09:30, 18:00 | intl_energy collection |

**PASA data (outage monitor, tmux window 7).** Separate from the DuckDB pipeline; writes parquet in `~/aemo_production/data/`:
- `outages_stpasa.parquet` — latest ST-PASA run per (DUID, half-hour), about 7 days of runs kept.
- `outages_pdpasa.parquet` — PD-PASA DUID availability, latest run only, refreshed every 30 min; supplies the "now" interval (ST-PASA starts at the next trading day).
- `outages_mtpasa.parquet` — latest MT-PASA publish per (DUID, DAY), days from today on. AEMO publishes about 4 times a day; a publish starts about 2 days after its publish date.
- `outages_mtpasa_history.parquet` — MT-PASA revision history, change-compressed: one row per (DUID, DAY) only when availability or unit state changes. Rebuild a view at any publish with `outage_monitor/mtpasa_history.py:mtpasa_asof`. Backfilled from the last publish of each day since 1 Oct 2025 (`outage_monitor/scripts/backfill_mtpasa_history.py`; NEMweb Current keeps every MT-PASA file since Aug 2020).
- `outages_high_impact.parquet` — AEMO High Impact Outages (transmission), weekly.

The Mac Studio watchdog checks .71 every 30 min and SMSes on failure (see memory note on the watchdog).

## Dashboard definitions worth knowing

- Battery tab: Cap MW = nameplate (registered max) MW. Util % = discharge ÷ (storage × days in window) — 100% means one full cycle a day. Util % and $/MWh-cap/yr weight each DUID by the hours it reported in the window.
- Battery figures count discharge only as "generation"; battery and transmission are excluded from renewable-share denominators.
- NEM prices are demand-weighted across regions.
- PASA tab (`web/pasa_data.py`, `web/pasa_transmission.py`): scheduled fuels only (Coal, CCGT, OCGT, Gas other, Water). MW out = `duid_mapping.capacity_mw` − ST-PASA *PASA availability* (not MAXAVAIL, which counts recallable economic shutdowns); a unit is out at ≥ 50 MW. MT-PASA outage days = unit state OUTAGE*/DERATING* with ≥ 50 MW out; Planned/Unplanned from that state; MOTHBALLED and RETIRED are footnoted, not counted. Return-date changes compare the latest publish with the last publish of each of the previous 25 weeks; "before" dates mark outages already present at the start of the window (slip is then a lower bound); open-ended = runs to within 7 days of the MT-PASA horizon.
