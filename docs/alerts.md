# AEMO Alerts Catalogue

Single source of truth for every alert that fires across the AEMO production
systems. Cross-referenced by both producers (collector, dashboard, gauge) and
consumers (admin SMS/email, iOS Nem Analyst app).

**Producer projects:**

- `aemo-data-updater` (collector, runs on `.71`)
- `aemo-energy-dashboard2` (dashboard + API server, runs on `.71`)
- `standalone_renewable_gauge_with_alerts_updated.py` (separate gauge process)
- `battery_monitor.py` (separate 5-min-polling daemon)
- `outage_monitor` (detection-only, no alerting yet)

**Consumer surfaces:**

- `sms` — Twilio to a single admin phone (`MY_PHONE_NUMBER` / `ALERT_PHONE_NUMBER`)
- `email` — SMTP to admin (`RECIPIENT_EMAIL`)
- `push` — APNs to iPhone via the Nem Analyst app (**not yet wired**)
- `badge` — app-icon badge count on Nem Analyst (**not yet wired**)
- `inline` — in-app banner / list, no notification (**not yet wired**)
- `log` — file log only, no surfacing

## Severity definitions

| Severity | Meaning | Default channels |
|---|---|---|
| **critical** | Market-significant event a person would interrupt their day for | sms + push + badge |
| **warning** | Operational issue (data freshness, new entity to classify) | email + inline |
| **info** | Notable but not urgent (records, milestones) | push (opt-in) + badge + inline |

---

## Master list

Alerts are stable kebab-case IDs. The iOS column is the *recommended* surface
for the Nem Analyst app once the push pipeline is built; current state for all
of them is "not surfaced on iOS".

### Critical

#### `spot-price-high-breach`

| Field | Value |
|---|---|
| Trigger | `price >= HIGH_THRESHOLD` (default `$1000/MWh`) per region, with stateful "armed" flag so each breach fires once |
| Producer | **DUPLICATED** in both `aemo-data-updater/src/aemo_updater/collectors/twilio_price_alerts.py:134-151` and `aemo-energy-dashboard2/src/aemo_dashboard/spot_prices/twilio_price_alerts.py:139-156` |
| State file | `/Users/davidleitch/aemo_production/data/price_alert_state.pkl` |
| Current channels | sms |
| Recommended iOS | **push + badge** — the headline alert. Tap-through to Prices → Spot for that region. |
| Suggested copy | "⚠️ NSW1 spot $1,420 — breached \$1k/MWh" |

#### `spot-price-extreme-spike`

| Field | Value |
|---|---|
| Trigger | `price >= EXTREME_THRESHOLD` (default `$10,000/MWh`) |
| Producer | Same files as `spot-price-high-breach` (lines 141-146) |
| State file | Same |
| Current channels | sms (with escalated emoji `🚨🚨🚨`) |
| Recommended iOS | **push + badge** with distinctive sound. Always raise even if user has muted normal price alerts. |
| Suggested copy | "🚨 SA1 spot $14,200 — extreme spike" |

### Warning

#### `new-duid-detected`

| Field | Value |
|---|---|
| Trigger | DUID appears in AEMO dispatch but isn't in `gen_info.pkl` / known-DUID set; per-DUID 24h cooldown |
| Producer | **DUPLICATED** in `aemo-data-updater/src/aemo_updater/collectors/unified_collector.py:246,250-292` and `aemo-energy-dashboard2/src/aemo_dashboard/generation/gen_dash.py:466-471,522-555` |
| State file | `/Volumes/davidleitch/aemo_production/data/unknown_duids_alerts.json` |
| Current channels | email (admin only) |
| Recommended iOS | **inline** in a "What's new in the market" feed. Not a push — a power-industry tester finds it interesting but not interruption-worthy. |

#### `data-file-stale`

| Field | Value |
|---|---|
| Trigger | Parquet file unmodified > 30 min (configurable per data type); escalates to ERROR if > 60 min |
| Producer | `aemo-data-updater/src/aemo_updater/alerts/alert_manager.py:159-205` |
| Current channels | email (always); sms only if escalated to ERROR |
| Recommended iOS | **none** — admin operational alert, not a tester-facing event. The app should already surface staleness via the existing freshness meta on each chart. |

#### `data-file-missing`

| Field | Value |
|---|---|
| Trigger | Expected parquet file does not exist |
| Producer | Same alert_manager.py (lines 177-184) |
| Current channels | email + sms |
| Recommended iOS | **none** — admin only |

### Info

The five record-tracking alerts below all live in
`aemo-energy-dashboard2/standalone_renewable_gauge_with_alerts_updated.py`,
share `/Volumes/davidleitch/aemo_production/data/renewable_records.json` for
state, and currently dispatch via Twilio SMS to `ALERT_PHONE_NUMBER`. None are
visible on iOS today.

| Alert ID | Trigger | Emoji | Recommended iOS |
|---|---|---|---|
| `renewable-record-percentage` | All-time NEM renewable % surpassed | 🌱 | **push (opt-in) + badge + inline** |
| `wind-record-mw` | New all-time NEM wind MW peak | 🌬️ | **push (opt-in) + badge + inline** |
| `solar-record-mw` | New all-time NEM utility solar MW peak | ☀️ | **push (opt-in) + badge + inline** |
| `hydro-record-mw` | New all-time NEM hydro MW peak | 💧 | **push (opt-in) + badge + inline** (hydro records are rare; valuable when they happen) |
| `rooftop-solar-record-mw` | New all-time NEM rooftop MW peak | 🏠 | **push (opt-in) + badge + inline** |

The iOS app should let testers toggle each record class individually under
Settings → Notifications. Default to all-on; testers can mute the spammy ones
(solar peak fires often in summer).

### Battery records (info)

All produced by `/Users/davidleitch/aemo_production/battery_monitor.py` —
a standalone daemon polling `bdu5` every 5 minutes
(`POLL_INTERVAL_SECONDS = 300`). Records state lives in
`/Users/davidleitch/aemo_production/data/battery_records.json` (active,
last-updated daily). Currently dispatches via Twilio SMS to
`MY_PHONE_NUMBER` only.

Three record metrics × four regions × NEM-wide = **15 record alerts**:

| Metric | Per-region IDs | NEM-wide ID |
|---|---|---|
| Peak state of charge (MWh) | `battery-soc-record-{nsw1,qld1,vic1,sa1}` | `battery-soc-record-nem` |
| Peak discharge (MW) | `battery-discharge-record-{nsw1,qld1,vic1,sa1}` | `battery-discharge-record-nem` |
| Peak charge (MW) | `battery-charge-record-{nsw1,qld1,vic1,sa1}` | `battery-charge-record-nem` |

| Field | Value |
|---|---|
| Trigger | Per scope/metric: current 5-min reading > stored max; record gets updated and SMS fires (single message per breach, no rate-limiting beyond "must beat current record") |
| Producer | `battery_monitor.py:264-289` (`check_records`); SMS in `:325-349` (`send_record_alert`) |
| State file | `/Users/davidleitch/aemo_production/data/battery_records.json` |
| Current channels | sms |
| Recommended iOS | **push (opt-in) + badge + inline** — same per-class toggle pattern as the renewable records. Default the NEM-wide records on (3 alerts, low-frequency, market-significant); default per-region records off (12 alerts, more frequent). |
| Suggested copy | "🔋 NEM new battery discharge record: 3,641 MW (prev 3,580)" |

The producer is structurally cleaner than the renewable-gauge equivalent
— `default_records()` factory, `seed_records` backfill mode, dataclass-
shaped JSON. Worth using as a template if/when the renewable-records
producer is refactored.

### Battery low-SOC alerts (warning)

Same producer (`battery_monitor.py`).

| Alert ID | Trigger | Recovery |
|---|---|---|
| `battery-low-soc-{nsw1,qld1,vic1,sa1}` | Region SOC drops below `LOW_SOC_TRIGGER_PCT = 5.0%` while `low_soc_alerts[region].active == false` | When SOC rises above `LOW_SOC_RECOVER_PCT = 15.0%`, sends a recovery SMS and clears the active flag |

| Field | Value |
|---|---|
| Producer | `battery_monitor.py:291-322` (`check_low_soc`); SMS in `:352-379` (`send_low_soc_alert`) |
| State file | Same JSON, under `low_soc_alerts.{region}` |
| Current channels | sms (both trigger and recovery) |
| Recommended iOS | **push + badge** — same tier as the price breach (low SOC across a region during evening peak is market-significant) |
| Notes | As of 7 May, **QLD1 is currently in active low-SOC state** (triggered 7 May 07:25 NEM); NSW1/VIC1/SA1 not active. The hysteresis (5% / 15%) prevents flapping. |

### Detection-only (no alerts today)

The outage monitor has the change-detection infrastructure plumbed in but
nothing currently dispatches the events. They live in
`outage_changes.parquet`. Listed here so the next pass can wire alerting once
priorities are agreed.

| Alert ID | Trigger |
|---|---|
| `outage-new-detected` | New ST-PASA / MT-PASA / High-Impact outage appears |
| `outage-extended` | Existing outage extended by > 1 day |
| `outage-cancelled` | Outage withdrawn |
| `outage-capacity-changed` | Outage capacity changed by > 100 MW |

Source: `outage_monitor/change_detector.py:32-40` (`ChangeType` enum).

---

## Outstanding issues

### Duplication

- **Price alerts** fire from BOTH `aemo-data-updater` and
  `aemo-energy-dashboard2`. Each maintains its own pickle of
  per-region "armed" state, so one breach raises two SMS messages. **Pick a
  single owner before iOS push is added** or every event will deliver in
  triplicate (sms × 2 + push). Recommendation: collector owns it (closer to
  the raw data); delete the dashboard copy.
- **`new-duid-detected`** is implemented twice with separate rate-limit caches.
  Same recommendation: collector wins.

### Missing — opportunity list

- **NEM demand records** — peak / minimum demand records (nothing today)
- **Renewable-share intra-day record** — current dashboard tracks all-time
  but not intra-day-since-midnight
- **Negative-price duration records** — sustained negative-price runs
- **Battery revenue record (daily)** — daily fleet revenue all-time high
  (would need the prices × dispatch join from `/v1/batteries/overview`)

---

## How to add a new alert

1. Pick a kebab-case ID and add a row to the master list in this file under
   the appropriate severity.
2. Implement the producer in the owning project (collector for market-data
   triggers; gauge for fuel records; outage_monitor for outage events).
3. Write to a JSON state file under `/Users/davidleitch/aemo_production/data/`
   following the `renewable_records.json` shape.
4. For iOS surfacing, add the ID to the iOS app's `AlertCategory` enum and
   wire the APNs payload. (See companion doc when written: TBD —
   cross-system docs overview.)

## See also

- [`alerts_plugin_architecture.md`](alerts_plugin_architecture.md) — the
  *how* (Plugin / Dispatcher / Sink design that consolidates the three
  current daemons and prepares the iOS push hook).
- A higher-level cross-system documentation overview covering the dashboard
  and the iOS app is planned. This catalogue will move into / be referenced
  from that overview when it lands.
- `aemo-energy-dashboard2/src/aemo_dashboard/api/CLAUDE.md` — API server
  contracts.
- `~/.claude/projects/.../memory/aemo_ios_app.md` — iOS app state +
  next-steps tracker.
