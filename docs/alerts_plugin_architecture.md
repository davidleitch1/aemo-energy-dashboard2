# Alerts Plugin Architecture

Companion to [`alerts.md`](alerts.md). That document is the *catalogue*
(what alerts exist); this document is the *architecture* (how they're
produced and dispatched).

**Goal**: collapse three separate alert daemons (`battery_monitor.py`,
`standalone_renewable_gauge_with_alerts_updated.py`, plus the alert
paths inside `unified_collector_duckdb.py`) into a single plugin layer
inside the collector, and at the same time prepare a clean hook for
iOS push notifications without rewriting existing producer logic.

**Constraint**: keep behaviour byte-identical for the existing SMS +
email channels through the migration. iOS push is added afterwards as a
new sink, not as a change to producer code.

---

## 1. Status quo and what's wrong with it

| Daemon | What it does | Channels |
|---|---|---|
| `unified_collector_duckdb.py` | Collects AEMO files into DuckDB; raises new-DUID + price alerts inline | email, sms |
| `battery_monitor.py` | Polls `bdu5` every 5 min; raises 15 record alerts + 8 low-SOC | sms |
| `standalone_renewable_gauge_with_alerts_updated.py` | Polls DuckDB; raises 5 renewable-record alerts | sms |
| `outage_monitor` | Detects outage changes; **no alerting wired** | (log only) |

Three problems:

1. **Three poll loops**, three places to babysit, three places where
   "did it just crash silently?" is a separate question.
2. **State and credentials scattered**. Each daemon has its own `.env`
   discovery, its own JSON pickles, its own SMS path.
3. **Adding iOS push means three new APNs hooks**, not one. Inevitably
   they'll drift.

We already fixed the worst symptom (duplicate price-alert SMS firing
twice; see commit `e6774a3`). The next pass is structural.

---

## 2. Target architecture

One process: **the collector**. After each successful merge cycle, it
hands control to an `AlertDispatcher` that runs every registered
plugin against the freshly-merged data and fans the resulting alerts
out to whichever sinks the routing table specifies.

```
┌──────────────────────────────── unified_collector_duckdb.py ───────────────────────────────┐
│                                                                                            │
│  poll AEMO ─→ parse files ─→ merge to DuckDB ─→ AlertDispatcher.run_cycle(ctx)             │
│                                                          │                                 │
│                          ┌───────────────────────────────┴───────────────────────────┐     │
│                          ▼                                                           ▼     │
│                                                                                            │
│                ┌─────── plugins ───────┐              ┌──────── sinks ────────┐            │
│                │ price_breach          │              │ TwilioSmsSink         │            │
│                │ new_duid              │              │ SmtpEmailSink         │            │
│                │ battery_records       │              │ ApnsPushSink (Phase B)│            │
│                │ battery_low_soc       │              │ LogSink               │            │
│                │ renewable_records     │              └────────┬──────────────┘            │
│                │ data_freshness        │                       │                           │
│                │ outage_changes ★      │                       │                           │
│                └────────┬──────────────┘                       │                           │
│                         │ list[Alert]                          │                           │
│                         ▼                                      │                           │
│                ┌──── routing.py ──────────────┐                │                           │
│                │ alert_id → [sink_name, ...]  │────────────────┘                           │
│                └──────────────────────────────┘                                            │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘

★ outage_changes is currently detection-only — wired here on first pass.
```

### Three roles, three contracts

| Role | Responsibility | Contract |
|---|---|---|
| **Plugin** | Inspect data, decide *whether* to raise alerts. Owns its own dedup state. Pure: returns `list[Alert]`. | `evaluate(ctx) -> list[Alert]` |
| **Dispatcher** | Iterate plugins, catch exceptions, look up sinks via routing table, hand each alert to each sink. | `run_cycle(ctx)` |
| **Sink** | Deliver an alert via one channel. SMS, email, APNs, log. Stateless (or owns delivery state only). | `emit(alert) -> None` |

Plugins know nothing about how alerts are delivered. Sinks know nothing
about why an alert was raised. The routing table is the only place
that knows both — and it lives next to (and is generated from) the
catalogue in `alerts.md`.

---

## 3. The Alert + AlertContext data shapes

Extend the existing `aemo_updater/alerts/base_alert.py` `Alert`
dataclass minimally:

```python
@dataclass
class Alert:
    # existing fields
    title: str
    message: str
    severity: AlertSeverity
    source: str
    timestamp: datetime
    metadata: dict[str, Any]
    # new fields
    id: str              # kebab-case ID matching alerts.md catalogue
                         # e.g. 'spot-price-high-breach', 'wind-record-mw'
    dedup_key: str       # e.g. 'price-NSW1' for armed-state, or
                         # 'wind-record' for last-record-fired
```

`AlertSeverity` and `format_for_sms` / `format_for_email` stay as-is.
We'll add `format_for_apns()` returning a JSON dict suitable for the
APNs HTTP/2 body in Phase B.

```python
@dataclass
class AlertContext:
    """Everything a plugin needs to evaluate. Constructed once per cycle
    by the dispatcher and shared across plugins."""
    db_path: str           # DuckDB path (per-plugin connection)
    data_dir: Path         # State files live here
    now: datetime          # UTC; consistent across all plugins in a cycle
    last_run_at: datetime  # When the dispatcher last ran (for "what's new")
    nem_now: datetime      # NEM-naive time (UTC+10 / +11 for DST)
```

A plugin opens its own short-lived DuckDB connection from `db_path`
(per-request pattern, ~1ms; matches the API server's habit). It reads
its rate-limit state from a JSON file under `data_dir/<plugin>.json`.

---

## 4. The Plugin interface

```python
class AlertPlugin(Protocol):
    """One alert-detection unit. Stateless except for the state file
    it owns under `ctx.data_dir`. Idempotent — safe to call multiple
    times in the same cycle if the dispatcher needs to retry."""

    name: str              # 'battery_records'  (used for state file + logs)
    severity: AlertSeverity # default severity for alerts this plugin emits
                            # (individual Alerts can override)
    poll_interval_s: int   # 300 for most; let the dispatcher skip if
                            # cycle is more frequent than this

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        """Inspect data via ctx.db_path, decide whether to raise alerts.
        Plugin-internal dedup happens here — the returned list contains
        only alerts that should fire NOW. The dispatcher emits each to
        all routed sinks."""
```

**Concrete example: `BatteryRecordsPlugin`** (replacing
`battery_monitor.py:check_records`):

```python
class BatteryRecordsPlugin:
    name = 'battery_records'
    severity = AlertSeverity.INFO
    poll_interval_s = 300

    METRICS = ['soc_mwh', 'discharge_mw', 'charge_mw']
    SCOPES = ['nem', 'NSW1', 'QLD1', 'VIC1', 'SA1']

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        records = self._load_state(ctx.data_dir)
        current = self._query_latest(ctx.db_path)
        alerts: list[Alert] = []
        for scope in self.SCOPES:
            for metric in self.METRICS:
                cur_val = current[scope][metric]
                rec = records[scope][metric]
                if cur_val > rec['value']:
                    alerts.append(Alert(
                        id=f'battery-{metric.split("_")[0]}-record-{scope.lower()}',
                        title=f'New {metric} record for {scope}',
                        message=f'{cur_val:.1f} (prev {rec["value"]:.1f})',
                        severity=self.severity,
                        source='battery_records',
                        timestamp=ctx.now,
                        metadata={'scope': scope, 'metric': metric,
                                  'old_value': rec['value'], 'new_value': cur_val},
                        dedup_key=f'battery-{metric}-{scope}',
                    ))
                    rec['value'] = cur_val
                    rec['timestamp'] = ctx.nem_now.isoformat()
        if alerts:
            self._save_state(ctx.data_dir, records)
        return alerts
```

Same JSON state file (`data_dir/battery_records.json`), same shape, no
data migration. The standalone `battery_monitor.py` daemon goes away.

---

## 5. The Sink interface

```python
class AlertSink(Protocol):
    name: str    # 'sms', 'email', 'apns', 'log'
    enabled: bool

    def emit(self, alert: Alert) -> None:
        """Deliver this alert via this channel. Failures are logged but
        not re-raised — one sink failing must not block other sinks."""
```

Concrete sinks (each ~50 lines):

| Sink | Adapts | Source |
|---|---|---|
| `LogSink` | logger.warning() | new — always-on, ~10 lines |
| `SmtpEmailSink` | `smtplib` + existing `email_sender.py` | rename of `aemo_updater/alerts/email_sender.py` |
| `TwilioSmsSink` | Twilio REST API | extracted from `collectors/twilio_price_alerts.py` |
| `ApnsPushSink` | APNs HTTP/2 + JWT | NEW (Phase B) |

Each sink reads its own credentials from env on init:

```python
class TwilioSmsSink:
    def __init__(self):
        self.client = twilio.Client(
            os.environ['TWILIO_ACCOUNT_SID'],
            os.environ['TWILIO_AUTH_TOKEN'],
        )
        self.from_number = os.environ['TWILIO_FROM_NUMBER']
        self.to_number = os.environ['MY_PHONE_NUMBER']
        self.enabled = bool(self.client and self.from_number and self.to_number)

    def emit(self, alert: Alert) -> None:
        if not self.enabled:
            return
        try:
            self.client.messages.create(
                from_=self.from_number, to=self.to_number,
                body=alert.format_for_sms(),
            )
        except Exception:
            logger.exception('TwilioSmsSink.emit failed')
```

---

## 6. The routing table

Single source of truth for "which alerts go where":

```python
# aemo_updater/alerts/routing.py
ALERT_ROUTING: dict[str, list[str]] = {
    # Critical — every channel + iOS push (Phase B)
    'spot-price-high-breach':         ['sms', 'apns', 'log'],
    'spot-price-extreme-spike':       ['sms', 'apns', 'log'],

    # Warning — admin email, plus iOS for new-DUID
    'new-duid-detected':              ['email', 'apns', 'log'],
    'data-file-stale':                ['email', 'log'],
    'data-file-missing':              ['email', 'sms', 'log'],

    # Info — record alerts. Renewable goes to iOS per scope decision;
    # battery stays SMS-only (per user's iOS scope choice 7 May 2026).
    'renewable-record-percentage':    ['sms', 'apns', 'log'],
    'wind-record-mw':                 ['sms', 'apns', 'log'],
    'solar-record-mw':                ['sms', 'apns', 'log'],
    'hydro-record-mw':                ['sms', 'apns', 'log'],
    'rooftop-solar-record-mw':        ['sms', 'apns', 'log'],

    # Battery records: 15 IDs, all sms+log only
    **{f'battery-soc-record-{r}':       ['sms', 'log']
       for r in ['nem', 'nsw1', 'qld1', 'vic1', 'sa1']},
    **{f'battery-discharge-record-{r}': ['sms', 'log']
       for r in ['nem', 'nsw1', 'qld1', 'vic1', 'sa1']},
    **{f'battery-charge-record-{r}':    ['sms', 'log']
       for r in ['nem', 'nsw1', 'qld1', 'vic1', 'sa1']},
    **{f'battery-low-soc-{r}':          ['sms', 'log']
       for r in ['nsw1', 'qld1', 'vic1', 'sa1']},
}

DEFAULT_SINKS = ['log']  # for any alert id not in the table
```

Generated from the catalogue table in `alerts.md` — when an alert is
added there, this dict gets an entry.

---

## 7. The Dispatcher

```python
class AlertDispatcher:
    def __init__(self, plugins: list[AlertPlugin],
                 sinks: dict[str, AlertSink],
                 routing: dict[str, list[str]] = ALERT_ROUTING):
        self.plugins = plugins
        self.sinks = sinks
        self.routing = routing

    def run_cycle(self, db_path: str, data_dir: Path) -> None:
        """Called by the collector after each successful merge cycle.
        Each plugin runs in isolation — one crashing does not block
        others."""
        ctx = AlertContext(
            db_path=db_path,
            data_dir=data_dir,
            now=datetime.now(timezone.utc),
            last_run_at=self._last_run or datetime.now(timezone.utc),
            nem_now=datetime.now(NEM_TZ).replace(tzinfo=None),
        )
        for plugin in self.plugins:
            try:
                alerts = plugin.evaluate(ctx)
            except Exception:
                logger.exception(f'plugin {plugin.name} failed')
                continue
            for alert in alerts:
                for sink_name in self.routing.get(alert.id, DEFAULT_SINKS):
                    sink = self.sinks.get(sink_name)
                    if not sink or not sink.enabled:
                        continue
                    try:
                        sink.emit(alert)
                    except Exception:
                        logger.exception(f'sink {sink_name} failed for {alert.id}')
        self._last_run = ctx.now
```

The dispatcher is ~60 lines, never grows. All complexity goes into
plugins (one per alert source) or sinks (one per channel).

---

## 8. File layout in `aemo-data-updater`

```
src/aemo_updater/alerts/
├── __init__.py            # re-exports Alert, AlertSeverity, AlertContext, AlertDispatcher
├── base_alert.py          # extend Alert with id + dedup_key (existing)
├── context.py             # NEW — AlertContext dataclass
├── dispatcher.py          # NEW — AlertDispatcher
├── routing.py             # NEW — ALERT_ROUTING dict
├── plugins/
│   ├── __init__.py
│   ├── price_breach.py    # was collectors/twilio_price_alerts.py
│   ├── new_duid.py        # was collectors/unified_collector.py:246-292
│   ├── battery_records.py # was /aemo_production/battery_monitor.py
│   ├── battery_low_soc.py # was /aemo_production/battery_monitor.py
│   ├── renewable_records.py  # was standalone_renewable_gauge_with_alerts_updated.py
│   ├── data_freshness.py  # was alert_manager.py:159-205
│   └── outage_changes.py  # NEW — wires up the existing detector
└── sinks/
    ├── __init__.py
    ├── log.py             # NEW
    ├── twilio_sms.py      # extracted from twilio_price_alerts.py
    ├── smtp_email.py      # rename of email_sender.py
    └── apns_push.py       # NEW (Phase B)
```

The `collectors/twilio_price_alerts.py` file is deleted; that logic is
now `plugins/price_breach.py` + `sinks/twilio_sms.py`.

---

## 9. Integration with the collector

Single hook in `unified_collector_duckdb.py`. Find the existing place
in the cycle where it logs "merged N rows" for the last table, and
just after that:

```python
# In unified_collector_duckdb.py module init:
from aemo_updater.alerts import build_default_dispatcher

dispatcher = build_default_dispatcher(
    db_path=DB_PATH,
    data_dir=DATA_DIR,
)

# In the poll cycle, after all merges:
dispatcher.run_cycle(db_path=DB_PATH, data_dir=DATA_DIR)
```

`build_default_dispatcher` is a 20-line factory in `alerts/__init__.py`
that constructs the standard plugin+sink set from env. Tests can build
custom dispatchers with mock sinks.

The standalone `battery_monitor.py` and
`standalone_renewable_gauge_with_alerts_updated.py` processes go away.
Their tmux windows close. One daemon, one log file, one PID to babysit.

---

## 10. Migration plan (Phase A — pre-iOS)

Each step is a small PR that ships independently. Tests pass on each.

| # | Step | Risk | Verification |
|---|---|---|---|
| 1 | Add `id` + `dedup_key` to `Alert` dataclass with `Optional` defaults so existing call sites still work | trivial | existing tests pass |
| 2 | Add `AlertContext`, `AlertDispatcher` skeleton, `ALERT_ROUTING` (empty), `LogSink` | trivial — no plugins yet | dispatcher.run_cycle is a no-op |
| 3 | Extract `TwilioSmsSink` + `SmtpEmailSink` from existing modules. Keep old modules importing the new sink classes for back-compat. | low — same Twilio/SMTP code, just relocated | run existing alert paths, check SMS still arrives |
| 4 | Convert price-alert logic to `PriceBreachPlugin`. Wire into dispatcher. Keep the old `collectors/twilio_price_alerts.py` calling site temporarily. | low | force a high-price test event, check SMS body byte-identical |
| 5 | Delete the old `collectors/twilio_price_alerts.py` call site once plugin proven | trivial | one less call path |
| 6 | Convert `new_duid` logic to `NewDuidPlugin`. Wire in. | low | force a synthetic new-DUID, confirm email arrives |
| 7 | Convert `data_freshness` (alert_manager.py:159-205) to plugin. Wire in. | low | unplug a collector temporarily, confirm staleness email |
| 8 | Convert `battery_monitor.py` to `BatteryRecordsPlugin` + `BatteryLowSocPlugin`. **Stop the standalone daemon**. | medium — losing standalone process means crashes aren't independent | shadow-run for 24h: both daemon and plugin running, compare outputs |
| 9 | Convert renewable gauge alerts to `RenewableRecordsPlugin`. **Stop the standalone gauge alert daemon**. | medium — same shadow-run approach | shadow-run 24h |
| 10 | Wire `outage_changes` plugin (was detection-only) | low — net new alert class, no old behaviour to preserve | manual outage extension test |

Phases 8 and 9 use shadow-running because the standalone daemons have
their own state files. Plugin reads (not writes) for 24h, compares
"would I have alerted" against the live SMS log. Once parity is
confirmed, the daemon stops and the plugin starts writing the state
file.

---

## 11. Phase B — iOS push (APNs sink)

After Phase A is stable and the collector owns everything.

### What's needed (server-side)

1. **APNs key.** `AuthKey_<KEYID>.p8` from
   developer.apple.com → Certificates, Identifiers, & Profiles → Keys.
   Different from the App Store Connect API key. Store at
   `~/.config/aemo-api/apns/AuthKey_<KEYID>.p8` mode 600.
2. **`ApnsPushSink`** in `alerts/sinks/apns_push.py`. ~80 lines:
   build JWT from p8 + Team/Key ID, POST to `api.push.apple.com/3/device/<token>`
   per registered device, handle `BadDeviceToken` / `Unregistered` by
   marking tokens inactive.
3. **Token registry table** in DuckDB:

   ```sql
   CREATE TABLE apns_tokens (
       token       VARCHAR PRIMARY KEY,
       user_label  VARCHAR,
       registered_at TIMESTAMP,
       last_seen_at TIMESTAMP,
       active      BOOLEAN DEFAULT true,
       categories  VARCHAR[]    -- ['price', 'new-duid', 'renewable-record']
   );
   ```

4. **API endpoint** `POST /v1/devices/register` on
   `aemo-energy-dashboard2`:
   ```
   { "token": "<apns hex>",
     "user_label": "David iPhone (optional)",
     "categories": ["price", "renewable-record"] }
   ```
   Upserts `apns_tokens`. The collector reads from this table at sink
   construction time and re-reads periodically (5 min) so newly-
   registered devices get pushes within the next cycle.

5. **Routing table additions** — already shown in §6 above.

### What's needed (iOS-side)

1. **`PushNotificationService.swift`** in the iOS app: request
   notification permission, retrieve the APNs token via
   `application(_:didRegisterForRemoteNotificationsWithDeviceToken:)`,
   POST it to `/v1/devices/register` on first launch and on token
   refresh.
2. **Settings → Notifications screen** with toggles for the three
   categories (price, new-DUID, renewable records). Battery records
   intentionally omitted per scope.
3. **Notification tap deep-link**: tapping a price-breach push opens
   Prices → Spot for the affected region; tapping a renewable-record
   push opens Today; tapping a new-DUID push opens Browse → Stations.

### Categories (iOS-side mapping)

The iOS app sees three category buckets, the catalogue's 8 alert IDs
map onto them:

| iOS category | Alerts |
|---|---|
| `price` | spot-price-high-breach, spot-price-extreme-spike |
| `new-duid` | new-duid-detected |
| `renewable-record` | renewable-record-percentage, wind-record-mw, solar-record-mw, hydro-record-mw, rooftop-solar-record-mw |

The `ApnsPushSink.emit(alert)` looks up the alert's category, queries
`apns_tokens WHERE active AND ? = ANY(categories)`, fans out to each.

---

## 12. Phase C — per-device subscriptions UI

After Phase B works for everyone-on-everything.

- Settings → Notifications shows three toggles (price / new-DUID /
  renewable record) with the current state from the user's last
  registration.
- Toggling re-POSTs `/v1/devices/register` with the new categories list.
- Per-record-class opt-out (e.g. "wind records only, not solar") is a
  Phase D refinement; v1 of Phase C is the three-category cut.

---

## 13. Open questions

1. **Should plugins share a DuckDB connection or open their own?**
   Recommendation: own. Per-request open is ~1ms; connection isolation
   means a misbehaving plugin can't lock another out. Matches the API
   server's pattern.

2. **What about retry?** Sinks fail silently to log today. For Phase B
   we may want retry for APNs (HTTP/2 connection drops) — keep that
   inside `ApnsPushSink`, not at the dispatcher.

3. **Migration of existing rate-limit state.** Each plugin reads from
   the same JSON path the standalone daemon used to write to. The
   shadow-running step lets us catch any schema drift before cutover.

4. **Test fixture.** `tests/api/fixtures/test.duckdb` doesn't have
   `bdu5` populated; need to extend it for `BatteryRecordsPlugin`
   tests. Pattern matches `extend_for_batteries.py`.

5. **Outage changes payload.** The `change_detector.py` writes to
   `outage_changes.parquet`. The plugin reads `WHERE detected_at >
   ctx.last_run_at`. That requires the dispatcher to persist
   `last_run_at` across restarts (currently it just initialises to
   `now()`). Add a single key to a state JSON.

6. **APNs sandbox vs production.** TestFlight builds use the
   *production* APNs server (`api.push.apple.com`). Dev builds (Xcode
   sideload) use sandbox. The sink should pick based on a config flag,
   default production.

7. **What if Twilio is rate-limiting us?** Track 429s in the SMS sink;
   surface as an `email`-only "alerts are degraded" notice. Not for v1.

---

## 14. What this gets us

- **One daemon, one PID, one log.** Reduces operational surface from
  three babysitting targets to one.
- **One place to add a new alert.** Plugin file + entry in `alerts.md`
  + one routing table line. Done.
- **One place to add a new channel.** Sink file + sink registration.
  Done. Adding APNs (Phase B), Slack, Discord, etc. all become
  drop-in.
- **No drift between alerting paths.** Plugins pure, sinks pure,
  routing is the contract. SMS and iOS push delivered from the same
  emit call to the same `Alert` object — divergence is impossible.
- **Sets up dedup centrally for free.** If plugins start sharing
  patterns (24h cooldown, hysteresis, etc.) they can grow shared
  helpers in `alerts/dedup.py` over time.

---

## 15. Not in scope of this doc

- The actual code rewrites (this is the architecture; PRs will follow
  the migration plan in §10).
- iOS UI for category toggles (covered briefly in Phase C).
- A unified state-file format (each plugin keeps its existing format
  through the migration; a refactor of the renewable_records.json
  shape onto the cleaner battery_records.json pattern can come later).
- Web UI for managing tokens. The token registry is opaque to testers;
  it's just a backing store for the iOS app's notification preferences.
