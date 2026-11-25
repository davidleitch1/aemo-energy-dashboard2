# Dashboard and data collector — session log

Covers the NEM dashboard (FastAPI/HTMX app on .71) and the AEMO data collector. The price forecasting system keeps its own log in `itk_lp_model/docs/sessions_history.md`.

**Protocol.** Start any dashboard or collector session by reading `docs/SYSTEM.md` (how the system runs now), then the state block and the last two entries here. When a session changes how something runs, rewrite the affected part of SYSTEM.md. End it by appending an entry at the top of the log and rewriting the state block. Each entry is a handoff: what was asked, what was produced, what was decided and why, what was corrected and must not be undone, what is open.

## State now (25 Nov 2026)

- **Dashboard code:** `~/aemo-redesign` on .71 (davidleitch@192.168.68.71), branch `web-dashboard-redesign`, remote `davidleitch1/aemo-energy-dashboard2`. The whole app is one file, `src/aemo_dashboard/web/app.py` (~9,000 lines). Tests for the web app are in `tests/web/`; run with `/Users/davidleitch/aemo_production/aemo-energy-dashboard2/.venv/bin/python -m pytest tests/web -q` from the repo root.
- **Live service:** tmux `services:1`, `uvicorn app:app --port 5008 --workers 4`, run from `src/aemo_dashboard/web`. No `--reload`, so code changes need a restart of that window. The launch line exists in both `~/tmux_files/start_services.sh` (boot) and `~/tmux_files/restart_dashboards.sh` (nightly 00:15); a change to how it launches must go in both, plus the process signature in `monitor_services.py`.
- **Testing a change before deploy:** run a second uvicorn from the same directory on port 5099, check it at `http://192.168.68.71:5099/...`, then kill it and restart `services:1`.
- **Collector:** `~/aemo_production/aemo-data-updater` on .71, tmux `services:0`.
- **Integrity check:** `scripts/weekly_integrity.py` now runs daily at 04:17 over all 12 base tables (5-minute: prices5, scada5, transmission5, bdu5, curtailment_regional5, curtailment5, curtailment_duid5; 30-minute: prices30, scada30, transmission30, demand30, rooftop30; plus `demand30.demand_less_snsg`). It takes the DuckDB lock between collector cycles and does not stop the collector. Sunday emails the full report; other days email only if something was missing. 175 unreachable historic intervals are in `~/aemo_production/logs/integrity_known_gaps.json`. Survey time is NEM time (UTC+10): the host clock is on daylight saving. Detail in SYSTEM.md "Data integrity".
- **Generation mix subtabs:** Yr on yr, Stack, Compare regions, Time of day, Trends, Transmission.
- **Futures data:** `~/aemo_production/data/futures.csv`, weekly Sunday 00:00 rows, runs to 11 Oct 2026. Update by saving the NEM-Review export in `~/futures_updates/` as `nemreview_raw_<date>.csv.raw`, keeping only its weekly 00:00 rows (drop the trailing part-week row stamped with the export date), and running `~/aemo_production/data/update_futures.py <file>` (merge: union of dates and columns, new wins). **Since Aug 2026 the export gives the newest 16 contracts (2029 Q3–2030 Q2) only as daily 17:00 rows**: average them Mon–Fri into the following Sunday (NEM-Review's own weekly rule, checked exact) — `~/futures_updates/build_futures_update.py` does this. Six mid-week part-week rows from Mar–Jul 2026 remain. The Monday launchd job `com.aemo.update-futures` has not run since 4 May 2026.
- **duid_mapping:** audited 8 Oct against AEMO sources; weekly refresh (cron Sun 05:10) fills gaps and emails conflicts — first live run 11 Oct. Pump loads carry fuel NULL and are out of all generation totals. **Open:** 14 refresh conflicts to review; stale `tests/api` fixture DB.
- **PASA tab:** "now" comes from PD-PASA (`outages_pdpasa.parquet`, every 30 min) spliced with ST-PASA after PD's horizon; Now has a supply-impact strip (coal % out, MW out vs demand, price by region). Rebuilt 10 Oct with four subtabs (Now & 7 days, Extended outages, Return-date changes, Transmission); logic in `web/pasa_data.py` and `web/pasa_transmission.py`; the Today outage tile uses the same code and `duid_mapping`. Feeds come from the outage monitor (tmux window 7, `~/aemo_production/outage_monitor`, local git only); see SYSTEM.md "PASA data".
- **Batteries subtabs:** Fleet (`/batteries`, alias `/batteries/fleet`) and Time of day (`/batteries/tod`, module `web/battery_tod.py`). **Demand tab** (`/demand`, after Prices; module `web/demand_tod.py`): demand by time of day, this period vs 52 weeks earlier, with operational/total and storage-load toggles. Both modules are framework-free and carry their own CSS scoped under `.btod` / `.dtod`; the standalone test apps `battery_tod_app.py` (5097) and `demand_tod_app.py` (5096) still work but are not running. `_build_url(base, /, **params)` is positional-only in `base` because `/demand` has a `base` query param.
- **Open (Demand tab):** sunny-days-only filter; modelled household-battery line (flagged as estimate); median price option; legend cramped at phone width. Reasoning and figures in the 5 Nov (midday) entry.
- **Open (analysis):** battery earnings vs WACC rerun on full-year operating batteries only; SA utility solar −19% (Aug–Nov 2026 vs 2025) unexplained. Figures in the 5 Nov (late afternoon) entry.
- **Open:** the futures launchd job is dead; not investigated (it only merges a file dropped by hand, so manual runs lose nothing).
- **Open:** collector scada30 catch-up bug (23 Oct): `collect_30min_scada` only builds endpoints newer than `MAX(scada30)`, so a scada5 file that lands after a later endpoint has been built leaves that label empty, and `gap_repair` does not rebuild it because no 5-minute row was newly inserted. Hit once after the 23 Oct NEMweb stall; fixed by hand.
- **Catch-up repair (13 Nov):** `scripts/catchup_repair.py` in aemo-data-updater, cron every 15 min, runs the integrity repair and emails once the collector has caught up after downtime but left holes. See SYSTEM.md "Data integrity". The collector still fetches newest-first; this repairs after it rather than changing it.
- **Catch-up repair, first live use (17 Nov):** after a ~16 h outage it recovered 1,073 of 1,104 holes and then, by design, did not retry the other 31 inside 24 h; a manual `weekly_integrity.py --no-email` filled them. Open: whether a second attempt after an hour would be better than waiting for 04:17.
- **Network tab is LIVE (25 Nov 2026, 09:30 AEDT restart of `services:1`):** subtabs Hotspots, Map, Causes, Units, Now, plus `/network/area` and `/network/unit` (`web/network.py`). Data: the collector stores binding constraints and AEMO local price adjustments each cycle in `~/aemo_production/data/constraints.duckdb` (readers use `constraints_readonly.duckdb`; history from 1 Jul 2024); `network_summary.duckdb` is rebuilt every 15 min by cron (`aemo-data-updater/scripts/build_network_summary.py`); the map reads `web/network_geo.json`, exported on the Studio by `snakeplay/AEMO_spot/analysis_code/network_map/export_network_geo.py` and copied here by hand. Crons: `constraints_repair.py` 04:45, `refresh_network_outages.py` :07/:37, `backfill_constraints.py --auto` 05:30 on the 3rd/10th/17th/24th. Area names: `aemo-data-updater/config/network_area_names.csv`. Plan and results: `snakeplay/AEMO_spot/plans/congestion_data_plan.md`. **Trap: this checkout IS the live code. The 00:15 nightly restart serves whatever is committed or even just saved here, so the four table subtabs went live at 00:15 on 25 Nov before David had reviewed them. Unreviewed work must not sit in this checkout overnight; use a branch in a separate worktree for the test port.** **Also live since 25 Nov ~10:20:** an Equation view on the area page (`/network/area?id=…&view=equation`: plain-English note, left and right sides of the constraint, term by term), "Rank by" hours or energy on Hotspots and Map with a 12-month rank and movement, five causes (network as built, equipment outage, stability limit on a plant, commissioning, other cap on a plant), Units ranked by share of intervals, GWh or $m. Data: `generic_constraint_rhs` and `emsmaster` in `constraints.duckdb` (filled by `backfill_constraints.py --fill-rhs`, also run by `--auto`); builder outputs `constraint_equation*`, `constraint_hour`, `area_rank`, `area_note`; stable area ids in `~/aemo_production/data/network_area_registry.json`; hand notes in `aemo-data-updater/config/network_area_notes.json`; monthly change report cron `15 7 11 * *` (`scripts/network_change_report.py`). **Handoff for the whole congestion project (objectives, decisions, rules, open list): `snakeplay/AEMO_spot/plans/network_congestion_SESSIONS.md` on the Studio — read it first.** Open here: route tests in `tests/web/test_network_routes.py` read the live DB and break when its shape changes; two `constraints.duckdb.bak_*` backups to delete once settled. Disk on .71 read 63 GiB free at 10:22 on 25 Nov and 511 GiB by 10:51 after a cleanup of `ITK_SYSTEM_WORKER` done outside this work (`~/cleanup_20261125`).
- **DuckDB locking (25 Nov review):** readers never lock (replicas); failures are side writers vs collector on the primaries. Recommendation: writer-side fix now, Quack trial on a copy, adopt Quack after DuckDB 2.0.1/2.0.2; not DuckLake. See 25 Nov (late morning) entry.
- **Replicas by clone (25 Nov):** `*_readonly.duckdb` are APFS clones of the primaries (aemo-data-updater 3e0d9e6); look for `(clone)` in the collector log. .71 at 511 GB free after cleanup. **Open:** after any collector restart the in-memory file sets are empty, so bids re-process ~58 BIDMOVE files (about an hour of ~190 s cycles) and bid_dispatch re-processes the ~396 Next_Day_Dispatch files on CURRENT 5 a cycle (about 80 cycles, ~7 h of ~60 s cycles; same after the 24 Nov 14:31 restart: 81 cycles). No new rows; longer lock holds.
- **Open:** whether to relabel Batteries-tab Util % as cycles a day (it is discharge ÷ storage per day × 100).

## 2026-11-25 (midday) — .71 disk cleanup; readonly replicas by APFS clone

**Asked.** .71 was 93% full; review the disk, clear what is safe, then fix the cause carefully without side effects on the collector, dashboard or access.

**Done.** (1) Deleted 11,291 old forecasting-worker run folders in `~/ITK_SYSTEM_WORKER` (258 GB, all older than 14 days; kept every run named in `docs/LIVE_STATE.md` on .69, the ESS policy stores, provenance, LP_INPUTS, DATA_PREPROCESSED); manifest, keep list and log in `~/cleanup_20261125/`. (2) `tmutil thinlocalsnapshots` released about 190 GB held by hourly local snapshots. Free space 61 GB → 511 GB. (3) aemo-data-updater commit 3e0d9e6: `replica.refresh_replica` clones the primary (`clonefile`) into `<replica>.tmp` and renames it over the replica, falling back to `shutil.copy2`; all four refresh sites use it (aemo, bids, constraints in the collector; `constraints_store.refresh_readonly`). Tests `tests/test_replica.py` (8, all in tmp_path). Collector restarted 10:46:44 in `services:0`; every copy since logs `(clone)` at 0.0–0.1 s (was 0.7–25 s).

**Measured.** One cycle changes 7.2 MB of the 2.1 GB `aemo_test.duckdb` and 3.2 MB of `constraints.duckdb`, which is what each hourly snapshot now holds instead of full replica copies.

**Decided, do not undo.** Clone in place rather than moving the replicas to a separate APFS volume: a move would change about 25 hard-coded paths and hide the files from the Mac Studio over SMB. Paths, bytes and the atomic rename are unchanged for readers.

**Found.** Bids were NOT stalled (an earlier reading in this session said they were; wrong). Bids come from daily `PUBLIC_BIDMOVE_COMPLETE_` files, so `bids: o` on most cycles is normal and the replica ending at 04:00 is the end of the previous trading day. But `last_files['bids']` and `last_files['bid_dispatch']` live only in memory, so after every collector restart all ~58 daily files on CURRENT look new and are re-processed 5 per cycle (58, 53, 48, 43 ...): about 12 cycles of ~190 s instead of ~25 s, holding the write lock longer, with no new rows. This also happened after the 24 Nov 14:31 restart (bids copied every cycle that evening). `tests/test_readonly_replica.py` opens a write connection on the live `aemo_test.duckdb`; do not run it while the collector runs.

**Open.** Seed the bids/bid_dispatch file sets from the DB (or a state file) at start so a restart does not re-process ~58 daily files. Optional `sudo tmutil addexclusion -p` for the three `*_readonly.duckdb` files. Writer-side lock fix from the morning review. Personal data on .71 (Messages 52 GB, Photos shared library 59 GB, Mail 42 GB, Dropbox 20 GB) left alone.

## 2026-11-25 (late morning) — DuckDB locking review (Quack, DuckLake)

**Asked.** As the files grow there are occasional locks; would Quack or DuckLake be better than the readonly-copy pattern?

**Found.** No reader has hit a lock: every app reads a `*_readonly.duckdb` copy. The lock errors in the logs are all writer against writer on the primaries: `sttm_update` (11:00 daily, retries and gets in), `refresh_network_outages.py` (:07/:37; failed at 10:09 today because a manual `fill_rhs` job held `constraints.duckdb` for longer than its 30 s of retries), `db_integrity`, and an anaconda python on 3 Nov. Replica copy of `aemo_test.duckdb` (2.1 GB): median 0.7 s, p99 2.6 s, max 24.9 s over 74,434 copies. Disk on .71 is 93% full (64 GB free); the three primary/replica pairs hold about 14 GB of duplicates. Collector venv runs duckdb 1.4.4, dashboard2 venv 1.3.2. DuckDB 2.0.0 LTS with Quack shipped 21 Oct 2026; DuckLake 1.0 shipped Apr 2026 and needs a Postgres catalog (or Quack) for several writers.

**Decided (recommendation, awaiting David).** DuckLake: no — it is built for many writers on object storage and would turn 5-minute appends into small parquet files needing compaction plus a Postgres service. Quack: the right long-run design (one server owns each file, writers and readers are clients, replicas go away) but needs a fleet-wide move off 1.4.4; run the half-day trial on a copy now, adopt after 2.0.1/2.0.2. Meanwhile fix the cause of today's failures: side writers and manual jobs should take short write windows (stage in a scratch file, merge in one transaction) and retry for at least one full cycle (~5 min), not 30 s.

**Open.** Whether to do the writer-side fix now; the Quack trial; disk headroom on .71.

## 2026-11-25 (late morning) — Equation view, energy ranking, five causes, Units sort: built in worktrees, merged, live

**Asked.** Rank limits by energy held back; an Equation view explaining each side of the constraint in plain English, reached from the existing binding-hours lists; split "limit on a single plant" into three causes; sort Units by GWh; design how the top five stay current; then "merge it in".

**Produced.** aemo-data-updater master 35e38d4 (RHS tables and `--fill-rhs`, per-constraint energy, equation tables, rankings, notes with fingerprints, area registry, five causes, `network_change_report.py`). This repo 51b517e + 97d15ae (Equation view, Rank by, 12-month rank, five-cause labels and key, Units sort, `NETWORK_SUMMARY_DB` env override). Both were built by delegated agents in git worktrees on a `network-equation` branch and tested on port 5099 against a test summary before merging; the worktrees and branches are now removed.

**Decided.** Top five = the five areas with the most wind and solar energy held back over the trailing 12 months, computed at every rebuild, all limit types. Every constraint gets a template explanation; hand notes are extras tied to a fingerprint of the right side's structure. Area ids are frozen in a registry. `GENERICEQUATIONDESC` is not used (it does not describe rating terms).

**Corrected, do not undo.** The Snowy area's note describes `N::N_CNCW_2`, its lead constraint by energy, not `N::N_SDUT_2`. Interconnector rows read "<name>, flow <direction>" with their own sentence (page side).

**Went wrong.** (1) `--fill-rhs` on the live DB locked the collector's constraint step out for two cycles; recovered by retry, no gap. (2) A route test asserting the old cause label failed against the new live data; test fixed. (3) Disk on .71: 63 GiB free at 10:22 against 187 GiB a day earlier; likely Time Machine local snapshots pinning replica copies, not confirmed. Detail and options in the plan, section 23.

**Open.** Disk actions (David to decide). David's read of the Snowy note. Two DB backups to delete once settled.

## 2026-11-25 (morning) — Network tab live

**Asked.** Go live; go ahead with the equation view's first two stages; consider how the top five should adjust over time.

**Done.** `services:1` restarted at about 09:30 AEDT (Ctrl-C, pgrep confirmed the old process gone, launch line from `restart_dashboards.sh`); up in 2 s; `/network`, `/network/map` (also `range=now`), `/network/causes`, `/network/units`, `/network/now` return 200 in 0.4–1.0 s; `/batteries` 200. tmux `network_test` (port 5099) closed.

**Found.** `/network` already returned 200 on port 5008 before this restart: the 00:15 nightly restart had loaded the Network tab committed on 24 Nov. It was live for about nine hours without David's review. No fault was reported, but the promise "not live until you say" was not kept. See the trap in the state block.

**Open.** SYSTEM.md updated for the Network tab. Equation view stages E0–E1. A worktree for future test-port work.

## 2026-11-25 — Network tab: map stages M0–M3, reference maps, equation view proposed

**Asked.** A map view with stress shown by colour; research a public map of all lines; build it in planned stages; show Port Macquarie–Herons Creek; a folder of reference maps per region; then add the map as a subtab. Also: rank limits by energy held back and plan a view that explains constraint equations in plain English.

**Produced.** On the Studio (`snakeplay/AEMO_spot/analysis_code/network_map/`): GA Electricity Infrastructure download (3,147 lines, 1,866 substations, 430 power stations; CC BY 4.0, stated in the service metadata), unit coordinates from open-licensed sources (97% of wind/solar/battery MW), an area-to-line matcher plus hand file, a review page with lightbox (https://claude.ai/artifact/WqgRG53Ba18VfA98HemVnv), reference maps in `AEMO_spot/reference/network_maps/`, and the export of `network_geo.json`. Here: the Map subtab (commits 14195b2, e942961 by the delegated agent, 18ba109 a zoom fix), 33 new tests.

**Decided.** Open Electricity's facility file is CC BY-NC and is not shipped; it was used only as a cross-check. Stability limits get no coloured line, only a reference line when their area is selected. Lines are never all drawn at once; the 220 kV+ network is a legend toggle, off by default.

**Corrected, do not undo.** Port Macquarie–Herons Creek is in GA as part of "Port Macquarie to Taree" with the place spelled "Heron Creek"; the section is cut into `hand_line_geoms.json`. Search GA by location when a name search fails.

**Went wrong.** The tmux session `network_test` and its server were gone when the zoom fix was deployed (cause not found; the page had answered minutes earlier). Restarted with `tmux new-session -d -s network_test -c ~/aemo-redesign/src/aemo_dashboard/web "<venv python> -m uvicorn app:app --host 0.0.0.0 --port 5099"`.

**Open.** David's review of the whole Network tab on 5099, then restart `services:1` to go live. Decisions on the equation view (plan section 19). `network_geo.json` is hand-copied. SYSTEM.md not yet updated for the map (do it at go-live).

## 2026-11-24 (evening) — Network congestion Phase 3: Network tab built, on test port

**Asked.** Go ahead with Phase 3. Mid-build David said the cause labels "unit specific" and "system normal" were not clear.

**Produced.** (1) Summary builder in `aemo-data-updater` (`src/aemo_updater/network_summary.py`, `scripts/build_network_summary.py`, `config/network_area_names.csv`; commits d67f3f6, 804ba28, 1b82dac, 06027e7): reads the two readonly DBs, writes `network_summary.duckdb` whole in about 6 s; cron `11,26,41,56 * * * *`, log `logs/network_summary.log`. (2) `web/network.py` and routes in `app.py` (commits 2b6ed6b, 91176c8, c058baf, d9d28ad): TABS entry after PASA, CSS added to the shell style line, routes `/network`, `/network/hotspots`, `/network/area`, `/network/causes`, `/network/units`, `/network/unit`, `/network/now`. `tests/web`: 203 pass. Coding delegated; reviewed, run and screenshotted here (1400 px; the agent checked 390 px).

**Decided.** Summaries live in their own file so page loads are 0.1–0.4 s and nothing contends with the collector's lock. Hotspot areas group constraints by shared unit set (David's choice). Causes display as "Network as built", "Equipment outage", "Limit on a single plant", with a key on Hotspots and Causes; stored values are unchanged ('System normal', 'Outage', 'Unit-specific limit'). Cost is curtailment while constrained valued at RRP (negative as zero), because AEMO marginal values reflect floor bids and violation penalties. The Now page shows "at violation penalty" for |marginal value| >= 100,000.

**Checked.** Builder reproduces the Phase 0 battery counts exactly (BRDDBES1 7,180 etc.). Hotspots, Causes, a unit page and Now viewed in a browser with no console errors.

**Open.** David's review, then restart `services:1` to go live. "MW in constraints" is too broad for large areas; dollar values are driven by a few spike days; no NEM-wide "hours with any constraint binding" measure; subtab links do not carry region/period. Listed in the plan, section 14.

## 2026-11-24 (afternoon) — Network congestion Phase 2: live collection deployed

**Asked.** Go ahead with Phase 2.

**Produced.** In `aemo-data-updater` (commits 3428a74, 5300ad4, 37cf8ca, 848df2c, 22708a3; coding delegated, reviewed and run here): `store_dispatchis`, `connect_with_retry`, snapshot replace and outage upsert in `constraints_store.py`; `collect_and_store_constraints` + `_copy_constraints_to_readonly` in `unified_collector_duckdb.py` (one guarded block after bids; no existing method changed); `scripts/constraints_repair.py`; `scripts/refresh_network_outages.py`; `backfill_constraints.py --auto`. 89 tests pass; `test_collector_regression.py` and `test_dispatchis_cache.py` pass unmodified.

**Deployed.** Initial repair filled 1–24 Nov (6,778 intervals from ARCHIVE daily zips and CURRENT, all recovered). Collector restarted 14:31 AEDT; first cycle: all feeds normal, `constraints: +`, 12 files stored, replica copy 0.8 s. Three cron lines added (backup `~/tmux_files/crontab.bak_pre_constraints_20261124`). Ledger complete 1 Jul 2024 → present. DB backup before Phase 2: `constraints.duckdb.bak_pre_phase2_20261124` (delete once settled).

**Went wrong, no harm.** The restart was meant to wait for a cycle to finish; the wait loop broke when the previous "Waiting" line scrolled out of the pane, so Ctrl-C landed as a cycle ended (after the replica copy). The process exited cleanly and pgrep confirmed it before the start command was sent. For next time: detect cycle end from the collector's log line count, not by comparing the last matching pane line.

**Decided.** Constraints follow the bids pattern (own file, own short-lived connection, own replica) so nothing here can fail a price or SCADA merge. Repair is a separate script and cron, not part of `weekly_integrity.py`, to leave that proven code alone. A cold start stores the newest 12 files; the daily repair covers the rest.

**Open.** See state block. First true live-versus-archive reconcile is December 2026 data, when its archive appears.

## 2026-11-24 (midday) — Network congestion Phase 1: history backfill into constraints.duckdb

**Asked.** Group hotspots by the set of units held back; proceed.

**Produced.** `aemo-data-updater`: `src/aemo_updater/constraints_store.py` (schema, anti-join loaders, views `unit_binding` and `local_price_computed`), `scripts/backfill_constraints.py`, 27 tests in `tests/test_constraints_store.py` and `tests/test_constraints_backfill.py` (commits 504cac1, 36085ee; coding delegated, verified here). Backfill run in tmux `constraints_bf` (now ended): 142 months, 55 minutes, 0 failures, log `~/aemo_production/logs/backfill_constraints_20261124.log`. New file `~/aemo_production/data/constraints.duckdb`: 5.41 M binding rows, 245,664 intervals (complete), 10.4 M local price rows from Aug 2024, definitions from Jan 2015, outage and DUDETAILSUMMARY snapshots for Oct 2026.

**Checked.** Interval ledger equals the expected count. Broadsound Sep-Oct discharge-limited intervals = 7,180, the Phase 0 figure. prices5 was current at the end of the run; a 20 s pause after each large download was enough to avoid nemweb rate limiting.

**Decided.** Binding rows only; definitions swept from 2015 because the monthly files are incremental; pre-2015 months (whole-month zips) skipped.

**Open.** Phase 2 go-ahead. 2013-08 and 2013-11 definitions (two network constraints: SVML_ZERO, N_X_MBTE_3A). Outage rows are never updated once loaded — fix before live outage collection. 4,586 repeated invocations in `genconsetinvoke`. SYSTEM.md now lists the new database.

## 2026-11-24 (later) — Network congestion Phase 0: proof on two months of archive data

**Asked.** Go ahead with Phase 0; history from 1 July 2024, with more history as a planned post-build addition.

**Produced (all on the Studio, no change on .71).** Sep-Oct 2026 MMSDM tables loaded to a scratch DuckDB; four scripts in `AEMO_spot/analysis_code/congestion_phase0/`; five charts in `AEMO_spot/pngs/congestion_phase0/` (hotspots under two grouping rules, causes, unit league, one unit page). Results and scope changes are section 11 of the plan.

**Gates.** G1 passed: our join reproduces AEMO's DISPATCH_LOCAL_PRICE adjustment in 99.7% of 1.28 million unit-intervals. G2 passed: battery constraint exposure matches NEMPulse within about 1% for five batteries, 20 of 25 constraint counts exact. Rule: positive energy weight on a binding <= network constraint is discharge-limited; exclude `F_`, `#`, `$` constraints.

**Found, and must carry into the build.** (1) GENCONDATA, SPD*CONSTRAINT and GENCONSET monthly files are incremental: only versions effective that month. The definitions backfill must sweep the archive from 2015; 2012-2014 exist only as whole-month zips. (2) Constraint weights are keyed by connection point; DUDETAILSUMMARY is needed to reach DUIDs. (3) MARGINALVALUE is not a cost measure (floor bidding and violation penalties); cost must come from curtailed energy x RRP via `curtailment_duid5`. (4) nemweb drops connections on large archive files; use curl with retries.

**Open.** G3 (David picks grouping and layout). 23,208 unit-intervals where we compute an adjustment and AEMO publishes none (Pelican Point, Torrens B, some wind farms). A weighting rule for "units behind" a constraint.

## 2026-11-24 — Network congestion: assessment of NEMPulse, use plan and collector scope (no code)

**Asked.** Whether the NEMPulse site and its MCP server give data we lack, especially on network congestion; then a plan for how constraint data would be used (dashboard page, model) and a scope for collecting it.

**Found.** NEMPulse (`https://nempulse.com.au/mcp`, no auth, 14 tools) is battery-only, gives constraint exposure as percentages over 30+ day windows, and has no marginal values or interval data. Its source is public AEMO data that we do not store: the dashboard databases have no constraint tables. The DispatchIS file the collector already downloads each cycle contains `DISPATCH,CONSTRAINT` (1,065 rows in the 24 Nov 08:55 file, 25 binding) and `DISPATCH,LOCAL_PRICE` (117 units with AEMO's own local price adjustment); only PRICE, INTERCONNECTORRES and REGIONSUM are parsed. `Reports/Current/Network` carries NETWORK_OUTAGEDETAIL and NETWORK_OUTAGECONSTRAINTSET (outage to constraint set). Definitions (GENCONDATA, SPD*CONSTRAINT, GENCONSET) exist only in MMSDM monthly, latest 2026_10.

**Produced.** `snakeplay/AEMO_spot/plans/congestion_data_plan.md`: questions answered, a four-subtab Network page (Hotspots, Causes, Units, Now), four uses in the forecasting model, sources, storage design, phases with gates G1-G4, decisions and risks.

**Decided (proposed).** Separate `constraints.duckdb` on the bids pattern, so the per-cycle copy of `aemo_test.duckdb` does not grow. Store binding rows only plus an interval ledger. Phase 0 is a proof on the Studio before any collector change. Predispatch constraints, MCC and a map are out of scope for now.

**Open.** David to approve Phase 0 and the history start (proposed 1 Jul 2024). When DISPATCH_LOCAL_PRICE history begins is unchecked.

## 2026-11-17 — Power outage: .71/.68 down ~16 h, catch-up repair first live use

**Asked.** Check and monitor recovery of nem-dash (.71) and JohnsM2 (.68) after a power failure.

**Found.** Both machines rebooted about 07:58 AEDT. On .71 all 15 `services` windows came back from the `@reboot` start script, the cloudflared launchd job was running, and the collector was current by 08:15. On .68 the launchd jobs com.itk.django, nginx, panel and aiindicators were running. Public hostnames nemgen, bids, spot_fcst and ai_dash answered 200.

**Data gap.** The collector missed 190 five-minute intervals (16 Nov ~15:35 to 17 Nov ~07:45 NEM). `catchup_repair.py` fired at 08:17 (its first live trigger): 1,104 missing, 1,073 recovered, 31 remaining, emailed. The 31 were mostly rooftop30 (16 Nov 14:00–22:00, not yet in the files fetched at 08:17) and two scada5 intervals that returned HTTP 403 from CURRENT, plus their scada30 slot. The script then stayed silent because the holes were in its 24 h attempted ledger, which is the intended design. A manual `weekly_integrity.py --no-email` at 08:51 found 20 holes and recovered all 20; the replica copied at 08:56 shows zero gaps from 16 Nov 12:00 onward in prices5, scada5, transmission5, scada30, rooftop30, prices30 and demand30.

**Open.** A second attempt about an hour after the first would have filled these without anyone stepping in; consider a shorter retry for the first retry only.

## 2026-11-13 (late morning) — Automatic repair after a collector catch-up

**Asked.** Run the repair automatically after a catch-up and email a summary. Do not interfere with the collector, which runs reliably, or introduce artefacts.

**Produced.** `aemo-data-updater/scripts/catchup_repair.py` and `tests/test_catchup_repair.py` (13 tests: hole detection, waiting while behind, newest labels ignored, 12 h window, once per hole, 24 h expiry, 04:00–05:00 quiet, two-hole threshold, end-to-end `main()` with a fake integrity script). Commit 7d28e36 in aemo-data-updater. Cron line `2-59/15 * * * *` added to .71's crontab; backup at `~/tmux_files/crontab.bak_pre_catchup_20261113`. A live run against the replica found no holes and exited silently.

**Decided.** A separate cron job rather than a change to the collector loop: the collector is untouched, the watcher reads only the read-only replica, and the existing integrity script does every write with its proven lock handling. Trigger on holes, not on reading the collector log, so it also catches holes from any other cause. Watch only the seven tables the collector keeps current every cycle; lagging feeds (curtailment5, rooftop30, demand30) would give false triggers and are covered once the integrity run starts. Threshold two holes: over the previous 14 days these seven tables had one hole (scada5 12 Nov 02:45, never published by AEMO), which would otherwise have sent a needless 3 a.m. email.

**Open.** None from this change. If an outage ends while it is 04:00–05:00 host time, the repair waits until the 05:02 run.

## 2026-11-13 — Power outage: reboot of .71/.68/.69, collector catch-up, integrity repair

**Asked.** After a power outage, watch the servers come back, confirm the dashboards run, and confirm the backfill and integrity check.

**Found.** .71, .68 and .69 all rebooted at about 08:30 AEDT; the Studio stayed up. On .71 `start_services.sh` (@reboot) created all 15 `services` windows; ports 5008, 5009, 8081, 5202, 5500, 8095, 8098, 5201 answered. On .68 the launchd jobs (django, nginx, panel, aiindicators) came up; 8089 and 8092 answered. nemgen, spot_fcst and ai_dash returned 200 through Cloudflare. .69 runs no services. Data stopped at 06:10 NEM (scada5 06:05). The collector's first cycle (08:30) ran before the network was up ("No route to host") and wrote nothing new. From the 08:35 cycle it fetched 12 files per feed. Those were the newest 12, not the oldest, so prices5/transmission5/bdu5/curtailment_regional5 06:15–06:40, scada5 06:10–06:35, scada30 06:30 and prices30/transmission30 06:30 and 07:00 were left empty while later intervals filled. The 04:17 daily integrity run predates the outage, so nothing automatic would have repaired this until 04:17 the next day.

**Produced.** `weekly_integrity.py --email-policy always` at 08:48: 36 missing, 35 recovered (from CURRENT, one scada interval from ARCHIVE), emailed. The one remaining is scada5 12 Nov 02:45, which predates the outage, is absent from CURRENT and ARCHIVE, and will go to the ledger after 4 days. A `--check-only` run straight after still showed 36 missing because it surveyed before the collector's next replica copy; the 08:55 copy put `aemo_readonly` at 0 missing for 05:00–07:30 NEM in every 5- and 30-minute table.

**Open.** After downtime longer than one 12-file batch, the collector leaves holes behind its newest files. Either fetch oldest-first when the backlog exceeds the batch, or have the collector run the integrity repair after a cycle that found a backlog (or once at startup). Until then, run `weekly_integrity.py` by hand after any outage.

## 2026-11-05 (late afternoon) — Battery economics and the solar price table (analysis, no dashboard code)

**Asked.** (1) Is A$0.4m/MWh right for a 4-hour battery in 2026? (2) What daily spread per MWh of capacity earns WACC, capital only? (3) A table of utility and rooftop solar, last 90 days vs the same 90 days a year earlier; then utility solar FWA added; then rooftop/total swapped for grid battery charging; then a PNG for LinkedIn.

**Found — capex.** GenCost 2025-26 final (Jul 2026) A$385/kWh for a 4-hour battery (via Energy-Storage.News; not checked against the report's appendix table). LP `scenario_costs_base_v3.json` 2026 vintage A$1.382m/MW = A$345k/MWh (falls to A$259k by 2030). Kemmerton stage 1 (WA, Dec 2024) about A$303k/MWh; Eraring stage 2 (2024) A$436k/MWh. Modo global 4-hour median 2026 US$201k/MWh. Answer given: A$0.3–0.4m/MWh, about A$0.35m central; 0.4 is the top of the range for a 2026 commitment.

**Found — capital-only hurdle.** Net margin needed per MWh of capacity per day = capex × CRF ÷ 365: at A$350k, $77 (5%/20 yr, the LP's CRF 0.0802), $90 (7%/20), $105 (7%/15), $112 (8%/15). At one cycle a day, 82.5% discharge/charge and a $30 charge price, that is a simple spread of about $83–118. Realised, 5 Nov 2025 – 4 Nov 2026, energy arbitrage at spot only, net per MWh of storage per DUID-day: NSW $25 (0.42 cycles/day), QLD $44 (0.64), VIC $43 (0.61), SA $78 (0.55), NEM $40 (0.54 cycles, 82.5% discharge/charge). The shortfall is cycling, not spread. Excludes FCAS, contracts and LTESA/CIS; includes commissioning units, which depress both measures.

**Found — solar, 8 Aug – 5 Nov 2026 vs 8 Aug – 6 Nov 2025 (52 weeks back).** Utility solar GWh NEM 4,270 → 5,159 (+21%); rooftop 7,825 → 8,490 (+9%). Clear-day peak (p99) utility +16%, rooftop +10% in every mainland region, so rooftop energy growth below capacity growth = a less sunny spring (QLD rooftop energy +3%). SA utility solar −19% with every farm down (Bungala 1/2 −28%/−23%) and SA solar curtailment down (141 → 119 GWh), so not curtailment; cause not established. Utility solar FWA (own-region RRP, output clamped at 0): NEM $17.7 → $41.6, NSW $23.8 → $56.7, QLD $7.9 → $31.7, VIC $18.9 → $21.6, SA $28.6 → $35.7. Share of utility solar output at negative prices NEM 55% → 24% (NSW 48% → 11%). Grid battery charging GWh: NSW 112 → 597, QLD 143 → 518, VIC 184 → 401, SA 112 → 176, NEM 552 → 1,692.

**Produced.** `snakeplay/research_pngs/battery_charging_vs_solar_90d_20261105.py` writes `_linkedin.png` (cream canvas: LinkedIn dark mode puts transparent images on near-black) and `_transparent.png` (house rule). Columns: extra utility solar GWh, extra battery charging GWh, solar FWA 2025 to 2026, solar FWA change. Title states the periods; it makes no claim, because charging grew less than solar in VIC and about the same in QLD.

**Decided.** Matplotlib reads a pair of `$` as mathtext: escape as `\$` in table text. The → glyph is missing from the installed fonts; use "to".

**Open.** Rerun the realised-earnings comparison on batteries in commercial operation for the whole year (offered, not done). Why SA utility solar output fell (outages, plant performance or weather in the north).

## 2026-11-05 (afternoon) — Batteries › Time of day and Demand tab live on 5008

**Asked.** Put the battery time-of-day prototype in as a subtab of Batteries and the demand time-of-day prototype in as a main tab.

**Produced.** `app.py`: TABS gives batteries subtabs Fleet / Time of day; `/batteries` and `/batteries/fleet` serve the existing page with the subtab strip on top; `/batteries/tod` has single-select Region (NEM, NSW, QLD, VIC, SA) and Period (7D–1Y) pills. New top-level `("demand", "Demand", [])` after Prices; `/demand` takes region (incl TAS), range, `base` (comma list op,total; never empty), `chg`/`pump`/`dis` (1/0; defaults 1/0/0), rendered as dashboard pill bars (Demand and Storage bars are toggles carrying all other params). `"demand"` added to the flat-route skip list. Module CSS scoped under `.btod`/`.dtod` with 24 px side padding, injected by `_render_shell`. Demand legend names shortened to "Operational · this period" etc. (the subtitle carries the date ranges; storage options go in the top panel's axis title) because long names were clipped. New `tests/web/test_tod_routes.py`; `tests/web` 142 passed. Checked in a browser on 5099, then `services:1` restarted with the standard launch line; all new routes 200 on 5008, `/prices` and `/pasa` still 307 to their default subtabs as before. Integration code written by a Sonnet subagent from a spec; reviewed, browser-tested and the padding/legend fixes made in the main session. Prototype tmux sessions `battery_tod`, `demand_tod`, `test5099` stopped. Backup `app.py.bak.pre_tod_20261105`.

**Decided.** Battery Fleet and Time of day do not share the region param (Fleet is multi-region, Time of day single). Shell teal pills replace the prototypes' purple/blue.

**Corrected — do not undo.** `_build_url(base, /, **params)`: without the `/`, `_build_url(url, base="op,total")` raises "multiple values for argument 'base'".

**Open.** Sunny-days-only filter for the Demand tab (separates weather from trend: QLD Oct 2026 rooftop fell 9% on a cloudier month while clear-day peak rose 7%). Optional modelled household-battery component (CER capacity × 8–10 kWh/day × self-consumption profile), flagged as an estimate. Median price option. Phone-width legend.

## 2026-11-05 (midday) — Reading the demand view: household batteries and the QLD rooftop drop (analysis, no code)

**Asked.** How to think about household battery charging, which the dashboard does not show; then whether QLD's fall in rooftop PV over the last 30 days, plus household charging, means more operational supply was needed.

**Found — where household batteries sit.** `rooftop30` is AEMO's estimate of PV generation, not exports, and OPERATIONAL_DEMAND is grid-supplied load. A home battery charging from its own PV cuts exports, so it raises operational demand; discharge lowers it in the evening. So Δ operational at midday = Δ underlying consumption − Δ rooftop + Δ household charging, and "Total" (operational + rooftop) counts household charging as load. Neither line separates it.

**Found — size (rough).** Cheaper Home Batteries Program: about 12.4 GWh validated in its first year to 30 Jun 2026, about 0.9 GWh/month registered Jul–Sep, so about 15 GWh nationally under the program (incl WA) and about 11–13 GWh more than a year earlier. Daily throughput is capped by overnight household load (8–10 kWh), not battery size, so about 4 GWh/day more NEM charging, about 0.6–1 GW extra over a 5-hour midday window; NSW (about a third of installs) about 0.3 GW. NSW 30 days, 10:00–15:00: grid batteries +784, household about +300 (estimate), pumps −168, so storage about +900 MW against rooftop +799 MW, consistent with the midday floor rising from about $10 to $75. Shape check: NSW operational demand this year sits above last year from about 06:30 to 12:30 and below from about 13:00, which is what self-consumption batteries (charge from first surplus, full by early afternoon) would produce.

**Found — QLD rooftop drop is weather.** QLD 30 days, 10:00–15:00: rooftop −517, operational +232, grid battery charging +473, pumps −145, so grid-supplied load about +560 MW; utility solar +61, wind +159; spot +$65. QLD1 rooftop is consistent across the 2026 format change (prior window QLD1 equals QLDC+QLDN+QLDS to the MW). Clear-day rooftop peak (p99) up 7% Oct on Oct (5,259 against 4,905 MW; Sep +13%) while the October average fell 9% (1,222 against 1,344 MW): more capacity, fewer sunny days. Utility solar shows the same (p99 +13%, midday output only +61 MW, about 200 MW short of what the extra capacity would give in last year's weather). So the cloud raised operational demand and cut utility solar at the same time; dispatchable supply plus imports met about 340 MW more at midday. Cloudy days also cut household charging. The lasting part is storage charging (grid and household); the rooftop and utility solar shortfall reverses in a sunny month.

**Found — data note.** rooftop30 holds 50 rows at 0 MW with `type = MEASUREMENT` (all five regions) at 00:30–04:30 on 13 Oct and 03:00 on 20 Oct 2026. Night hours, so no effect on any figure; not investigated further.

**Decided.** These are estimates for reasoning, not dashboard numbers. The household-battery size rests on CER/press figures (energyxai to 30 Jun 2026, ESS News Sep 2026, Energy-Storage.News Aug 2026) and an assumed 8–10 kWh/day throughput.

**Open.** Sunny-days-only comparison (days with rooftop above about 80% of the month's clear-day peak, both years) to separate weather from trend and estimate how much household charging depends on the weather. Modelled household charging/discharge line from monthly CER capacity by state, flagged as an estimate. Compare with AEMO's 2026 ESOO/IASR estimate of home battery operation.

## 2026-11-05 (late morning) — Demand time-of-day prototype (port 5096)

**Asked.** A second small dashboard: demand by time of day for the same regions and periods, with a line for the same period a year earlier, checkboxes for total demand, operational demand and whether to include charging. Purpose: see whether storage charging or other demand changes explain the change in price.

**Produced.** `src/aemo_dashboard/web/demand_tod.py` and `demand_tod_app.py`, tmux session `demand_tod`, `uvicorn demand_tod_app:app --host 0.0.0.0 --port 5096`. Components per half-hour slot (mean MW over the window): operational demand (`demand30.demand`), rooftop PV (`rooftop30`, parent regions only), battery charging and discharge (scada30, Battery Storage), pump load (scada30 for PUMP1, PUMP2, SNOWYP, SHPUMP, KIDSPHL1/2, which carry fuel NULL). Checkboxes: operational and/or total (operational + rooftop) as the base lines; add battery charging, add pump load, subtract battery discharge. Three panels sharing the time axis: demand lines (solid this period, dashed a year earlier), spot price both years (NEM demand-weighted by operational demand), and the stacked year-on-year change in each component the checkboxes include, with a net line. Table: each component's mean this period, a year earlier, and the change all day, 10:00–15:00 and 17:00–21:00. All 30 region × period combinations run in 0.2–0.5 s with no missing slots in either window; regional changes sum to the NEM change.

**Decided.**
- Year-earlier window is 52 weeks back so 7D compares the same weekdays; YTD uses the same calendar span a year earlier.
- Pump load is its own checkbox (default off): it is storage charging like the batteries, and Tumut 3 alone pumps up to about 590 MW.
- The form posts back to `/` with `HX-Request`, which returns only the content; the URL carries every setting, and `v=1` marks a submitted form so unticked boxes are not replaced by defaults.

**Found (NSW, 30 days to 5 Nov 2026 vs a year earlier).** At 10:00–15:00 operational demand −361 MW and rooftop PV +799 MW, battery charging +784 MW, pump load −168 MW, spot +$119/MWh (midday floor from about $10 to about $75). At 17:00–21:00 battery discharge +643 MW, spot −$33/MWh. The view is demand-side only; supply changes (outages, fuel prices, bidding) are not in it.

**Open.** Price panel uses the mean, so single spike days (15:00 this year) dominate a slot; a median toggle would separate the floor from the spikes. Legend sits to the right and is cramped at phone width. Whether either prototype goes into `app.py`.

## 2026-11-05 (morning) — Battery time-of-day prototype (port 5097)

**Asked.** A time-of-day view of battery charge and discharge, with period (7D/30D/90D/YTD/1Y) and region (NEM, states) selectors and charge/discharge prices on a second axis. Build it standalone first; it may be added to the main dashboard later.

**Produced.** `src/aemo_dashboard/web/battery_tod.py` and `battery_tod_app.py`. 48 half-hour slots from scada30 × duid_mapping (fuel Battery Storage): average discharge MW above zero, charge MW below. Charge and discharge prices are volume-weighted (each DUID's MW × own-region RRP ÷ MW) per slot; the spot reference line is regional RRP, demand-weighted for NEM. Tiles: GWh charged and discharged, discharge ÷ charge, average charge and discharge price, spread, net energy revenue. Queries run in 0.05–0.2 s for a year, so no caching. Served from tmux session `battery_tod`: `uvicorn battery_tod_app:app --host 0.0.0.0 --port 5097`.

**Decided.**
- Slots are labelled by interval end, like the rest of the dashboard; the 00:00 label is plotted at 24:00.
- A slot's charge or discharge price is left off the chart when that side runs below 10% of its peak MW (`MIN_VOLUME_SHARE`). Over 90 days the NEM fleet discharges about 70 MW at 15:00 and earns about $800/MWh on it, which set the price axis and flattened the evening prices. The tiles still include every MWh.
- TAS dropped from the region pills: duid_mapping has no TAS batteries.

**Corrected — do not undo.** The plot div id is unique per render. With a fixed id, htmx's settle step copied the old element's attributes onto the new div and Plotly drew a garbled chart after a pill click (unstyled modebar, axis titles spread down the page). `app.py` already uses timestamped ids for the same reason.

**Open.** Whether and where to add it to `app.py` (Batteries subtab or Generation mix › Time of day). No tests yet in `tests/web/`.

## 2026-10-23 — NEMweb publishing stall 12:30–13:35; collector caught up; scada30 label rebuilt

**Asked.** dash_services emailed stale-data alerts (scada5, prices5, transmission5 over 45 min). Is it the collector or AEMO? Then: poll every 15 min, make sure the backfill lands, run the integrity check.

**Found.**
- Collector healthy: cycles every 5 min, no errors, DB matched the newest NEMweb files (DispatchIS 12:30, Dispatch_SCADA 12:25 NEM time). Every NEMweb Current feed (DispatchIS, SCADA, TradingIS, P5MIN, PredispatchIS, Dispatch_Reports, Operational_Demand, Market_Notice) stopped at the same point, seen identically from .71 and the Studio.
- The aemo.com.au NEM dashboard kept updating (13:25): it is fed by the visualisations API (`visualisations.aemo.com.au/aemo/apps/api/report/ELEC_NEM_SUMMARY`), which had 13:25 while NEMweb had 12:30. Open Electricity (NEMweb-fed) stopped at 12:25 and forward-fills its price after that. No market notice about the stall.
- NEMweb resumed by 14:40 AEDT (13:40 NEM) with every 12:35–13:35 file present. The collector worked through the backlog at 12 files a cycle; all 5-minute tables complete from 12:00 by 14:48.
- scada30 12:30 was left empty: the 12:30 scada5 file arrived in a cycle after the 13:00 and 13:30 endpoints were built, and `collect_30min_scada` only looks past `MAX(scada30)`. The integrity repair did not rebuild it either (it rebuilds a derived label only when it inserted a 5-minute row).

**Produced.** Integrity run 14:51 (`--email-policy always`, emailed): 0 recovered, scada30 1 and rooftop30 1 remaining. Rebuilt scada30 12:30 by hand under `acquire_primary`: AVG(scadavalue) over 12:05–12:30 per DUID, anti-join insert, 517 rows (all six scada5 intervals had 517 DUIDs). rooftop30 13:30 arrived on its own with the next rooftop file. Check-only at 15:08: every table 0 missing except rooftop30 20 Oct 03:00, which predates the stall, is absent from AEMO's file, and will go to the ledger once older than 4 days.

**Decided.** No fallback to the visualisations API: it has regional price and demand but no unit SCADA, and this was a one-hour stall.

**Open.** Fix the scada30 catch-up bug: build any endpoint whose six scada5 intervals are complete and which has no scada30 row, rather than only endpoints past the max; or have `gap_repair` survey derived labels against complete 5-minute windows. prices30/transmission30 re-aggregate a trailing window each cycle and were complete this time.

## 2026-10-13 (evening) — futures refresh; 2029–30 contracts restored

**Asked.** Merge the latest NEM-Review weekly futures export into `futures.csv`.

**Found.** The export (and the 29 Sep one) carries the 16 newest contracts (NSW/QLD/SA/VIC 2029 Q3–2030 Q2) only as daily 17:00 rows; the weekly 00:00 rows leave them blank. The 29 Sep update kept only weekly rows, so `futures.csv` had no 2029–30 prices after 13 Aug. NEM-Review's weekly value stamped Sunday D is the mean of the Mon–Fri trading days before D: rebuilt from daily rows it matched the stored 2 Aug and 9 Aug values exactly for all 16 contracts. Each export also ends with a part-week row stamped on the export date (30 Jul Thu, 13 Aug Thu, 29 Sep Tue were in the file next to the full weeks).

**Produced.** `~/futures_updates/build_futures_update.py` builds `futures_update_2026_10_13.csv`: export weekly rows to 11 Oct plus weekly means of the daily rows from both raw exports, 16 Aug–11 Oct (9 weeks, 5 days each). Backup `futures.csv.bak_20261013`; dropped the 30 Jul, 13 Aug and 29 Sep part-week rows; merged with `update_futures.py`: 318 → 320 rows, to 11 Oct 2026. Raw export kept as `nemreview_raw_2026_10_13.csv.raw`. NSW 2029 Q3 108.00 and NSW 2027 Q1 101.84 for the week to 11 Oct. Dashboard reads the CSV per request, no restart.

**Open.** David is checking whether NEM-Review can export those contracts weekly. Six older mid-week rows (12 Mar, 18 Mar, 30 Apr, 8 May, 5 Jun, 3 Jul 2026) are the same part-week kind and were left in.

## 2026-10-13 (afternoon) — integrity check extended to all 12 base tables, daily, no collector stop

**Asked.** The outage at 01:05-07:10 left holes in bdu5, demand30, rooftop30 and curtailment_regional5 that the weekly check did not survey, and two bugs in its collector stop/start. Extend survey and repair to every collector-written table, fix the lock handling, run it daily.

**Found.**
- Production held 458 bdu5 and 512 curtailment_regional5 missing intervals, nearly all isolated singles and short runs from Jan-Oct 2026 (Feb-Mar worst), none of them surveyed before. rooftop30 had 178 (172 from Aug-2024 to Aug-2025, before nemweb ARCHIVE starts), demand_less_snsg had 20 NULL rows from 11 Oct, and scada30 had 2 absent labels.
- `pgrep -f unified_collector_duckdb` matched the ssh shell that ran the script, and `stop_collector(150)` is shorter than one collector cycle. The start command typed after a failed stop queues in the collector's terminal.
- The collector closes its DuckDB connection for the 4.5 min sleep and retries a lock conflict for 30 s (`_open_conn`), so stopping it is unnecessary.
- The host clock follows Sydney daylight saving (AEDT, UTC+11) while every settlementdate is AEST (UTC+10). The old survey compared `datetime.now()` to NEM timestamps, which made each table look an hour behind from October to April. The new survey uses `nem_now()`; `tests/test_gap_repair.py` was changed to match.
- Operational_Demand_Less_SNSG CURRENT holds every file since 30-Jan-2025; Next_Day_Dispatch CURRENT holds one daily zip back to Sep-2025 (the monthly ARCHIVE zips are 230 MB); ROOFTOP_PV MEASUREMENT files are stamped 30 minutes after the interval they hold.

**Produced.** Repo commit 14b2dde (`aemo-data-updater`): per-table config in `gap_repair.py`; survey of all 12 tables plus the SNSG column; repair from CURRENT/ARCHIVE (MMSDM for prices5/scada5 only); anti-join inserts; `acquire_primary()` replaces stop/start; precise collector match and a 600 s stop wait kept behind `--stop-collector`; known-unreachable ledger; email policy (Sunday always, other days only when something was missing); 124 offline tests in `tests/test_integrity_all_tables.py`. Live parsers were checked against production rows for the new feeds through both CURRENT and ARCHIVE (0 value mismatches). First production run inserted 2,245 bdu5, 2,515 curtailment_regional5, 30 rooftop30 and 516 scada30 rows and filled 20 demand_less_snsg; the collector was not touched. Crontab now `17 4 * * *` (backup `~/aemo_production/logs/crontab.bak_20261013`).

**Decided.**
- Lock by waiting for the collector's gap, not by stopping it: no keystrokes into its terminal, and the insert takes seconds against a 30 s tolerance. The replica-copy check (replica mtime equals primary mtime, no `.tmp`) stops a write landing inside the collector's close-then-copy window.
- Gaps older than 4 days that survive a repair go to the ledger (daily runs ignore them, Sunday retries them) instead of capping the survey window: the full-history survey takes under a second.
- curtailment5/curtailment_duid5 are repaired from Next_Day_Dispatch CURRENT only. The collector's parsers fuse download and parse, so `_parse_nextday` mirrors them line for line and a test runs both on one synthetic file.
- prices30/scada30/transmission30 are not downloaded; a (label, key) row is replaced only when a 5-minute row for it was newly inserted.

**Corrected — do not undo.** Survey time is NEM time. 30-minute labels are interval-end. Inserts are anti-joins; never reintroduce the DELETE-range-then-INSERT of the legacy backfill functions. The SNSG NULL check starts at the first non-NULL value and ignores the newest 3 h (the collector fills the column a cycle later).

**Open.**
- The first Sunday run (18 Oct) is the first full retry of the ledger.
- 8 failures and 4 errors already present in `tests/` (alerts SMTP sink, nemweb URL checks, `test_collectors.py` fixtures) are unrelated and untouched.
- The initial survey reads the replica, which can be up to one collector cycle stale after a repair; the after-survey reads the primary under the lock.
- `repair_after_outage.py`, `recalc_30min_duckdb.py` and the one-off scripts in `~/aemo_production` still use the shifted 30-minute bucket and DELETE-range inserts; none is scheduled.

## 2026-10-10 (evening) — PASA Now: live availability, region filter, supply-impact strip

**Asked.** The Now subtab showed 7,039 MW of coal out, which looked high: test it against actual generation. Then: fix "now", fix the region selector, and show the % of coal out in the selected region, to judge how far outages affect supply and price.

**Found.**
- SCADA agreed with the tab. All 10 coal units at 0 MW PASA availability generated 0 MW over 24 h, and the 4 partly derated units generated at or below their declared availability. Coal fleet 21,255 MW, output 12,616 MW at 18:30. GSTONE1 was at 0 MW but declared available (economic shutdown, correctly not counted).
- ST-PASA starts at the next trading day, so the "now" interval came from the previous day's run (about 30 h old). ER01 showed 66 MW available while at 0 MW.
- On Now, only the 7-day chart applied the region; the bars and units table were always NEM-wide.
- Live unit availability exists in `PDPASA_DUIDAvailability` on NEMweb Current: same columns as ST-PASA, half-hourly, run every 30 min, covering from now to about 04:00 two days ahead. DispatchIS and P5MIN carry no unit rows; `bids.duckdb` `bid_volume5.pasaavailability` lags (next-day bid file).

**Produced.** Outage monitor 3ba6e04: `collectors/pdpasa.py` keeps the latest run only in `outages_pdpasa.parquet` (scheduled every 0.48 h; not fed to the change detector). Dashboard da86780, 9f5d003: `pasa_data.combine_pasa` (PD rows, then ST rows after PD's last interval) feeds the Today tile and Now; region filter on the bars and table; `supply_impact()` strip with per-region coal capacity, coal out, coal % out, scheduled out, demand (`demand30`, latest 30-min) and price (`prices5`, NEM demand-weighted). 130 tests pass. At 18:55 AEST: coal out 7,105 of 21,255 MW (33%); NSW 4,625 of 8,305 MW (56%); scheduled out 10,293 MW, 45% of operational demand; NEM price 131 $/MWh (NSW 166).

**Decided.** Coal % out uses the same 50 MW threshold as the bars, and the denominator excludes mothballed/retired units. Demand is 30-min operational demand because there is no 5-min demand table.

**Open.** "Out as % of demand" sets scheduled MW out against demand, not against required reserve. A price-vs-%-coal-out history needs a stored PD/ST-PASA series, which is not kept (latest run only).

## 2026-10-10 — PASA tab rebuilt; MT-PASA collector fixed and history backfilled

**Asked.** The PASA tab had been a placeholder since the Panel → FastAPI move. Rebuild it: short-term outages affecting prices now (ST-PASA), plus extended outages from MT-PASA.

**Found.**
- The old Panel tab (`aemo-energy-dashboard2/src/aemo_dashboard/pasa/pasa_tab.py`) had five summary figures, outage bars by fuel (the chart the Today tile copied), a generator changes table with notice class and MT-PASA return date, and High Impact Outages tables.
- **MT-PASA collector bug since Feb 2026:** `outage_monitor/collectors/mtpasa.py` `save()` sorted on `RUN_DATETIME`, a column MT-PASA files do not have (they carry `PUBLISH_DATETIME`), so `drop_duplicates(['DUID','DAY'])` kept the existing row and every unit-day stayed at the first value seen (mostly the 9 Feb publish). Any MT-PASA return date shown before 10 Oct (old Panel tab, iOS outages router) was stale. Example: KPP_1 showed 750 MW NODERATINGS for October while it was out.
- ST-PASA has two availability columns. MAXAVAIL is 0 for recallable plant switched off for price reasons; PASA availability is the physical figure. The old tab mixed them.

**Produced.**
- Outage monitor (`~/aemo_production/outage_monitor`, now under local git: f711148 baseline, e37f284 fix + history, ac4ea23 backfill): the snapshot keeps the newest publish per (DUID, DAY); new change-compressed `outages_mtpasa_history.parquet` with `mtpasa_history.py:mtpasa_asof`; new files processed oldest first; backfill of the last publish of each day 1 Oct 2025 → 9 Oct 2026 (321 files, 465k rows, 0.4 MB). Change-detector state seeded so the first corrected run was quiet. Old snapshot kept as `outages_mtpasa.parquet.bak_20261010`.
- Dashboard (aemo-redesign 2f2e602 … bdcf87c): PASA tab with four subtabs; 118 tests in `tests/web` pass. Definitions in SYSTEM.md "Dashboard definitions". Slippage is computed once per worker per history change and warmed at startup in a background thread (cold first load was ~8 s; cached 0.06 s).
- Numbers at 10 Oct (ST-PASA 09:00, MT-PASA 9 Oct 18:00): 10,036 MW of scheduled plant out now (coal 7,105 MW); 45 of 72 tracked outages changed return date over the window; TUNGATIN went from a 2 Sep 2027 return to open-ended this week (unplanned); ER02 11 Oct → 7 Nov; ER01 back 24 Oct; KPP_1 back 20 Oct; 79 units with 7+ day outages ≥ 100 MW in the next 12 months; weekly MW out peaks at 8–9 GW in Oct–Nov 2026.

**Decided.**
- MW out = `duid_mapping` capacity − ST-PASA PASA availability, scheduled fuels only, threshold 50 MW. Wind and solar (report 0) and batteries (rating noise) excluded.
- Planned/Unplanned comes from the MT-PASA unit state (AEMO's own tag), not from the old change-log notice class. The state for "today" comes from the first MT-PASA day on or after today, because a publish starts about 2 days after its publish date.
- Mothballed and retired units are footnoted, not counted (SNUG1 63 MW, POR01 50 MW).
- History holds one publish per day before 10 Oct (backfill) and every publish after.

**Corrected — do not undo.** The MT-PASA sort key is `PUBLISH_DATETIME`. Commit 8d279c2 accidentally added the two `app.py.bak.*` files; c98b522 untracks them (files still on disk, blobs in history).

**Open.**
- iOS API `api/routers/outages.py` (aemo-energy-dashboard2) still reads MT-PASA with the old logic; it now gets current data but has not been reviewed.
- The Transmission "Unplanned" section is empty because the 6 Oct High Impact Outages report leaves `Unplanned?` blank; check a later report.
- History rows for past days keep the state from the last publish that covered them (about 2 days before the day), which can make "Out from" early for long-running outages.
- `aemo-energy-dashboard2/src/aemo_dashboard/pasa/` (Panel) is no longer used by the web app; delete it once the iOS router is reviewed.

## 2026-10-08 (3) — Pump loads out of generation; weekly duid_mapping refresh; SYSTEM.md

**Asked.** Go ahead with excluding pump loads and with a scheduled `duid_mapping` refresh; check the standalone gauge too. Then write a system map, since David is the only user and Claude the maintainer.

**Produced.**
- Pump loads (PUMP1, PUMP2, SNOWYP, SHPUMP, KIDSPHL1/2) set to fuel NULL in `duid_mapping` (log `duid_mapping_history/pump_loads_null_20261008.csv`). Views already drop NULL fuel. Code fixes where NULL would still leak, in `aemo-energy-dashboard2` (commit `b97c85a`): iOS renewable gauge (`api/routers/gauges.py`), iOS evening peak (`api/routers/evening_peak.py`), web evening peak (`evening_peak/evening_analysis.py`). Test `tests/api/test_pump_loads.py`. The 10 other failures in `tests/api` existed before this change (stale fixture DB). Restarted windows 1, 2, 11.
- `PUMPED_HYDRO_DUIDS` corrected in both checkouts (`7c6fd10` in aemo-redesign) to TUMUT3, SHGEN, W/HOE#1, W/HOE#2, KIDSPHG1, KIDSPHG2, plus `PUMP_LOAD_DUIDS`; `~/aemo_production/data/pumped_hydro_duids.txt` rewritten (old copy in `duid_mapping_history/`).
- `aemo-data-updater` commit `e0fed35`: `src/aemo_updater/duid_registry.py`, `scripts/refresh_duid_mapping.py`, 58 tests. Cron Sunday 05:10, log `~/aemo_production/logs/duid_refresh.log`, emails RECIPIENT_EMAIL. Dry run 8 Oct: 0 inserts, 0 fills, 14 conflicts (mostly small water-utility batteries' storage, TB2B1, WDBESS1, SNB01, LIMBESS1; capacity on ADPPV1, CLOVER, SMTHBES1, PIONEER) — left for review.
- `start_services.sh` windows 2 and 3 pointed at `aemo_readonly.duckdb` (they were on `aemo_test.duckdb` at boot, readonly only after the 00:15 restart). Backup `start_services.sh.bak_20261008`.
- `docs/SYSTEM.md`: current-state map (checkouts, services, data flow, tables, mapping maintenance, cron). Protocol now says read it at session start and rewrite it when something changes.

**Decided.** NULL fuel rather than a new label for pump loads: a new label would have leaked into four live totals or series (web gauge total, `_generation_fuel_stats`, 5009 gauge, iOS generation stack); NULL needed three fixes. Pumping is not plotted anywhere — it is load, like battery charging, and hydro is now gross hydro output.

**Corrected — do not undo.** The live renewable gauges never used `PUMPED_HYDRO_DUIDS` (only the retired Panel gauge did), so renewable % was not missing conventional hydro. What was wrong in live numbers was pump consumption counted as `Water` generation: NSW hydro 7 Sep–7 Oct was 471 MW with pumps, 284 MW without. YoY hydro for that window is 1.49 → 1.27 GW, not the 1.84 → 1.46 quoted in chat on 7 Oct.

**Open.** Review the 14 refresh conflicts. Stale `tests/api` fixture DB. Weekly refresh's live write path runs for the first time on Sunday 11 Oct — check the email.

## 2026-10-08 (2) — duid_mapping audit against AEMO sources; battery time-weighting

**Asked.** Fix `duid_mapping` (not touched since the July Gen Info file); check every DUID, not just batteries; find a source fresher than the quarterly Gen Info.

**Sources.** (1) AEMO NEM Registration and Exemption List, sheet "PU and Scheduled Loads" (`https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls`, last-modified 22 Sep 2026): DUID, region, station, participant, fuel, reg/max MW, max storage MWh. (2) Gen Info July 2026 (`~/Downloads` on the Studio): nameplate MW (AC column), storage, commitment status. (3) MMSDM monthly `DUDETAILSUMMARY` (region, dispatch type) and `DUDETAIL` (REGISTEREDCAPACITY, MAXCAPACITY, MAXSTORAGECAPACITY), latest month 2026_08. The existing mapping's MW matches Gen Info nameplate for 91% of rows.

**Produced.** 33 rows corrected, 8 inserted (591 → 599). Edit list with old → new values: `~/aemo_production/data/duid_mapping_history/duid_mapping_edits_20261008.csv`; pre-edit table kept in the DB as `duid_mapping_bak_20261008`. Applied to `aemo_test.duckdb` between collector cycles; readonly picked it up next cycle. Rules: region from MMS (dispatch) > registration > Gen Info; battery MW = registration Max Cap generation; other MW = Gen Info nameplate (AC) > registration Reg Cap; storage = registration > MMS > Gen Info; names and owners from the registration list, trustee clauses stripped.
- Wrong plant behind the DUID (names had been guessed from the code): CGBESS01 Clements Gap SA (was "Castlereagh" NSW), TRGBESS1 Terang VIC (was "Taronga" NSW), LGAPBS1 Lincoln Gap SA (was "Logan" QLD), QPSFB1/2 Quorn Park NSW (was "QLD Solar Farm Battery"; QPSFB1 is solar), PUMP1/2 Wivenhoe pumps QLD (was Tumut 3 NSW), BRDDSF01 Broadsound QLD (was Berida NSW), GUSF1 Gunsynd QLD (was Gunnedah NSW), GESF1 → NSW, WNSF1 Wangaratta, SHOAL1 Shoalhaven Starches gas cogen 54 MW (was hydro 240 MW).
- Blank region / zero MW / zero storage filled: NESBESS1/2, MRNBESS1, STABESS1, SMFBESS1/2, PLBESS1, ORABESS1 (49 → 415 MW, 1,660 MWh), SWANBBF1, BUSF1, LANCSF1, MULWASF1, WANDSF2, CUSF1. ERB01 700 → 460 MW, 1,073 → 1,997 MWh; HPR1 storage 117 → 194; WKIEWA1 34 → 68 MW.
- Inserted (active batteries never mapped): MLB01, SNB02, WILLBES1, BRDDBES1, WOOLES1, ERB02, WOORB1, TB3B1.
- Batteries tab: Util % and $/MWh-cap/yr now divide by storage × hours each DUID was in the window (`n_intervals × 0.5`), so batteries commissioned mid-window are not diluted. Test added. 1Y after both fixes: NSW util 39 / $8,220, QLD 62 / $16,523, VIC 61 / $16,039, SA 56 / $29,148; 69 DUIDs, 27,462 MWh.

**Root cause of the missing rows.** The collector classifies a DUID once, on first sight (`_auto_insert_duid_mapping`), from the DUID string only; low-confidence guesses are skipped and never retried because `known_duids.txt` already holds the DUID. Rows it does insert have blank region, 0 MW, 0 MWh. `duid_exceptions.json` only silences alert emails.

**Not changed (deliberate).** Solar rows where the mapping holds registration Reg Cap rather than Gen Info AC nameplate (10–25% apart: CHILDSF1, CRWARP1, DAYDSF1, GNNDHSF1, HAYMSF1, KARSF1, KERNGSP1, MANNSF2, METZSF1, SRSF1, TB2SF1, WANDSF1, …) — convention, not error. Hydro peaks above registered MW (Tumut 3 1,789 vs 1,500; Poatina, Cethana) are overload capability. TB2B1 storage 84 kept (AEMO lists 41; public sources say ~2 h). 111 mapping rows have no SCADA in 12 months (retired); untouched. UWF1 (auto-classified 6 Oct, wind) is in no source yet.

**Open.**
- Pump loads are in the generation views as `Water`: PUMP1/2, SNOWYP, SHPUMP (and KIDSPHL1/2) report consumption as positive SCADA, and `generation_by_fuel_*` sums them as hydro output — about 166 MW average over 12 months. Needs a decision on how to exclude them (affects all hydro and renewable-share history).
- ERB01 storage is a single current value; it was smaller earlier in the window, so its 1Y cycles are understated.
- Replace the one-shot classifier with a scheduled refresh from the registration list + MMS (proposal in chat 8 Oct).

## 2026-10-08 — Batteries tab: Cap MW fix and column audit

**Asked.** Cap MW totals on the Batteries tab looked wrong (NSW 205 MW against 4,928 MWh); then a correctness audit of every column in that table.

**Produced.** `_battery_agg_node` now returns nameplate `cap_mw` (summed up the tree) instead of `storage_mwh / 24`. Util % keeps the storage/24 denominator, so it reads as cycles per day ×100. 3 tests in `tests/web/test_battery_capacity.py`. Deployed to 5008. 1Y region Cap MW now NSW 2,596, QLD 2,198, VIC 1,665, SA 1,077.

**Audit (1Y window to 8 Oct 2026).** Formulas check out: GWh = Σ max(±scada,0) × 0.5 h; $M = Σ MWh × regional RRP; $/MWh = volume-weighted; spread = disch − charge price; $/MWh-cap/yr = annualised (disch rev − charge cost) / storage MWh; NSW totals reproduce from an independent query. 30-min valuation vs 5-min (scada5 × prices5, last 90 days): net revenue within 1.5%, energy 2–4% low on 30-min. No missing prices. The errors are in `duid_mapping`, not the code:
- 7 battery DUIDs have no region (auto-classified Feb–Jul 2026: NESBESS1/2, MRNBESS1, STABESS1, SMFBESS1/2, PLBESS1), so they are dropped from the tab entirely (~38 GWh discharge).
- Zero storage_mwh on DUIDs that do dispatch: ORABESS1 (also cap 49, peaks 416 MW), CGBESS01, TRGBESS1, LGAPBS1, SWANBBF1, QPSFB1/2. Their GWh and $ count in region totals but not in the storage denominator, inflating region Util % and $/MWh-cap/yr (NSW ~15% of discharge, QLD ~11%).
- ERB01 cap 700 vs observed peak 460 (storage 1,073 = stage 1). QPSFB1 discharges 38 GWh with ~0 charging (behaves like solar); KEPBG1 never charges from grid (DC-coupled) so spread is overstated.
- Util % and $/MWh-cap/yr divide by the full window for DUIDs commissioned inside it (BUNGAMB1, CGBESS01, SWANBBF1, ...).
- Lollipop labels are station names per DUID, so two-DUID stations (Western Downs) appear twice.
- Generators tab (`_pivot_agg_node` ~line 4821) also shows storage/24 as Cap MW for batteries; not changed.

**Open.** Fill region/capacity/storage for the DUIDs above (needs David's go-ahead: `duid_mapping` feeds every tab); decide whether to show Util % as cycles/day.

## 2026-09-29 — Futures data refresh; financial years in the single-contract chart

**Asked.** Bring the futures data up to date; then add financial years to the Futures tab's contract dropdown, with the calculation adjusted.

**Produced.**
- Merged NEM-Review export (21 Jul–29 Sep) into `futures.csv`: 313 → 321 rows. Backup `futures.csv.bak_20260929`; merged file `~/futures_updates/futures_update_2026_09_29.csv`; raw export `nemreview_raw_2026_09_29.csv.raw` (non-`.csv` extension so the updater's newest-file scan skips it).
- Dropdown now has two optgroups, Financial years (slug `FY2027`) then Quarters (slug `2027-1`). Functions `_fy_quarters`, `_fy_contract_average`, `_futures_fy_available`; `_build_single_contract` takes a key `(year, q)` or `("FY", fy)`. 11 tests in `tests/web/test_futures_fy.py`. Deployed to 5008.

**Decided.**
- FY = Australian FY, FY2027 = Jul 2026–Jun 2027 = mean of Q3 2026, Q4 2026, Q1 2027, Q2 2027. Simple mean, not hours-weighted, to match the Cal+1/Cal+2 chart's `_cal_year_average`.
- Shown only once all four quarters have listed (no partial averages as quarters list). A quarter that has finished delivery stops trading about a week after its quarter ends; its last settlement is carried forward so the FY line runs through the year. The FY line ends when its last quarter stops trading.
- Only the weekly 00:00 rows of the export were merged. The export also carried daily 17:00 settlement rows; merging them would have switched the series from weekly to daily partway through.

**Corrected — do not undo.** First version carried expired quarters forward indefinitely, so FY2026 ran on past June 2026 as a flat line. Now clipped at the last quarter's last trade; a test guards it.

**Checked.** FY2027 NSW on 29 Sep 2026 = $86.33, equal to the hand mean of the four quarter columns. FY2024 ends in Jan 2023 because older quarter columns end there in `futures.csv`.

## 2026-09-26 — Compare regions subtab (Generation mix)

**Asked.** A subtab comparing the generation mix across regions, in absolute and share terms, with the usual range selectors. Then: mark each region's renewable share and VRE share on the share chart.

**Produced.** `/generation-mix/regions`, commits `f3ff15c` and `8bc8cab`, deployed to port 5008. Two horizontal stacked-bar charts:
- Energy by region and fuel over the window, GWh or TWh by size. NEM left out (it would dwarf TAS and SA). A label beside each bar gives the total, average GW and net interconnector imports.
- Share by region with NEM (sum of the five regions) on top. Black ticks mark the VRE share (wind + solar + rooftop) and the renewable share (VRE + hydro); both values are listed right of each bar.

Functions: `_regional_mix_energy`, `_regional_net_imports`, `_renewable_shares`, `_generation_regions_content`. 15 tests in `tests/web/test_generation_regions.py`.

**Decided.**
- Energy, not average GW, for the absolute chart (David's call). Always read from the 30-min tables; energy = Σ MW × 0.5 h. Verified `generation_by_fuel_30min` rows are per-interval MW means (equal to the mean of the six 5-min rows) and the table is on a clean 30-min grid in every year from 2020, where it starts.
- Stack order on this subtab is wind, solar, rooftop, hydro, battery, gas, coal, other, on both charts. This puts the VRE and renewable boundaries on segment edges. It differs from the Stack subtab, which keeps coal at the base.
- Battery counts discharge only and is not renewable. Hydro includes pumped-hydro output (the `Water` fuel type doesn't separate it), so RE is slightly high in NSW and QLD; the chart footnote says so.
- Every fuel is clipped at zero per interval before summing (small negative readings exist on "Other" in SA).
- Region pills hidden on this subtab (all regions shown); 1H range hidden.
- Net imports are shown as labels, not bar segments.

**Corrected — do not undo.**
- The new constant is `GENMIX_COMPARE_REGIONS`. The first draft named it `COMPARE_REGIONS`, which already exists at ~line 1837 for Prices › Period Compare and includes NEM; redefining it removed NEM from that subtab. A test guards this.
- Rooftop is filtered to the five parent regionids. `rooftop30` still holds QLDC/QLDN/QLDS/TASN/TASS rows before 2026, which double-count QLD and TAS. A test compares a March 2024 QLD value against a parent-only query.

**12-month values at deploy (to 26 Sep 2026):** VRE / RE share — NEM 39/46, NSW 37/43, QLD 34/36, VIC 39/43, SA 79/79, TAS 24/98. NEM generation including rooftop about 226 TWh.
