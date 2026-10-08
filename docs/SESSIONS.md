# Dashboard and data collector — session log

Covers the NEM dashboard (FastAPI/HTMX app on .71) and the AEMO data collector. The price forecasting system keeps its own log in `itk_lp_model/docs/sessions_history.md`.

**Protocol.** Start any dashboard or collector session by reading `docs/SYSTEM.md` (how the system runs now), then the state block and the last two entries here. When a session changes how something runs, rewrite the affected part of SYSTEM.md. End it by appending an entry at the top of the log and rewriting the state block. Each entry is a handoff: what was asked, what was produced, what was decided and why, what was corrected and must not be undone, what is open.

## State now (8 Oct 2026)

- **Dashboard code:** `~/aemo-redesign` on .71 (davidleitch@192.168.68.71), branch `web-dashboard-redesign`, remote `davidleitch1/aemo-energy-dashboard2`. The whole app is one file, `src/aemo_dashboard/web/app.py` (~9,000 lines). Tests for the web app are in `tests/web/`; run with `/Users/davidleitch/aemo_production/aemo-energy-dashboard2/.venv/bin/python -m pytest tests/web -q` from the repo root.
- **Live service:** tmux `services:1`, `uvicorn app:app --port 5008 --workers 4`, run from `src/aemo_dashboard/web`. No `--reload`, so code changes need a restart of that window. The launch line exists in both `~/tmux_files/start_services.sh` (boot) and `~/tmux_files/restart_dashboards.sh` (nightly 00:15); a change to how it launches must go in both, plus the process signature in `monitor_services.py`.
- **Testing a change before deploy:** run a second uvicorn from the same directory on port 5099, check it at `http://192.168.68.71:5099/...`, then kill it and restart `services:1`.
- **Collector:** `~/aemo_production/aemo-data-updater` on .71, tmux `services:0`.
- **Generation mix subtabs:** Yr on yr, Stack, Compare regions, Time of day, Trends, Transmission.
- **Futures data:** `~/aemo_production/data/futures.csv`, weekly Sunday 00:00 rows, runs to 29 Sep 2026. Update by dropping a NEM-Review export in `~/futures_updates/` and running `~/aemo_production/data/update_futures.py` (merge: union of dates and columns, new wins). The Monday launchd job `com.aemo.update-futures` has not run since 4 May 2026.
- **duid_mapping:** audited 8 Oct against AEMO sources; weekly refresh (cron Sun 05:10) fills gaps and emails conflicts — first live run 11 Oct. Pump loads carry fuel NULL and are out of all generation totals. **Open:** 14 refresh conflicts to review; stale `tests/api` fixture DB.
- **Open:** the futures launchd job is dead; not investigated (it only merges a file dropped by hand, so manual runs lose nothing).
- **Open:** whether to relabel Batteries-tab Util % as cycles a day (it is discharge ÷ storage per day × 100).

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
