# NEM Dashboard contents

Inventory of every tab, card, plot, and table in the AEMO dashboard
(`src/aemo_dashboard/web/app.py` on the `web-dashboard-redesign` branch,
running on `.71:5008` behind the public Cloudflare tunnel).

This doc serves three purposes:

1. **Coverage tracker** — what's built, what's placeholder, when to update.
2. **Attribution registry** — data-source label per card.
3. **Calculation contract** — formula for every derived metric, so users
   and engineers can interpret numbers correctly.

The final section (**Calculation reference**) collects every formula
the dashboard uses in one place. Card descriptions cite it by name.

**Attribution convention**: each visible card carries an italic footer
`Data: <source> · plot: ITK` from the `_attribution()` helper. Sources:
`AEMO` (most cards), `AEMO STTM` (gas), `Global-Roam` (futures),
`Global-Roam + AEMO` (Cal+2-vs-spot overlay).

---

## Tab nav order

1. Today
2. Generation mix (subtabs: Yr on yr / Stack / Time of day / Trends / Transmission)
3. Evening peak
4. Prices (subtabs: Price Analysis / Price Bands)
5. Batteries
6. Futures
7. Generators (formerly "Pivot table")
8. Curtailment
9. PASA *(placeholder)*
10. Gas

**Deep-link only** (not in top nav): Station Analysis
(`/station-analysis?duid=…` or `?station=…` or `?fuel=…&region=…&owner=…`).

---

## Today

Tile-based grid (3×3). Page-level attribution sits below the grid in
`today_body.html`. Each tile loads lazily via HTMX and refreshes every
5 minutes.

- [x] **Page footer** (`today_body.html`) — `Data: AEMO · plot: ITK`
- Tiles (page footer covers attribution):
  - **Renewable gauge** (`/tile/renewable-gauge`) — gauge needle at
    current renewable %. See [Renewable share](#renewable-share).
  - **Generation mix mini** (`/tile/generation-mix`) — 24h stacked area
    by fuel. See [Generation by fuel](#generation-by-fuel).
  - **Demand gauge** (`/tile/demand-gauge`) — current total demand vs
    hour-of-day all-time record. See [Total demand](#total-demand).
  - **Price chart** (`/tile/price-chart`) — last 24h regional spot
    prices, optional LOESS smoothing.
  - **Price table** (`/tile/price-table`) — VWAP by fuel × region over
    last 24h. See [VWAP](#vwap).
  - **Battery SoC** (`/tile/battery-soc`) — NEM-wide state of charge
    vs 30-day rolling max capacity. See [Battery SoC](#battery-soc).
  - **Forecast chart** (`/tile/forecast-chart`) — AEMO predispatch +
    ST PASA price/demand outlook.
  - **Market notices** (`/tile/market-notices`) — live AEMO market
    notices (external HTTP fetch, cached 120 s server-side).
  - **Outages summary** (`/tile/outages-summary`) — generator outage
    fuel breakdown from PASA (external fetch, cached 120 s).

## Generation mix · Yr on yr

- [x] **Year on year fuel + price table** — annualised TWh, VWAP, deltas
  vs same window one year ago. See [Annualised TWh](#annualised-twh)
  and [VWAP](#vwap). Total row uses [demand-weighted TWAP](#demand-weighted-twap).
  Battery row marked with `*` because for batteries it shows
  [discharge − charge spread](#battery-net-spread). — `Data: AEMO`

## Generation mix · Stack

- [x] **Generation by fuel stack** — stacked area in GW; bucket size
  auto-picked from range (5-min ≤ 24h, 30-min ≤ 7d, daily above).
  See [Generation by fuel](#generation-by-fuel) and
  [Transmission flows](#transmission-flows). — `Data: AEMO`
- [x] **Price chart** (paired below stack) — regional RRP over the
  same window. LOESS smoothing for ≤ 7-day windows; raw above. — `Data: AEMO`
- [x] **Fuel stats table** — per-fuel TWh, [VWAP](#vwap), share.
  For NEM regions, output is priced at each region's RRP. — `Data: AEMO`

## Generation mix · Time of day

- [x] **Hour-of-day generation + price** — fuel × 24-hour-bucket mean
  MW, paired with mean spot price (regional RRP for single region;
  demand-weighted for NEM). See [Demand-weighted price](#demand-weighted-price). — `Data: AEMO`

## Generation mix · Trends

- [x] **VRE production chart** — annualised TWh of selected VRE fuel,
  last 3 calendar years overlaid on day-of-year. 30-day EWM smoothing.
  — `Data: AEMO`
- [x] **Renewable generation by fuel** — stacked area in TWh over time. — `Data: AEMO`
- [x] **Thermal vs renewables share** — % over time, smoothed. — `Data: AEMO`
- *(Methodology note card — text only, no attribution)*

## Generation mix · Transmission

- [x] **Transmission flows time series** — per-interconnector flows,
  signed so positive = imports into selected region. See
  [Transmission flows](#transmission-flows). — `Data: AEMO`
- [x] **Transmission by hour of day** — mean flow per hour. — `Data: AEMO`

## Evening peak

- [x] **4-panel evening peak chart** — load shape, prices, fuel mix,
  marginal-fuel mix for this year's evening peak window vs same window
  prior year. Annualised over selected days. — `Data: AEMO`
- [x] **% composition card** — stacked-bar % share of generation
  across the entire evening-peak window. — `Data: AEMO`

## Prices · Price Analysis

- [x] **Spot prices chart** — regional RRP line over the selected
  window with optional LOESS smoothing. — `Data: AEMO`
- [x] **Price statistics table** — per-region mean / quartiles /
  max plus per-fuel [VWAP](#vwap). — `Data: AEMO`
- [x] **Time of day chart** — mean raw price by hour-of-day. — `Data: AEMO`

## Prices · Price Bands

- [x] **Price band butterfly chart** — left bars = % of time RRP fell
  in each band; right bars = each band's $/MWh contribution to the
  flat-load average. See [Price bands](#price-bands). — `Data: AEMO`
- [x] **Price contribution by band (stack)** — per-region stacked bars;
  total bar height = regional mean RRP. — `Data: AEMO`
- [x] **Price band details table** — % time, average $/MWh, and
  [revenue](#price-band-revenue) per band per region. — `Data: AEMO`

## Gas

- [x] **STTM ex-post gas price** — daily ex-post hub prices ($/GJ) for
  SYD / BRI / ADL plus computed STTM Avg (unweighted mean of the three).
  Display clipped at $100/GJ for outliers; raw price in hover. — `Data: AEMO STTM`
- [x] **Total STTM gas demand** — 7-day rolling mean of summed daily
  network allocation across hubs, last 3 calendar years overlaid by
  day-of-year. — `Data: AEMO STTM`
- [x] **STTM gas demand by hub** — same year-overlay structure per hub. — `Data: AEMO STTM`

## Generators

- [x] **Generators pivot table** — Tabulator tree with three
  ordered group dimensions (Region / Fuel / Owner), Station always
  implicit above DUID. Columns: GWh / Rev $M / $/MWh / Util % / Cap MW.
  See [Generators metrics](#generators-metrics). Discharge-only
  convention for batteries (positive scadavalue only). Group rows roll
  up totals so collapsed groups stay informative. — `Data: AEMO`

## Batteries

- [x] **Batteries pivot table** — battery-specific column set:
  Disch GWh / Chrg GWh / Disch $M / Chrg $M / Disch $/MWh /
  Chrg $/MWh / Spread $/MWh / **$/MWh-cap/yr** / Util % /
  Cap MW (effective = storage/24) / Storage MWh.
  See [Battery metrics](#battery-metrics). Group dims locked to
  Region / Owner (fuel pre-filtered to Battery Storage). Row
  anchors link to /station-analysis. — `Data: AEMO`
- [x] **Battery ranking lollipop** — horizontal lollipop, top-N
  batteries by selected metric. Default metric = `$/MWh-cap/yr`
  (annualised arbitrage revenue per MWh of storage; see
  [Battery investment metric](#battery-investment-metric)). Marker
  colour = region. Top-N selector (10/20/50/All). — `Data: AEMO`

## Station Analysis (deep-link only)

- [x] **Output and price time series** — dual-axis chart: generation
  MW (left, green) and RRP $/MWh (right, red). Capacity reference
  line dashed at effective capacity. Resolution tiering: 5-min
  for ≤ 7 d, 30-min for 7–30 d, daily mean above. Price axis
  flips to log when [spike threshold](#log-axis-trigger) crossed.
  For batteries scadavalue stays signed (negative = charging). — `Data: AEMO`
- [x] **Average performance by hour of day** — mean generation MW +
  mean RRP $/MWh by hour-of-day (0-23). Always 30-min sourced,
  independent of TS chart resolution. — `Data: AEMO`
- [x] **Stats strip** — for non-battery subjects: GWh / Rev $M /
  $/MWh (VWAP) / Util % / Cap MW. For battery subjects: 12-tile
  battery-economics surface (Disch GWh, Chrg GWh, Disch $M, Chrg $M,
  Disch $/MWh, Chrg $/MWh, Spread $/MWh, $/MWh-cap/yr, Util %, RT
  efficiency %, Cap MW eff, Storage MWh).
  *(Stats tiles only — no per-card attribution.)*
- [x] **Battery daily trend card** (battery subjects only) — daily-
  aggregated trend chart with primary + optional secondary metric on
  dual y-axes. Metrics: Spread $/MWh, Cycles/day, Disch/Chrg GWh/day,
  Disch/Chrg $/MWh, Net rev $K/day, RT efficiency %, Util % daily.
  Smoothing pill (Raw / 7-day / 30-day / 90-day). Designed for
  questions like "spread shrinks as more batteries come online". — `Data: AEMO`

## Futures

- [x] **Forward curve (per region)** — three snapshots (today /
  3 months ago / 1 year ago) of each future-quarter contract's price.
  Shows curve drift. — `Data: Global-Roam`
- [x] **Calendar-year forward averages** — Cal+1 and Cal+2 over time.
  See [Cal+1 / Cal+2](#cal1-cal2). Endpoint markers carry the value. — `Data: Global-Roam`
- [x] **Cal+2 forward vs trailing spot** — Cal+2 forward line vs 52-week
  rolling spot. Spread reads as forward risk premium. — `Data: Global-Roam + AEMO`
- [x] **Single contract (all regions)** — NSW/QLD/SA/VIC overlaid for
  the selected quarterly contract. — `Data: Global-Roam`

## Curtailment

- [x] **Stats strip** — Total curt MWh, Rate % (vs UIGF), Econ MWh + %,
  Grid MWh + %, Peak MW, # DUIDs curtailed. See
  [Curtailment](#curtailment-1) and [Classification](#curtailment-classification). — `Data: AEMO`
- [x] **Curtailment over time** — stacked area with up to 4 traces
  (Solar+economic, Solar+grid, Wind+economic, Wind+grid). Auto
  resolution. — `Data: AEMO`
- [x] **Regional comparison table** — per region: Total MWh / Rate % /
  Solar MWh + % / Wind MWh + % / Econ MWh / Grid MWh. — `Data: AEMO`
- [x] **Top curtailed DUIDs table** — DUID anchors link to Station
  Analysis. Top-N pills (10 / 20 / 50 / All). — `Data: AEMO`

## Placeholders (not yet built)

- PASA — production code at `src/aemo_dashboard/pasa/pasa_tab.py`.
- (Coal / Coal Evolution — handled separately, see
  [dashboard_redesign_gaps.md](../memory/dashboard_redesign_gaps.md).)

When PASA lands, add a section per card here.

---

# Calculation reference

Formulae used across the dashboard, in alphabetical order. Variable
names match the SQL columns or Python locals where possible.

## Annualised TWh

Used to compare partial-window generation across different period
lengths.

```
annualised_twh = window_mwh × (365 / window_days) / 1,000,000
```

For a 30-day window with 6,200 GWh of generation:
`6,200,000 × (365 / 30) / 1,000,000 ≈ 75.4 TWh/year`.

## Battery investment metric

The **annualised spread revenue per MWh of installed storage**.
Sized against capex per MWh to assess payback before opex /
degradation.

```
spread_revenue_mwh   = discharge_revenue − charge_cost
annualised_spread    = spread_revenue × (year_hours / window_hours)
                     where year_hours = 365 × 24 = 8,760
spread_per_mwh_cap_yr = annualised_spread / storage_mwh
```

Example: $500/kWh battery ⇒ $500,000/MWh capex. For a 10% gross
return you need **$50,000 / MWh-cap / yr**. The lollipop chart
sorts batteries by this metric and the $50K line is the
"would justify a 10% return on a $500/kWh build" threshold.

## Battery metrics (pivot + station-analysis stats)

All sums are over (DUID × interval) tuples in the window. `h` =
interval hours (0.5 for 30-min source, 5/60 for 5-min source).

```
discharge_mwh     = SUM( GREATEST(scada, 0) ) × h
charge_mwh        = SUM( GREATEST(-scada, 0) ) × h
discharge_rev_$   = SUM( GREATEST(scada, 0)  × rrp × h )
charge_cost_$     = SUM( GREATEST(-scada, 0) × rrp × h )
discharge_$_per_MWh = discharge_rev_$ / discharge_mwh        (VWAP, sell side)
charge_$_per_MWh    = charge_cost_$   / charge_mwh           (VWAP, buy side)
spread_$_per_MWh    = discharge_$_per_MWh − charge_$_per_MWh
RT_efficiency_pct   = discharge_mwh / charge_mwh × 100
cycles_per_day      = discharge_mwh / storage_mwh            (per day, when
                       resampled daily)
effective_cap_mw    = storage_mwh / 24                        (one-cycle-per-day
                       equivalent MW)
util_pct            = discharge_mwh / (effective_cap_mw × window_hours) × 100
```

Each DUID is priced at **its own region's RRP** (so multi-region
battery fleet selections compute correct revenues / costs).

### Battery net spread (Yr-on-yr table only)

For Battery rows on the Generation-mix Yr-on-yr table, the `$/MWh`
column shows `discharge_$_per_MWh − charge_$_per_MWh` rather than a
single-side VWAP. Marked with `*` in the table.

## Battery SoC

NEM-wide battery state-of-charge at the latest BDU dispatch interval.

```
stored_mwh         = SUM(bdu5.energy_storage) at latest settlementdate,
                      mainland only (TAS excluded — NaN)
stored_1h_ago_mwh  = same, at settlement_date closest to (latest − 55 min)
capacity_mwh       = 30-day rolling MAX of the above SUM
```

## Cal+1 / Cal+2

Calendar-year forward averages from ASX base-load futures:

```
Cal+1 = mean( Q1, Q2, Q3, Q4 of (current_year + 1) ) settlement prices
Cal+2 = mean( Q1, Q2, Q3, Q4 of (current_year + 2) ) settlement prices
```

Plotted as time series — each x-axis point is one weekly settlement
date; the y value is the mean of the four contracts available at
that settlement.

## Curtailment

```
curtailment_mw   = UIGF − cleared       (already pre-computed
                   in curtailment_regional5.solar_curtailment /
                   wind_curtailment / total_curtailment)
curtailment_mwh  = SUM(curtailment_mw) × (5/60)   (5-min source)
curtailment_rate_pct = curtailment_mwh / uigf_mwh × 100
```

UIGF = Unconstrained Intermittent Generation Forecast — what a
wind / solar plant could have produced absent dispatch constraints.

## Curtailment classification

Per-interval per-region classification of each MW of curtailment
by joining `curtailment_regional5` (or `curtailment_duid5`) to
`prices_5min` on `(settlementdate, regionid)`:

```
economic curtailment  if regional_rrp ≤ $0 / MWh
   (oversupply / negative pricing made the plant stop voluntarily,
    or the dispatch optimiser wound it down)

grid curtailment      if regional_rrp > $0 / MWh
   (forced curtailment despite a positive price — transmission
    or system constraint)
```

Threshold lives in two `CASE WHEN rrp ≤/> 0` expressions in the SQL.
Easy to refine to e.g. `≤ $30` if "low-price economic" is wanted
as a third category.

## Demand-weighted price

Used on the time-of-day chart for the NEM (all-region) case and on
the Station Analysis chart for multi-region fleet selections.

```
demand_weighted_price = SUM(rrp × demand) / SUM(demand)
                        across the regions in scope at each interval
```

Demand comes from `demand30.demand` (operational demand only;
rooftop deliberately excluded because it doesn't pay or get the
RRP).

## Demand-weighted TWAP

Used as the "Total" row's `$/MWh` figure on the Yr-on-yr table.
Same formula as [demand-weighted price](#demand-weighted-price) but
accumulated across every interval in the window:

```
twap = SUM(rrp × demand × interval_h) / SUM(demand × interval_h)
```

## Generation by fuel

```
generation_mw = SUM(scada5.scadavalue) over (DUID × interval × region)
                grouped by (settlementdate, fuel_type)
```

Fuel labels come from `duid_mapping.fuel`. The display layer
collapses CCGT / OCGT / Gas other → "Gas" and Water → "Hydro".

Rooftop is added via a join to `rooftop30.power` (30-min cadence)
forward-filled onto the 5-min generation grid. Filter
`regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')` to avoid the
historical sub-region double-count.

## Generators metrics (pivot)

Same as [Battery metrics](#battery-metrics) but on the discharge
side only (the `GREATEST(scada, 0)` floor is a no-op for fuels that
never produce negative values):

```
gwh         = discharge_mwh / 1,000
rev_$M      = discharge_rev_$ / 1,000,000
$_per_MWh   = discharge_$_per_MWh                  (VWAP)
util_pct    = discharge_mwh / (effective_cap_mw × window_hours) × 100
cap_mw      = SUM(effective_cap_mw)
```

Effective cap for batteries is `storage_mwh / 24`; for everything
else it's `capacity_mw` from `duid_mapping`.

Group-row totals **pool the numerator and denominator first**, then
derive ratios. (Do not average the children's averages.)

## Log axis trigger

The Station Analysis time-series price axis (right) flips to log when
any of:

```
max(price)        > $1,000 / MWh
max(price) / p95  > 5
max(price) / p50  > 20
```

Negative prices silently drop on the log axis (plotly behaviour). The
trigger is in `_price_axis_should_log`.

## Price bands

Buckets of RRP used on the Prices · Bands subtab. Default boundaries
match production:

```
< $0          (negative)
$0  - $30
$30 - $100
$100 - $300
$300 - $1,000
> $1,000
```

For each region × band:

```
% time         = (# intervals in band) / total_intervals × 100
avg_$_per_MWh  = AVG(rrp) for intervals in band
contribution_$_per_MWh = (% time / 100) × avg_$_per_MWh
revenue_$      = SUM(rrp × regional_demand × 0.5h)  (intervals in band)
```

Bar widths on the butterfly are in `% of time` and `$/MWh contribution`
respectively so they read on the same scale.

### Price band revenue

Per-region per-band gross revenue across the window:

```
revenue = SUM(rrp × demand × interval_hours)
          for intervals where rrp falls inside the band
```

## Renewable share

Used on the Today renewable gauge and the iOS `/v1/gauges/today`
renewable section.

```
total_gen  = SUM(scada > 0 for all generating fuels except Battery Storage,
                 Transmission) + rooftop_mw
hydro_pct    = SUM(scada where fuel='Water')  / total_gen × 100
wind_pct     = SUM(scada where fuel='Wind')   / total_gen × 100
solar_pct    = SUM(scada where fuel='Solar')  / total_gen × 100
rooftop_pct  = rooftop_mw                     / total_gen × 100
renewable_pct = hydro_pct + wind_pct + solar_pct + rooftop_pct
```

`rooftop_mw = SUM(rooftop30.power)` at the latest 30-min bucket on
or before the generation timestamp, filtered to the five main NEM
regions.

## Total demand

NEM-wide instantaneous total demand:

```
total_demand_mw = SUM(demand30.demand) + SUM(rooftop30.power)
                  at latest demand30 settlementdate,
                  region IN ('NSW1','QLD1','VIC1','SA1','TAS1')
```

`demand30.demand` is operational demand; rooftop self-generation
served on the customer side is added back to get a total-system
demand figure (consistent with AEMO's "operational demand +
rooftop" view).

## Transmission flows

```
interconnector_flow = AVG(meteredmwflow) per interconnector per interval
```

The flow is signed per AEMO's "from"/"to" convention. The dashboard
chart re-signs so that **positive = imports into the selected region**.

Per-region INTERCONNECTOR_MAP definitions live in `app.py`.

## VWAP

**Volume-Weighted Average Price** — the average price each MWh of
generation earned, weighted by generation:

```
vwap = SUM(rrp × generation_mw × interval_hours)
       / SUM(generation_mw × interval_hours)
```

Equivalent to `revenue / generation_mwh`. Distinct from the **flat-load
mean** (simple time-weighted mean of RRP) — VWAP captures the
correlation between when a plant generates and when prices are high.

For multi-region selections each DUID is priced at its own region's
RRP (rather than a demand-weighted reference). This keeps revenue
totals consistent with what each plant actually earned.

## Window resolution tiering

Most time-series cards auto-pick resolution based on window length
to keep Plotly under ~2,000 points per series:

| Window length | Source | Resample |
|---|---|---|
| ≤ 24 h | `scada5` / `prices_5min` | none (raw 5-min) |
| 1–7 d  | `scada5` / `prices_5min` | none |
| 7–30 d | `generation_30min` / `prices_30min` | none |
| > 30 d | `generation_30min` / `prices_30min` | daily mean |

Time-of-day charts always source from 30-min data regardless of TS
resolution — they aggregate by hour-of-day in SQL and return 24 rows.
