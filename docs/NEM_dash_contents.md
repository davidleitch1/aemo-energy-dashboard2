# NEM Dashboard contents

Inventory of every tab, card, plot, and table in the AEMO dashboard mockup
(`src/aemo_dashboard/web/app.py` on the `web-dashboard-redesign` branch).

Used to track:
- What's built vs placeholder
- Attribution status per card
- Source data origin (drives the attribution text)

**Convention**: each visible card carries an italic footer in the form
`Data: <source> · plot: ITK` rendered by the `_attribution()` helper. Most
cards use `Data: AEMO`. Cards reading from STTM use `Data: AEMO STTM`.
Futures (ASX via Global-Roam) use `Data: Global-Roam`. The Cal+2-vs-spot
chart uses `Data: Global-Roam + AEMO` since it overlays both sources.

Update this file when cards are added, moved, renamed, or removed.

## Tab nav order

1. Today
2. Generation mix (subtabs: Yr on yr / Stack / Time of day / Trends / Transmission)
3. Evening peak
4. Prices (subtabs: Price Analysis / Price Bands)
5. Batteries (placeholder)
6. Futures
7. Generators (formerly "Pivot table")
8. Curtailment (placeholder)
9. PASA (placeholder)
10. Gas

**Deep-link only** (not in top nav): Station Analysis (`/station-analysis?duid=…`).

---

## Today

Tile-based grid (3×3). Page-level attribution sits below the grid in
`today_body.html` since per-tile attribution would clutter the small cards.

- [x] **Page footer** (`today_body.html`) — `Data: AEMO · plot: ITK`
- Tiles (no per-tile attribution; covered by page footer):
  - Renewable gauge (`/tile/renewable-gauge`)
  - Generation mix mini (`/tile/generation-mix`)
  - Demand gauge (`/tile/demand-gauge`)
  - Price chart (`/tile/price-chart`)
  - Price table (`/tile/price-table`)
  - Battery SoC (`/tile/battery-soc`)
  - Forecast chart (`/tile/forecast-chart`)
  - Market notices (`/tile/market-notices`)
  - Outages summary (`/tile/outages-summary`)

## Generation mix · Yr on yr

- [x] **Year on year fuel + price table** — `Data: AEMO`

## Generation mix · Stack

- [x] **Generation by fuel stack** — `Data: AEMO`
- [x] **Price chart** (paired below stack) — `Data: AEMO`
- [x] **Fuel stats table** — `Data: AEMO`

## Generation mix · Time of day

- [x] **Hour-of-day generation + price** — `Data: AEMO`

## Generation mix · Trends

- [x] **VRE production chart** — `Data: AEMO`
- [x] **Renewable generation by fuel** — `Data: AEMO`
- [x] **Thermal vs renewables share** — `Data: AEMO`
- *(Methodology note card — text only, no attribution)*

## Generation mix · Transmission

- [x] **Transmission flows time series** — `Data: AEMO`
- [x] **Transmission by hour of day** — `Data: AEMO`

## Evening peak

- [x] **4-panel evening peak chart** — `Data: AEMO`
- [x] **% composition card** — `Data: AEMO`

## Prices · Price Analysis

- [x] **Spot prices chart** — `Data: AEMO`
- [x] **Price statistics table** — `Data: AEMO`
- [x] **Time of day chart** — `Data: AEMO`

## Prices · Price Bands

- [x] **Price band butterfly chart** — `Data: AEMO`
- [x] **Price contribution by band (stack)** — `Data: AEMO`
- [x] **Price band details table** — `Data: AEMO`

## Gas

- [x] **STTM ex-post gas price** — `Data: AEMO STTM`
- [x] **Total STTM gas demand** — `Data: AEMO STTM`
- [x] **STTM gas demand by hub** — `Data: AEMO STTM`

## Generators

- [x] **Generators pivot table** — `Data: AEMO`

## Batteries

- [x] **Batteries pivot table** — full battery-economics column set:
  Disch GWh / Chrg GWh / Disch $M / Chrg $M / Disch $/MWh / Chrg $/MWh /
  Spread $/MWh / **$/MWh-cap/yr** (annualised spread revenue per MWh of
  storage, the investment metric) / Util % / Cap MW / Storage MWh.
  Group dims locked to Region / Owner; fuel pre-filtered to Battery Storage.
  Row anchors link to /station-analysis. — `Data: AEMO`
- [x] **Battery ranking lollipop** — horizontal lollipop, top-N batteries
  by selected metric. Default metric = `$/MWh-cap/yr`. Marker colour = region.
  Top-N selector (10/20/50/All). — `Data: AEMO`

## Station Analysis (deep-link only)

- [x] **Output and price time series** — `Data: AEMO`
- [x] **Average performance by hour of day** — `Data: AEMO`
- *(Stats strip — numeric tiles only, no attribution needed)*

## Futures

- [x] **Forward curve (per region)** — `Data: Global-Roam`
- [x] **Calendar-year forward averages** — `Data: Global-Roam`
- [x] **Cal+2 forward vs trailing spot** — `Data: Global-Roam + AEMO`
- [x] **Single contract (all regions)** — `Data: Global-Roam`

## Placeholders (not yet built)

- Curtailment
- PASA

When these are built, add a section per tab with each card listed as above.
