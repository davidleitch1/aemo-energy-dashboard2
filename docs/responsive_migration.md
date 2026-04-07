# AEMO Dashboard — Responsive Migration Plan

## Target State

The dashboard will be fully responsive and work correctly at any viewport width,
from desktop (1920px) down to phone (375px). All charts will use Plotly.
HoloViews, hvplot, and Bokeh rendering will be completely removed.

### Architecture

```
Browser ──► Cloudflare reverse proxy ──► Panel server (port 5008)
            (nemgen.itkservices2.com)     (daves_mini .71)
```

- **No iframe.** The Quarto site links directly to `nemgen.itkservices2.com`.
  Cloudflare proxies to the Panel app. The dashboard gets the full browser viewport.
- **All charts are Plotly** with `autosize=True`, no hardcoded pixel widths.
- **All containers use `sizing_mode='stretch_width'`** — no `sizing_mode='fixed'`
  except where genuinely needed (small indicator badges).
- **Tables wrapped in `overflow-x: auto`** — horizontal scroll on narrow screens.
- **Today tab gauges** remain fixed-size inside a responsive flexbox that wraps.
- **CSS foundation**: `box-sizing: border-box`, viewport meta tag, `max-width: 100%`
  on all containers.

### What changes for users

- No horizontal scrollbar on any tab at any window size.
- Charts resize when the window resizes.
- On narrow screens, gauges wrap to multiple rows instead of overflowing.
- Tables scroll horizontally within their container rather than pushing the page.

---

## Current State (Pre-Migration)

| Metric | Count |
|--------|-------|
| Hardcoded pixel widths (`width=N`) | 213 |
| `sizing_mode='fixed'` with pixel dims | 43 |
| `pn.pane.HoloViews` instances | 21 |
| `hvplot` chart method calls | 44 |
| `hv.Text` placeholder elements | 49 |
| `hv.Overlay` / `hv.HLine` elements | 7 |
| CSS hardcoded widths (inline/raw) | 60+ |
| Tables without scroll wrappers | 34 |
| **Total issues** | **417** |
| **Files affected** | **22** |

---

## Migration Phases

### Phase 0: CSS Foundation

Add global responsive CSS and viewport meta. Does not change any charts or
layout — just prevents overflow and establishes the safety net.

**Changes:**
- `overflow-x: hidden` on `body` (prevent horizontal scroll from any rogue element)
- `box-sizing: border-box` globally
- `max-width: 100%` on `.bk-root` containers
- Responsive table wrapper class (`.responsive-table { overflow-x: auto }`)
- Viewport meta tag via Panel's `raw_css` / `meta_tags`

**Files:** `generation/gen_dash.py` (CSS injection block, lines 149-225)

### Phase 1: Today Tab — Gauges & Fixed Layouts

Convert the Today tab's 18 `sizing_mode='fixed'` widgets to a responsive
flexbox grid. Gauges keep their pixel dimensions but the container wraps.

**Files:**
- `nem_dash/nem_dash_tab.py` — 18 sizing_mode fixes, 2 table wrappers
- `nem_dash/renewable_gauge.py` — 1 inline CSS width
- `nem_dash/generation_overview.py` — 5 HoloViews→Plotly conversions, 5 inline widths
- `nem_dash/price_components_hvplot.py` — 3 HoloViews→Plotly, 3 sizing_mode fixes
- `nem_dash/price_components.py` — 3 sizing_mode fixes
- `nem_dash/daily_summary.py` — 1 sizing_mode fix, 2 table wrappers

**Issue count:** ~55

### Phase 2: Prices Tab

Heavy HoloViews usage — 6 panes, 17 hv.Text, 2 hvplot charts plus price_chart,
fuel_relatives, price_bands modules.

**Files:**
- `prices/prices_tab.py` — 6 HoloViews panes, 17 hv.Text, 14 widths, 3 Tabulator tables
- `prices/price_chart.py` — 2 hvplot.line, 2 hardcoded widths
- `prices/fuel_relatives.py` — 2 hvplot.line, 1 hv.HLine, 2 widths
- `prices/price_bands.py` — 2 hvplot.bar, 2 hv.Overlay, 2 hv.Text

**Issue count:** ~50

### Phase 3: Generation Mix Tab

The largest file. 4 remaining HoloViews panes (price analysis sub-tab),
18 hv.Text, hardcoded widths throughout.

**Files:**
- `generation/gen_dash.py` — 4 HoloViews panes, 3 hvplot calls, 18 hv.Text,
  45 width constraints, CSS max-widths

**Issue count:** ~75

### Phase 4: Trends (Penetration) Tab

Heaviest hvplot usage — 13+ `hvplot.line` calls, 3 HoloViews panes.

**Files:**
- `penetration/penetration_tab.py` — 3 HoloViews panes, 13+ hvplot.line,
  25+ width constraints

**Issue count:** ~45

### Phase 5: Remaining Tabs

**Curtailment:**
- `curtailment/curtailment_tab.py` — 4 hvplot charts, 8 widths, 2 Tabulator tables

**Station Analysis:**
- `station/station_analysis_ui.py` — hvplot scatter/line, 13 widths, 1 Tabulator
- `analysis/price_analysis_ui.py` — 12 widths, 2 Tabulator tables

**Insights (Pivot Table):**
- `insights/insights_tab.py` — 1 HoloViews pane, 3 hvplot, 36 widths, 6+ tables

**PASA:**
- `pasa/pasa_tab.py` — CSS bar widths, 2 Tabulator tables

**Gas & Futures:**
- Already Plotly. Only widget widths to fix (5 total).

**Spot Prices (separate dashboard):**
- `spot_prices/display_spot.py` — 7 sizing_mode='fixed', inline styles

**Issue count:** ~190

### Phase 6: Serving — Reverse Proxy

Replace iframe embedding with Cloudflare reverse proxy.

**Changes:**
- Cloudflare Zero Trust dashboard: route `nemgen.itkservices2.com` directly
  to `192.168.68.71:5008`
- Update `pn.serve()` `allow_websocket_origin` list
- Remove or redirect the Quarto iframe page (`nemgen.qmd`)
- Add `<meta name="viewport">` via Panel config
- Test WebSocket connectivity through proxy

### Phase 7: Cleanup

Remove all dead code after migration is complete and verified.

**Remove:**
- `import holoviews as hv` / `import hvplot.pandas` from all files
- `hv.extension('bokeh')` call
- `hv.opts.defaults(...)` block
- All Bokeh hook functions (`_get_flexoki_background_hook`,
  `_get_datetime_formatter_hook`, `_flexoki_background_hook`)
- `from bokeh.models import DatetimeTickFormatter, PrintfTickFormatter`
- `create_generation_plot_cached()` function
- Bokeh CSS classes from `raw_css` block (`.bk-canvas-wrapper`, `.bk-Figure`, etc.)
- `'bokeh'` related Panel extension loading if no longer needed
- Backup files: `gen_dash.py.bak`, `*_original.py`, `*_cached.py`, etc.
- Old fix documentation `.md` files in repo root

**Verify:** `grep -r 'holoviews\|hvplot\|hv\.\|bokeh' src/` returns zero matches
(excluding comments and any intentional references).

---

## File Inventory

### Files requiring HoloViews → Plotly conversion

| File | hvplot calls | HoloViews panes | hv.Text | hv.Overlay/HLine |
|------|-------------|-----------------|---------|-------------------|
| `generation/gen_dash.py` | 3 | 4 | 18 | 0 |
| `prices/prices_tab.py` | 0 | 6 | 17 | 0 |
| `prices/price_chart.py` | 2 | 0 | 0 | 0 |
| `prices/fuel_relatives.py` | 2 | 0 | 0 | 1 |
| `prices/price_bands.py` | 2 | 0 | 2 | 2 |
| `penetration/penetration_tab.py` | 13+ | 3 | 0 | 0 |
| `curtailment/curtailment_tab.py` | 4 | 0 | 0 | 0 |
| `insights/insights_tab.py` | 3 | 1 | 0 | 0 |
| `nem_dash/generation_overview.py` | 2 | 1 | 0 | 0 |
| `nem_dash/price_components_hvplot.py` | 1 | 3 | 0 | 0 |
| `station/station_analysis_ui.py` | 2+ | 0 | 0 | 0 |

### Files requiring width/sizing fixes only (already Plotly or HTML)

| File | Hardcoded widths | sizing_mode='fixed' | Tables needing wrappers |
|------|-----------------|---------------------|------------------------|
| `nem_dash/nem_dash_tab.py` | 17 | 18 | 2 |
| `nem_dash/renewable_gauge.py` | 1 | 1 | 0 |
| `nem_dash/price_components.py` | 0 | 3 | 0 |
| `nem_dash/daily_summary.py` | 0 | 1 | 2 |
| `analysis/price_analysis_ui.py` | 12 | 0 | 2 |
| `pasa/pasa_tab.py` | 10 | 1 | 2 |
| `gas/sttm_tab.py` | 3 | 0 | 0 |
| `futures/futures_tab.py` | 2 | 0 | 0 |
| `spot_prices/display_spot.py` | 9 | 7 | 1 |

---

## Testing Strategy

One test file per module, all marked `@pytest.mark.responsive`.

Run all: `pytest -m responsive`
Run one module: `pytest tests/test_responsive_prices.py`
Track progress: `pytest -m responsive --tb=no -q` (shows pass/fail counts)

Tests verify:
1. **Return type** — every converted function returns `go.Figure` (not HoloViews)
2. **No hardcoded width** — figure layout has no `width` key, or uses `autosize=True`
3. **Flexoki theme** — `paper_bgcolor == FLEXOKI_PAPER`
4. **Pane types** — `pn.pane.Plotly` not `pn.pane.HoloViews`
5. **Table wrappers** — HTML tables wrapped in `overflow-x: auto` div
6. **No HoloViews imports** — post-cleanup, modules don't import hv/hvplot

---

## Definition of Done

- [ ] `pytest -m responsive` — 100% pass
- [ ] `grep -r 'import holoviews\|import hvplot\|pn.pane.HoloViews' src/` — zero matches
- [ ] `grep -r "sizing_mode='fixed'" src/` — only gauge badges (documented exceptions)
- [ ] `grep -r 'width=1200\|width=1000\|width=900\|width=800\|width=700' src/` — zero in plot opts
- [ ] Dashboard renders without horizontal scrollbar at 1920px, 1366px, 768px, 375px
- [ ] All tabs load and display correctly after restart
- [ ] Cloudflare reverse proxy serves dashboard without iframe
- [ ] No `.bak`, `*_original.py`, `*_cached.py` files in `src/`
