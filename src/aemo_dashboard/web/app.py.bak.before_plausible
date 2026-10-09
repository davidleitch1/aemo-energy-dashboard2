"""Standalone Today-tab mockup. Demonstrates the tile-by-tile HTMX pattern.

Run with the existing dashboard .venv:
    /Users/davidleitch/aemo_production/aemo-energy-dashboard2/.venv/bin/uvicorn \\
        app:app --host 0.0.0.0 --port 8090

Then open  http://192.168.68.71:8090  from any machine on the LAN,
or  http://localhost:8090  via SSH tunnel:
    ssh -L 8090:127.0.0.1:8090 davidleitch@192.168.68.71

Six tiles run real DuckDB queries against aemo_readonly.duckdb. Three are
stubbed with a small artificial delay so you can watch the staggered
loading pattern unambiguously.
"""
from __future__ import annotations

import asyncio
import html as html_lib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOESS = True
except ImportError:
    HAS_LOESS = False

# Sibling-package helpers (compute_price_bands, ChangeDetector, etc.) are
# imported normally inside the functions that use them — see _get_notices,
# _get_outage_data, _prices_bands_content, _band_detail_df_to_html.

DB_PATH = "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb"
RECORDS_DIR = Path("/Users/davidleitch/aemo_production/data")
NEM_TZ = timezone(timedelta(hours=10))

PAPER = "#fffcf0"
INK = "#100f0f"
MUTED = "#878580"
BORDER = "#e6e4d9"
TEAL = "#24837b"  # ITK brand accent — used for card titles and header underline

# Plotly chart defaults. Setting font.family explicitly avoids Plotly's
# fall-through-to-Open-Sans, which renders less crisply than the system font
# the rest of the page uses. PLOTLY_CFG controls the modebar: 'hover' means
# the toolbar only appears when the user hovers, so the default view stays
# clean but interactivity (box-zoom on x-axis, pan, autoscale reset) is one
# mouse-move away. Legend click-to-toggle is enabled by default in Plotly
# regardless of modebar setting.
PLOTLY_FONT = dict(
    family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    size=11, color=INK,
)
PLOTLY_CFG = ("{displayModeBar:'hover',displaylogo:false,"
              "modeBarButtonsToRemove:['lasso2d','select2d','toImage',"
              "'sendDataToCloud'],responsive:true}")


def _plot_json(fig) -> str:
    """Apply shared chart defaults (font) and serialise."""
    fig.update_layout(font=PLOTLY_FONT)
    return fig.to_json()

REGION_ORDER = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
REGION_COLORS = {
    "NSW1": "#879a39", "QLD1": "#da702c", "SA1": "#a02f6f",
    "TAS1": "#24837b", "VIC1": "#8b7ec8",
}
# Display palette uses normalised fuel names. The raw `generation_by_fuel_5min`
# table calls hydro "Water" and splits gas into CCGT/OCGT/Gas other; rooftop
# isn't in that table at all (it lives in `rooftop30`). `_normalise_fuels`
# folds raw → display.
#
# Colours match production's gen_dash.get_fuel_colors() exactly — a mix of
# FLEXOKI_ACCENT 600-shade darks (Gas, Wind, Hydro, Battery, Transmission)
# and three custom shades (Coal brown, Solar gold, Rooftop lighter yellow).
# Production calls #E8C547 "lighter yellow — distributed solar" specifically.
FUEL_COLORS = {
    "Coal":            "#6B3A10",   # brown — production custom
    "Gas":             "#BC5215",   # FLEXOKI_ACCENT['orange']
    "Hydro":           "#24837B",   # FLEXOKI_ACCENT['cyan']
    "Wind":            "#66800B",   # FLEXOKI_ACCENT['green']
    "Solar":           "#D4A000",   # gold — production custom (bright yellow)
    "Rooftop Solar":   "#E8C547",   # lighter yellow — production custom
    "Battery Storage": "#5E409D",   # FLEXOKI_ACCENT['purple']
    "Biomass":         "#4A7C23",   # dark green — production custom
    "Other":           "#878580",
}
GAUGE_FUEL_COLORS = {
    "Hydro":         "#205ea6",
    "Wind":          "#66800b",
    "Solar":         "#ad8301",
    "Rooftop Solar": "#bc5215",
}
RENEWABLE_FUELS = ["Hydro", "Wind", "Solar", "Rooftop Solar"]
RAW_TO_DISPLAY = {
    "Water": "Hydro",
    "CCGT": "Gas", "OCGT": "Gas", "Gas other": "Gas",
}


def _normalise_fuels(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Water→Hydro and collapse CCGT/OCGT/Gas other→Gas in-place."""
    df = df.copy()
    df["fuel_type"] = df["fuel_type"].map(lambda f: RAW_TO_DISPLAY.get(f, f))
    return df


def _rooftop_latest_mw() -> float:
    """Most-recent rooftop NEM-wide MW from rooftop30 (5-region sum).

    Filter to the 5 main NEM regions: rooftop30 historically (pre-2026)
    also contained sub-region IDs (QLDC/QLDN/QLDS/TASN/TASS) that
    double-count QLD+TAS when summed across all regions. See
    [[rooftop-subregion-bug]] in MEMORY.md."""
    df = q("""
        WITH latest AS (
            SELECT MAX(settlementdate) AS ts FROM rooftop30
            WHERE regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')
        )
        SELECT SUM(power) AS mw
          FROM rooftop30, latest
         WHERE settlementdate = latest.ts
           AND regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')
    """)
    if df.empty or df["mw"].iloc[0] is None:
        return 0.0
    return float(df["mw"].iloc[0])

app = FastAPI(title="NEM Today mockup")


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    """Fresh read-only connection per query, like the iOS API does."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        return conn.execute(sql, params or []).df()
    finally:
        conn.close()


# ============================================================================
# Tab structure + shell rendering
# ============================================================================
#
# Pattern C navigation: top-level horizontal nav with 9 tabs, contextual
# subtab pills below for tabs that have subtabs. Each tab is its own URL
# so links are bookmarkable and browser back/forward works.
#
# HTMX-aware: same route serves the full shell on direct nav (refresh,
# bookmark) and just the tab body on HX-Request — so tab switches don't
# re-render the chrome or re-fetch tiles outside the swapped region.

# Mirrors the production Panel dashboard tab list (12 tabs) with one
# reorder: Batteries and Futures hoist to follow Prices so the price-related
# market views cluster together. Subtab list per tab is empty for now —
# infrastructure to render them is still here for any tab that gets them later.
TABS = [
    ("today",            "Today",            []),
    ("generation-mix",   "Generation mix",   [("yr-on-yr",      "Yr on yr"),
                                              ("stack",         "Stack"),
                                              ("tod",           "Time of day"),
                                              ("trends",        "Trends"),
                                              ("transmission",  "Transmission")]),
    ("evening-peak",     "Evening peak",     []),
    ("prices",           "Prices",           [("analysis", "Price Analysis"),
                                              ("bands",    "Price Bands")]),
    ("batteries",        "Batteries",        []),  # moved
    ("futures",          "Futures",          []),  # moved
    ("generators",       "Generators",       []),
    # Station Analysis is no longer in the top nav. The pivot table /
    # Generators page is the directory; clicking a DUID/station drills
    # into /station-analysis?duid=X — that URL still works as a deep
    # link, bookmark, and share target, but it's not advertised as a
    # standalone destination.
    # "Trends" removed from the top nav — it's now a subtab of Generation mix
    # at /generation-mix/trends. Bookmarks to /trends redirect via the
    # legacy_trends_redirect handler below.
    ("curtailment",      "Curtailment",      []),
    ("pasa",             "PASA",             []),
    ("gas",              "Gas",              []),
]
TAB_LOOKUP = {slug: (label, subs) for slug, label, subs in TABS}


SHELL_CSS = """
:root {
  --paper:#fffcf0; --ink:#100f0f; --muted:#878580; --border:#e6e4d9;
  --teal:#24837b;
}
body {
  margin: 0; background: var(--paper); color: var(--ink);
  font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  /* Tighter macOS anti-aliasing — Inter + grayscale smoothing is what gives
     the production dashboard its crisp tabular numbers. */
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
}
header {
  padding: 16px 24px;
  border-bottom: 2px solid var(--teal);
  display: flex; justify-content: space-between; align-items: baseline;
}
h1 { font-size: 22px; margin: 0; font-weight: 600; letter-spacing: -0.2px; }
h1 .brand { color: var(--teal); }
.wordmark { color: var(--teal); font-weight: 700; font-size: 13px;
            letter-spacing: 1px; margin-right: 12px; }
.clock { color: var(--muted); font-size: 13px; }

/* ---- Top-level tab nav ---- */
.tab-nav {
  display: flex; gap: 0; padding: 0 24px;
  background: var(--paper);
  border-bottom: 1px solid var(--border);
  overflow-x: auto; scrollbar-width: none;
}
.tab-nav::-webkit-scrollbar { display: none; }
.tab-link {
  padding: 12px 16px;
  color: var(--muted);
  text-decoration: none;
  font-size: 13px; font-weight: 500;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;          /* overlap the 1px nav-bottom border */
  white-space: nowrap;
  transition: color 0.15s;
}
.tab-link:hover { color: var(--ink); }
.tab-link.active {
  color: var(--ink);
  border-bottom-color: var(--teal);
  font-weight: 600;
}

/* ---- Subtab pills (per tab) ---- */
.subtab-nav {
  display: flex; gap: 4px;
  padding: 8px 24px;
  background: #f7f5e8;
  border-bottom: 1px solid var(--border);
  overflow-x: auto; scrollbar-width: none;
}
.subtab-nav::-webkit-scrollbar { display: none; }
.subtab-link {
  padding: 4px 10px;
  color: var(--muted);
  text-decoration: none;
  font-size: 12px; font-weight: 500;
  border-radius: 4px; white-space: nowrap;
}
.subtab-link:hover { background: var(--border); color: var(--ink); }
.subtab-link.active { background: var(--teal); color: #fffcf0; }

/* ---- Tile grid + card chrome ---- */
.grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px; padding: 12px;
}
.card {
  background: var(--paper);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 12px;
  min-height: 230px;
}
.skeleton {
  color: var(--muted); font-size: 13px;
  display: flex; align-items: center; justify-content: center;
  height: 200px;
  background: repeating-linear-gradient(90deg,
    var(--paper) 0, var(--paper) 40px,
    #f5f3e6 40px, #f5f3e6 80px);
  background-size: 80px 100%;
  animation: shimmer 1.5s linear infinite;
  border-radius: 4px;
}
@keyframes shimmer { from {background-position: 0 0} to {background-position: 80px 0} }
.card.htmx-request { opacity: 0.7; transition: opacity 0.2s; }
@media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }

/* ---- Selector strip (region + date range) ---- */
.selector-strip {
  display: flex; flex-wrap: wrap; gap: 20px;
  padding: 12px 24px;
  background: var(--paper);
  border-bottom: 1px solid var(--border);
  align-items: center;
}
.pill-bar { display: flex; align-items: center; gap: 8px; }
.pill-bar-label {
  font-size: 10px; color: var(--muted);
  text-transform: uppercase; letter-spacing: 0.5px;
  font-weight: 600;
}
/* Single-select: one capsule, shared internal borders */
.pill-group {
  display: inline-flex;
  border: 1px solid var(--border);
  border-radius: 14px;
  overflow: hidden;
  background: var(--paper);
}
.pill-group .pill { border-right: 1px solid var(--border); border-radius: 0; }
.pill-group .pill:last-child { border-right: none; }
/* Multi-select: independent capsules */
.pill-toggles { display: inline-flex; gap: 6px; }
.pill-toggles .pill {
  border: 1px solid var(--border);
  border-radius: 14px;
}
.pill {
  background: var(--paper); color: var(--ink);
  font-size: 12px; font-weight: 500;
  padding: 4px 12px;
  cursor: pointer; white-space: nowrap;
  border: none;
  transition: background 0.1s, color 0.1s;
  font-family: inherit;
}
.pill:hover { background: var(--border); }
.pill.active { background: var(--teal); color: #fffcf0; font-weight: 600; }
.pill.active:hover { background: var(--teal); }
.pill:focus-visible { outline: 2px solid var(--teal); outline-offset: 2px; }
.pill.disabled {
  color: #c7c5b8; cursor: not-allowed; pointer-events: none;
  background: var(--paper);
}

/* Custom-range date form (visible only when range=custom) */
.custom-form {
  display: inline-flex; gap: 6px; align-items: center;
  padding: 4px 10px;
  background: #f7f5e8;
  border: 1px solid var(--border);
  border-radius: 14px;
  margin-left: 8px;
}
.custom-form input[type="date"] {
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 12px; font-family: inherit;
  background: var(--paper);
}
.custom-form button {
  background: var(--teal); color: #fffcf0;
  border: none; padding: 4px 10px;
  border-radius: 4px;
  font-size: 11px; font-weight: 600;
  cursor: pointer; font-family: inherit;
}
.custom-form .sep { color: var(--muted); font-size: 11px; }

/* Prices tab — chart over stats table, both full width */
.prices-stack {
  display: flex; flex-direction: column;
  gap: 12px; padding: 12px;
}

/* ---- Placeholder content for unbuilt tabs ---- */
.placeholder {
  padding: 60px 24px; text-align: center;
  color: var(--muted); font-size: 14px;
}
.placeholder strong { color: var(--ink); }
"""


# Tabulator overrides for the pivot table. Loaded once in the shell so they
# persist across HTMX body swaps. Scoped to #pivot-tabulator so they don't
# bleed into any future Tabulator instance on other pages.
PIVOT_TABULATOR_CSS = """
#pivot-tabulator .tabulator {
  background: #fffcf0; font-family: Inter, sans-serif;
  border: 1px solid #e6e4d9; border-radius: 6px;
}
#pivot-tabulator .tabulator-header {
  background: #e6e4d9; color: #100f0f;
  border-bottom: 1px solid #e6e4d9;
}
#pivot-tabulator .tabulator-col {
  background: #e6e4d9;
  font-size: 11px; text-transform: uppercase;
  letter-spacing: 0.4px; color: #100f0f; font-weight: 600;
}
#pivot-tabulator .tabulator-row {
  background: #fffcf0; color: #100f0f;
  border-bottom: 1px solid #f0eedf;
  font-size: 13px;
}
#pivot-tabulator .tabulator-row.tabulator-row-even { background: #f7f5e8; }
#pivot-tabulator .tabulator-row:hover { background: #ece9d5; cursor: pointer; }
#pivot-tabulator .pivot-group-label { font-weight: 600; color: #100f0f; }
#pivot-tabulator .pivot-station-label { color: #100f0f; }
#pivot-tabulator .pivot-duid-label {
  color: #878580; font-family: ui-monospace, Menlo, monospace; font-size: 12px;
}
/* Drill-down cue: underline + teal on hover for the anchors wrapped around
   DUID, station, and group names. Soft chevron is appended in the cell
   formatter. */
#pivot-tabulator a.pivot-duid-label,
#pivot-tabulator a.pivot-station-label,
#pivot-tabulator a.pivot-group-label {
  border-bottom: 1px solid transparent;
  transition: color 0.1s, border-color 0.1s;
}
#pivot-tabulator a.pivot-duid-label:hover,
#pivot-tabulator a.pivot-station-label:hover,
#pivot-tabulator a.pivot-group-label:hover {
  color: #24837b;
  border-bottom-color: #24837b;
}
#pivot-tabulator .pivot-chev { color: #c7c5b8; font-size: 11px; margin-left: 4px; }
#pivot-tabulator a:hover .pivot-chev { color: #24837b; }
#pivot-tabulator .pivot-n { color: #878580; font-size: 11px; margin-left: 6px; }
#pivot-selection-plot { margin-top: 12px; }
"""

# Pivot-table Tabulator builder. Lives in the shell so HTMX swaps just emit
# a container + a pair of globals (window._pivotData, window._pivotCols).
# The `htmx:afterSettle` event fires AFTER the browser has done layout
# on the new body, so the container's measured width is real — fixing
# the column-stacking bug that happened when Tabulator initialised
# from an inline script in the still-being-laid-out swap.
PIVOT_TABULATOR_JS = r"""
(function () {
  function _pivotLabelFmt(cell) {
    var row = cell.getRow();
    var d = row.getData();
    var depth = 0;
    var p = row.getTreeParent();
    while (p) { depth++; p = p.getTreeParent(); }
    var indent = (depth * 16) + "px";
    var name = d.label || "";
    var cls = d.kind === "duid" ? "pivot-duid-label"
            : d.kind === "station" ? "pivot-station-label"
            : "pivot-group-label";
    var meta = (d.kind !== "duid" && d.n_items)
             ? '<span class="pivot-n">(' + d.n_items + ')</span>'
             : '';
    // DUID + Station rows wrap the name in an <a> so the browser navigates
    // natively. This sidesteps rowClick / dataTree event-handling quirks
    // and gets middle-click / ctrl-click "open in new tab" for free. We
    // append the pivot's current range so the station view shows the same
    // window the user was inspecting.
    var rangeQs = "";
    if (window._pivotRange) {
      rangeQs += "&range=" + encodeURIComponent(window._pivotRange);
      if (window._pivotRange === "custom") {
        if (window._pivotStart)
          rangeQs += "&start=" + encodeURIComponent(window._pivotStart);
        if (window._pivotEnd)
          rangeQs += "&end=" + encodeURIComponent(window._pivotEnd);
      }
    }
    var chev = '<span class="pivot-chev">›</span>';
    if (d.kind === "duid") {
      var key = (d.ctx && d.ctx.duid) || name;
      return '<span style="padding-left:' + indent + '">'
           + '<a href="/station-analysis?duid=' + encodeURIComponent(key)
           +    rangeQs + '" '
           +    'class="' + cls + '" style="text-decoration:none;'
           +    'color:inherit">' + name + chev + '</a></span>';
    }
    if (d.kind === "station") {
      var key = (d.ctx && d.ctx.station) || name;
      return '<span style="padding-left:' + indent + '">'
           + '<a href="/station-analysis?station=' + encodeURIComponent(key)
           +    rangeQs + '" '
           +    'class="' + cls + '" style="text-decoration:none;'
           +    'color:inherit">' + name + chev + '</a>' + meta + '</span>';
    }
    // Group rows (Coal, NSW1, Pacific Hydro, etc.): build a fleet filter
    // URL from the row's context. ctx carries whichever dims are in scope
    // at this depth — outer groups have one filter, inner groups have more.
    if (d.kind === "group") {
      var ctx = d.ctx || {};
      var groupQs = "";
      if (ctx.fuel)   groupQs += "&fuel="   + encodeURIComponent(ctx.fuel);
      if (ctx.region) groupQs += "&region=" + encodeURIComponent(ctx.region);
      if (ctx.owner)  groupQs += "&owner="  + encodeURIComponent(ctx.owner);
      // Strip leading & — query string starts with "?".
      if (groupQs) groupQs = "?" + groupQs.slice(1) + rangeQs;
      return '<span style="padding-left:' + indent + '">'
           + '<a href="/station-analysis' + groupQs + '" '
           +    'class="' + cls + '" style="text-decoration:none;'
           +    'color:inherit">' + name + chev + '</a>' + meta + '</span>';
    }
    return '<span style="padding-left:' + indent + '" class="' + cls + '">'
         + name + meta + '</span>';
  }
  function _pivotIntDashFmt(cell) {
    var v = cell.getValue();
    if (v === null || v === undefined)
      return '<span style="color:#878580">—</span>';
    return Math.round(v).toLocaleString('en-AU');
  }

  function _buildPivotColumns(selectedCols) {
    var num = {hozAlign: "right", headerHozAlign: "right"};
    var intMoney = {formatter: "money",
                    formatterParams: {precision: 0, thousand: ","}};
    // Column registry. The pivot (Generators) and the /batteries page both
    // request from this set via window._pivotCols (a list of slugs).
    var registry = {
      // Generators (existing)
      gwh:        {title: "GWh",        field: "gen_gwh",  fmt: "int"},
      rev:        {title: "Rev $M",     field: "rev_m",    fmt: "int"},
      price:      {title: "$/MWh",      field: "price",    fmt: "dash"},
      util:       {title: "Util %",     field: "util",     fmt: "dash"},
      cap:        {title: "Cap MW",     field: "cap_mw",   fmt: "int"},
      // Batteries (new)
      disch_gwh:  {title: "Disch GWh",  field: "discharge_gwh", fmt: "int"},
      ch_gwh:     {title: "Chrg GWh",   field: "charge_gwh",    fmt: "int"},
      disch_rev:  {title: "Disch $M",   field: "discharge_rev_m", fmt: "int"},
      ch_cost:    {title: "Chrg $M",    field: "charge_cost_m",   fmt: "int"},
      disch_price:{title: "Disch $/MWh", field: "discharge_price", fmt: "dash"},
      ch_price:   {title: "Chrg $/MWh", field: "charge_price",    fmt: "dash"},
      spread:     {title: "Spread $/MWh", field: "spread_pmwh",   fmt: "dash"},
      spread_yr:  {title: "$/MWh-cap/yr", field: "spread_per_mwh_yr",
                   fmt: "int", minWidth: 110},
      storage:    {title: "Storage MWh", field: "storage_mwh",   fmt: "int"},
    };
    var cols = [{
      title: "Name", field: "label", frozen: true,
      minWidth: 260, widthGrow: 2, headerSort: false,
      formatter: _pivotLabelFmt,
    }];
    for (var i = 0; i < selectedCols.length; i++) {
      var spec = registry[selectedCols[i]];
      if (!spec) continue;
      var entry = {
        title: spec.title, field: spec.field,
        minWidth: spec.minWidth || 80, headerSort: true,
      };
      if (spec.fmt === "int") Object.assign(entry, intMoney);
      else if (spec.fmt === "dash") entry.formatter = _pivotIntDashFmt;
      cols.push(Object.assign(entry, num));
    }
    return cols;
  }

  function buildPivotTable() {
    var container = document.getElementById("pivot-tabulator");
    if (!container) {
      // Page is not the pivot tab; clean up any prior instance.
      if (window._pivotTable) {
        try { window._pivotTable.destroy(); } catch (e) {}
        window._pivotTable = null;
      }
      return;
    }
    if (!window._pivotData || !window._pivotCols) return;
    if (typeof Tabulator === "undefined") {
      // Tabulator JS hasn't loaded yet — retry after one more frame.
      requestAnimationFrame(buildPivotTable);
      return;
    }
    if (window._pivotTable) {
      try { window._pivotTable.destroy(); } catch (e) {}
      window._pivotTable = null;
    }
    window._pivotTable = new Tabulator("#pivot-tabulator", {
      data: window._pivotData,
      dataTree: true,
      dataTreeStartExpanded: false,
      dataTreeBranchElement: '<span style="color:#878580;margin-right:4px">│</span>',
      dataTreeCollapseElement: '<span style="color:#24837b;cursor:pointer;margin-right:4px;font-weight:700">▾</span>',
      dataTreeExpandElement: '<span style="color:#24837b;cursor:pointer;margin-right:4px;font-weight:700">▸</span>',
      dataTreeChildIndent: 0,
      layout: "fitColumns",
      layoutColumnsOnNewData: true,
      // maxHeight (not height) so the table shrinks to fit its rows when
      // collapsed and only scrolls internally when the expanded tree would
      // overflow the viewport. Avoids the big empty card that was hiding
      // the lollipop / other content below the pivot.
      maxHeight: "calc(100vh - 280px)",
      columns: _buildPivotColumns(window._pivotCols),
      // No rowClick handler — every clickable name (DUID, station, group)
      // is an <a> in the cell formatter, so clicks navigate natively.
    });
  }

  window._buildPivotTable = buildPivotTable;
  document.addEventListener("htmx:afterSettle", buildPivotTable);
  document.addEventListener("DOMContentLoaded", buildPivotTable);
  window.addEventListener("resize", function () {
    if (window._pivotTable) {
      try { window._pivotTable.redraw(true); } catch (e) {}
    }
  });
})();
"""


def _render_tab_nav() -> str:
    """Top-level nav. The .active class is applied client-side based on URL
    so the nav HTML itself doesn't change per tab — keeps it cacheable."""
    links = []
    for slug, label, _ in TABS:
        url = f"/{slug}"
        links.append(
            f'<a class="tab-link" href="{url}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true" '
            f'hx-swap="innerHTML">{label}</a>'
        )
    return f'<nav class="tab-nav">{"".join(links)}</nav>'


def _render_subtab_nav(parent_slug: str, subtabs: list, active: str | None,
                       carry_params: dict | None = None) -> str:
    """Subtab pills. `carry_params` adds query-string state so navigating
    between subtabs preserves the selector values (region/range/smooth)."""
    if not subtabs:
        return ""
    links = []
    for slug, label in subtabs:
        url = f"/{parent_slug}/{slug}"
        if carry_params:
            url = _build_url(url, **carry_params)
        cls = "subtab-link active" if slug == active else "subtab-link"
        links.append(
            f'<a class="{cls}" href="{url}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true" '
            f'hx-swap="innerHTML">{label}</a>'
        )
    return f'<nav class="subtab-nav">{"".join(links)}</nav>'


def _render_tab_body(subtab_html: str, content_html: str) -> str:
    """The swap target. Contains subtab strip (or nothing) + content."""
    return f"{subtab_html}{content_html}"


def _render_shell(body_html: str) -> str:
    """Full HTML doc for direct navigation / refresh. The tab-nav and the
    tiny activate-on-URL JS handle the active state without server help.

    Tabulator CSS+JS load here (not inside swapped content) so they persist
    across HTMX swaps. The Generators page only emits a container div + a
    pair of globals; the shell-level builder runs on `htmx:afterSettle`
    (i.e. AFTER the browser has done a layout pass), eliminating the
    fitColumns-measures-zero-width bug that was wrapping the column titles."""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>NEM Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link href="https://unpkg.com/tabulator-tables@6.3.1/dist/css/tabulator_simple.min.css" rel="stylesheet">
  <script src="https://unpkg.com/htmx.org@2.0.4"></script>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <script src="https://unpkg.com/tabulator-tables@6.3.1/dist/js/tabulator.min.js"></script>
  <style>{SHELL_CSS}</style>
  <style>{PIVOT_TABULATOR_CSS}</style>
</head>
<body>
  <header>
    <div style="display:flex;align-items:baseline;gap:16px">
      <h1>NEM <span class="brand">Dashboard</span></h1>
      <span class="clock" hx-get="/tile/clock" hx-trigger="load, every 30s"></span>
    </div>
    <span class="wordmark">ITK</span>
  </header>
  {_render_tab_nav()}
  <div id="tab-body">{body_html}</div>
  <script>{PIVOT_TABULATOR_JS}</script>
  <script>
    function updateActiveTab() {{
      const path = window.location.pathname;
      document.querySelectorAll('.tab-link').forEach(a => {{
        const href = a.getAttribute('href');
        a.classList.toggle('active', href === path || path.startsWith(href + '/'));
      }});
    }}
    document.addEventListener('htmx:afterSettle', updateActiveTab);
    document.addEventListener('DOMContentLoaded', updateActiveTab);
  </script>
</body>
</html>
"""


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request") == "true"


# ----------------------------------------------------------------------------
# Selector strip primitives  (used by any tab that needs region/date controls)
# ----------------------------------------------------------------------------
#
# The whole tab-body (subtab nav + selector strip + content) is the HTMX swap
# target. So pills render with active state server-side based on URL params,
# no client-side state machine needed. The trade-off: each selector click is
# a server round-trip. With the in-process DuckDB query taking ~10-30 ms for
# typical Prices queries, that's fine; the perceived cost is the network
# hop, which is also ~10-30 ms on LAN/CF Access.

from urllib.parse import urlencode

RANGE_OPTIONS = [("1h", "1H"), ("24h", "24H"), ("7d", "7D"),
                  ("30d", "30D"), ("ytd", "YTD"), ("1y", "Yr"),
                  ("all", "All")]


def _build_url(base: str, **params) -> str:
    clean = {k: v for k, v in params.items() if v is not None and v != ""}
    return f"{base}?{urlencode(clean)}" if clean else base


def _region_display(region: str) -> str:
    """Display label for a region. NEM stays as 'NEM'; physical regions
    drop their trailing '1' (NSW1 → NSW)."""
    return "NEM" if region == "NEM" else region[:-1]


def _render_region_pills(base_url: str, active: str,
                         other_params: dict, multi: bool = False,
                         regions: list[str] | None = None) -> str:
    """Single-region (segmented capsule) by default. multi=True switches to
    independent toggle pills with comma-separated `region` URL param —
    clicking adds/removes from the selection. Last selected region cannot
    be deselected (would leave nothing to query).

    `regions` overrides the default REGION_ORDER list — e.g. pass
    ['NEM','NSW1','QLD1','SA1','TAS1','VIC1'] for the Generation stack
    where a NEM-wide view is meaningful."""
    region_list = regions if regions is not None else REGION_ORDER
    if not multi:
        pills = []
        for region in region_list:
            params = dict(other_params, region=region)
            url = _build_url(base_url, **params)
            cls = "pill active" if region == active else "pill"
            pills.append(
                f'<button class="{cls}" '
                f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
                f'{_region_display(region)}</button>'
            )
        return (f'<div class="pill-bar">'
                f'<span class="pill-bar-label">Region</span>'
                f'<div class="pill-group">{"".join(pills)}</div>'
                f'</div>')

    selected = set((active or "").split(","))
    pills = []
    for region in region_list:
        is_active = region in selected
        if is_active and len(selected) > 1:
            new_set = selected - {region}      # toggle off
        elif is_active:
            new_set = selected                  # last one — no-op
        else:
            new_set = selected | {region}      # toggle on
        new_param = ",".join(r for r in region_list if r in new_set)
        params = dict(other_params, region=new_param)
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{_region_display(region)}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Region</span>'
            f'<div class="pill-toggles">{"".join(pills)}</div>'
            f'</div>')


# Fuel pill bar for the Trends subtab. Maps to production PenetrationTab's
# fuel_select values: VRE = aggregate Wind+Solar+Rooftop, else single fuel.
TRENDS_FUEL_OPTIONS = [("vre", "VRE"), ("wind", "Wind"),
                       ("solar", "Solar"), ("rooftop", "Rooftop")]
TRENDS_FUEL_TO_PROD = {"vre": "VRE", "wind": "Wind",
                       "solar": "Solar", "rooftop": "Rooftop"}


def _render_fuel_pills(base_url: str, active: str, other_params: dict) -> str:
    """Single-select fuel pill bar — VRE / Wind / Solar / Rooftop."""
    pills = []
    for slug, label in TRENDS_FUEL_OPTIONS:
        params = dict(other_params, fuel=slug)
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Fuel</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _render_smooth_pills(base_url: str, smooth_on: bool, other_params: dict) -> str:
    """On/Off segmented pill for line smoothing (LOESS)."""
    pills = []
    for state, label in (("on", "On"), ("off", "Off")):
        params = dict(other_params, smooth=state)
        url = _build_url(base_url, **params)
        is_active = (state == "on") == smooth_on
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Smooth</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


# ── Gas (STTM) hub + range pills ────────────────────────────────────────────
# Power data is sub-hourly; STTM is daily back to 2010, so the standard
# 1h/24h/7d ladder makes no sense. Gas uses month/year presets.

GAS_HUBS = [
    ("SYD", "Sydney"),
    ("BRI", "Brisbane"),
    ("ADL", "Adelaide"),
    ("AVG", "STTM Avg"),
]
GAS_HUB_COLORS = {
    "SYD": "#205EA6",   # FLEXOKI_ACCENT['blue']
    "BRI": "#BC5215",   # FLEXOKI_ACCENT['orange']
    "ADL": "#24837B",   # FLEXOKI_ACCENT['cyan']
    "AVG": "#100F0F",   # ink — composite line sits on top in black
}
GAS_RANGE_OPTIONS = [("3m", "3M"), ("6m", "6M"), ("1y", "1Y"),
                     ("2y", "2Y"), ("5y", "5Y"), ("all", "All")]
GAS_RANGE_DAYS = {"3m": 90, "6m": 180, "1y": 365,
                  "2y": 730, "5y": 1825, "all": None}


def _render_gas_hub_pills(base_url: str, active: str,
                          other_params: dict) -> str:
    """Multi-select hub pills. `active` is a comma-separated list of hub codes
    (SYD/BRI/ADL/AVG). Clicking toggles. Last-selected hub cannot be turned
    off (matches the multi-region pattern)."""
    selected = set((active or "").split(","))
    pills = []
    for code, label in GAS_HUBS:
        is_active = code in selected
        if is_active and len(selected) > 1:
            new_set = selected - {code}
        elif is_active:
            new_set = selected
        else:
            new_set = selected | {code}
        # Preserve canonical order so URL is stable.
        new_param = ",".join(c for c, _ in GAS_HUBS if c in new_set)
        params = dict(other_params, hub=new_param)
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Hub</span>'
            f'<div class="pill-toggles">{"".join(pills)}</div>'
            f'</div>')


def _render_gas_range_pills(base_url: str, active: str,
                            other_params: dict) -> str:
    """Range pills tuned for the daily STTM series. No Custom pill — the
    presets cover the meaningful windows and a Custom date picker would
    invite asking for sub-day windows that don't exist."""
    pills = []
    for slug, label in GAS_RANGE_OPTIONS:
        params = dict(other_params, range=slug)
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Range</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


# ── Generators (pivot) helpers ──────────────────────────────────────────────
# Three ordered group-slots over three dims (Region/Fuel/Owner). Station is
# always implicit between innermost chosen group and DUID. Range pills lift
# from the standard set but drop sub-day windows — 30-min data, daily totals.

PIVOT_DIMS = [("region", "Region"),
              ("fuel",   "Fuel"),
              ("owner",  "Owner")]
PIVOT_RANGE_OPTIONS = [("7d", "7D"),  ("30d", "30D"), ("90d", "90D"),
                       ("ytd", "YTD"), ("1y", "1Y"),   ("5y", "5Y")]
PIVOT_COLUMNS = [
    ("gwh",   "GWh",      "Generation (GWh) — discharge for batteries"),
    ("rev",   "Rev ($M)", "Revenue ($M) — VWAP × generation"),
    ("price", "$/MWh",    "Volume-weighted average price"),
    ("util",  "Util (%)", "Capacity factor — batteries: storage_mwh/24 base"),
    ("cap",   "Cap (MW)", "Nameplate capacity"),
]
PIVOT_FUEL_GROUPS = [
    ("Coal",            "Coal"),
    ("Gas",             "Gas"),       # CCGT + OCGT + Gas other
    ("Hydro",           "Hydro"),     # raw fuel = Water
    ("Wind",            "Wind"),
    ("Solar",           "Solar"),
    ("Battery Storage", "Battery"),
    ("Biomass",         "Biomass"),
    ("Other",           "Other"),
]
# Map display-fuel slug to the raw d.fuel values that feed into it.
PIVOT_FUEL_TO_RAW = {
    "Coal":            ["Coal"],
    "Gas":             ["CCGT", "OCGT", "Gas other"],
    "Hydro":           ["Water"],
    "Wind":            ["Wind"],
    "Solar":           ["Solar"],
    "Battery Storage": ["Battery Storage"],
    "Biomass":         ["Biomass"],
    "Other":           ["Other"],
}


def _render_pivot_group_pills(base_url: str, slot: int, active: str | None,
                              disabled_dims: set[str],
                              other_params: dict) -> str:
    """One ordered group slot. `slot` is 1/2/3; `active` is the dim slug
    selected for this slot (or None for slot 3 which can be empty).
    `disabled_dims` lists dims chosen in earlier slots — clicking them is a
    no-op (visually greyed). For slot ≥2 a '(none)' pill is offered so the
    user can drop the slot entirely."""
    pills = []
    if slot >= 2:
        params = dict(other_params, **{f"g{slot}": ""})
        url = _build_url(base_url, **params)
        cls = "pill active" if not active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'(none)</button>'
        )
    for slug, label in PIVOT_DIMS:
        is_active = slug == active
        is_disabled = slug in disabled_dims and not is_active
        if is_disabled:
            pills.append(f'<button class="pill disabled" disabled>{label}</button>')
            continue
        params = dict(other_params, **{f"g{slot}": slug})
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Group {slot}</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _render_pivot_range_pills(base_url: str, active: str,
                              other_params: dict,
                              start: str = "", end: str = "") -> str:
    """Preset pills + Custom. Mirrors _render_range_pills but uses the
    pivot-specific option list (no 1H/24H; adds 90D and 5Y)."""
    pills = []
    for slug, label in PIVOT_RANGE_OPTIONS:
        params = {k: v for k, v in other_params.items()
                  if k not in ("start", "end")}
        params["range"] = slug
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    custom_params = dict(other_params, range="custom")
    custom_url = _build_url(base_url, **custom_params)
    custom_cls = "pill active" if active == "custom" else "pill"
    pills.append(
        f'<button class="{custom_cls}" '
        f'hx-get="{custom_url}" hx-target="#tab-body" hx-push-url="true">'
        f'Custom</button>'
    )

    form_html = ""
    if active == "custom":
        today = datetime.now(NEM_TZ).date()
        default_start = (today - timedelta(days=30)).isoformat()
        default_end = today.isoformat()
        s_val = start or default_start
        e_val = end or default_end
        hidden = "".join(
            f'<input type="hidden" name="{k}" value="{v}">'
            for k, v in other_params.items() if v and k not in ("start", "end")
        )
        form_html = f"""
        <form class="custom-form" hx-get="{base_url}"
              hx-target="#tab-body" hx-push-url="true">
          {hidden}
          <input type="hidden" name="range" value="custom">
          <input type="date" name="start" value="{s_val}" max="{today.isoformat()}">
          <span class="sep">to</span>
          <input type="date" name="end" value="{e_val}" max="{today.isoformat()}">
          <button type="submit">Apply</button>
        </form>"""

    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Range</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'{form_html}'
            f'</div>')


def _render_pivot_column_pills(base_url: str, active_cols: list[str],
                               other_params: dict) -> str:
    """Multi-select column toggles. Last-on column cannot be toggled off."""
    selected = set(active_cols)
    pills = []
    for slug, label, _desc in PIVOT_COLUMNS:
        is_active = slug in selected
        if is_active and len(selected) > 1:
            new_set = selected - {slug}
        elif is_active:
            new_set = selected
        else:
            new_set = selected | {slug}
        new_param = ",".join(s for s, _, _ in PIVOT_COLUMNS if s in new_set)
        params = dict(other_params, cols=new_param)
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Columns</span>'
            f'<div class="pill-toggles">{"".join(pills)}</div>'
            f'</div>')


def _render_pivot_fuel_pills(base_url: str, active_fuels: list[str],
                             other_params: dict) -> str:
    """Multi-select fuel filter (display-fuel groups)."""
    selected = set(active_fuels)
    pills = []
    for code, label in PIVOT_FUEL_GROUPS:
        is_active = code in selected
        if is_active and len(selected) > 1:
            new_set = selected - {code}
        elif is_active:
            new_set = selected
        else:
            new_set = selected | {code}
        new_param = ",".join(c for c, _ in PIVOT_FUEL_GROUPS if c in new_set)
        params = dict(other_params, fuel=new_param)
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Fuel</span>'
            f'<div class="pill-toggles">{"".join(pills)}</div>'
            f'</div>')


def _render_range_pills(base_url: str, active: str, other_params: dict,
                        start: str = "", end: str = "",
                        options: list[tuple[str, str]] | None = None) -> str:
    """Preset range pills + a Custom pill. When range=custom, the date form
    renders inline next to the pills so the user can refine the window.

    `options` lets a caller override the default RANGE_OPTIONS list (used by
    the station-analysis tab to expose 90D/5Y so its pills line up with the
    pivot's range set)."""
    pills = []
    opts = options if options is not None else RANGE_OPTIONS
    for slug, label in opts:
        # Preset click drops any custom start/end.
        params = {k: v for k, v in other_params.items() if k not in ("start", "end")}
        params["range"] = slug
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    # Custom pill — sets range=custom, server then renders the form alongside.
    custom_params = dict(other_params, range="custom")
    custom_url = _build_url(base_url, **custom_params)
    custom_cls = "pill active" if active == "custom" else "pill"
    pills.append(
        f'<button class="{custom_cls}" '
        f'hx-get="{custom_url}" hx-target="#tab-body" hx-push-url="true">'
        f'Custom</button>'
    )

    # Inline form, visible only when the user has actually selected custom.
    form_html = ""
    if active == "custom":
        today = datetime.now(NEM_TZ).date()
        default_start = (today - timedelta(days=7)).isoformat()
        default_end = today.isoformat()
        s_val = start or default_start
        e_val = end or default_end
        hidden = "".join(
            f'<input type="hidden" name="{k}" value="{v}">'
            for k, v in other_params.items() if v and k not in ("start", "end")
        )
        form_html = f"""
        <form class="custom-form" hx-get="{base_url}"
              hx-target="#tab-body" hx-push-url="true">
          {hidden}
          <input type="hidden" name="range" value="custom">
          <input type="date" name="start" value="{s_val}" max="{today.isoformat()}">
          <span class="sep">to</span>
          <input type="date" name="end" value="{e_val}" max="{today.isoformat()}">
          <button type="submit">Apply</button>
        </form>"""

    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Range</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'{form_html}'
            f'</div>')


def _render_selector_strip(*pill_bars: str) -> str:
    return f'<div class="selector-strip">{"".join(pill_bars)}</div>'


def _placeholder(tab_label: str, sub_label: str | None = None) -> str:
    sub_bit = f" &middot; {sub_label}" if sub_label else ""
    return (f'<div class="placeholder">'
            f'<p><strong>{tab_label}{sub_bit}</strong></p>'
            f'<p>This tab is not yet built. The nav scaffold is in place '
            f'so layout decisions can be reviewed before content is ported.</p>'
            f'</div>')


# ============================================================================
# Page-level routes — one per tab + a few redirects
# ============================================================================

@app.get("/", response_class=HTMLResponse)
def root() -> RedirectResponse:
    return RedirectResponse(url="/today")


@app.get("/today", response_class=HTMLResponse)
def today_page(request: Request) -> HTMLResponse:
    body_partial = (Path(__file__).parent / "today_body.html").read_text()
    body = _render_tab_body("", body_partial)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /prices  — worked example of the selector-strip pattern (single region + range)
# ----------------------------------------------------------------------------

def _range_window(range_slug: str, start: str | None, end: str | None
                  ) -> tuple[pd.Timestamp, pd.Timestamp, str, str]:
    """Resolve a (start, end, table, human_label) tuple for a range slug.

    Picks prices5 for windows ≤24h, prices30 otherwise (matches the production
    iOS API's downsampling decision at prices.py:_pick_price_table)."""
    now_ts = pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None))
    presets = {
        "1h":  (timedelta(hours=1),  "last 1h"),
        "24h": (timedelta(hours=24), "last 24h"),
        "7d":  (timedelta(days=7),   "last 7 days"),
        "30d": (timedelta(days=30),  "last 30 days"),
        "1y":  (timedelta(days=365), "last 12 months"),
    }
    if range_slug in presets:
        delta, label = presets[range_slug]
        return (now_ts - delta, now_ts,
                "prices5" if delta <= timedelta(hours=24) else "prices30",
                label)
    if range_slug == "ytd":
        return (pd.Timestamp(year=now_ts.year, month=1, day=1), now_ts,
                "prices30", "year to date")
    if range_slug == "all":
        return (pd.Timestamp(2010, 1, 1), now_ts, "prices30", "all time")
    if range_slug == "custom":
        if start and end:
            s = pd.Timestamp(start)
            e = pd.Timestamp(end) + timedelta(days=1)  # inclusive end day
            tbl = "prices5" if (e - s) <= timedelta(hours=24) else "prices30"
            return (s, e, tbl, f"{start} → {end}")
        # Custom with no dates yet → default to last 7 days
        return (now_ts - timedelta(days=7), now_ts, "prices30", "last 7 days")
    # Unknown slug fallback
    return (now_ts - timedelta(hours=24), now_ts, "prices5", "last 24h")


# Display fuel buckets that get a VWAP row, in display order.
PRICE_TABLE_FUELS = ["Battery", "Gas", "Hydro", "Coal", "Wind", "Solar"]
RAW_TO_TABLE_FUEL = {
    "Battery Storage": "Battery",
    "CCGT": "Gas", "OCGT": "Gas", "Gas other": "Gas",
    "Water": "Hydro",
    "Coal": "Coal", "Wind": "Wind", "Solar": "Solar",
}


def _vwap_by_fuel_region(regions: list[str], s_ts, e_ts) -> pd.DataFrame:
    """Compute VWAP per (region, display_fuel) over the window.

    Critical correctness points:
      * Both sides on 30-min grid (generation_by_fuel_30min × prices30) — the
        interval-duration term in MWh cancels in the num/denom ratio. Mixing
        5-min generation with 30-min prices would introduce a 6× bias on the
        5-min side.
      * Raw fuel buckets (CCGT/OCGT/Gas other → Gas; Battery Storage → Battery;
        Water → Hydro) are re-aggregated by *summing num and denom* first, then
        taking the ratio. Averaging the per-bucket VWAPs is wrong because each
        bucket carries different volume weight.
      * Only positive generation included — battery charging (negative gen)
        would invert the VWAP sign of the "Battery" row.
    """
    region_list = ",".join(f"'{r}'" for r in regions)
    raw = q(
        f"""WITH g AS (
                SELECT settlementdate, region, fuel_type,
                       SUM(total_generation_mw) AS gen_mw
                  FROM generation_by_fuel_30min
                 WHERE settlementdate >= ? AND settlementdate < ?
                   AND region IN ({region_list})
                 GROUP BY settlementdate, region, fuel_type
            )
            SELECT g.region, g.fuel_type,
                   SUM(g.gen_mw * p.rrp) AS num,
                   SUM(g.gen_mw)         AS denom
              FROM g
              JOIN prices30 p
                ON p.settlementdate = g.settlementdate
               AND p.regionid       = g.region
             WHERE g.gen_mw > 0
             GROUP BY g.region, g.fuel_type""",
        [s_ts, e_ts],
    )
    if raw.empty:
        return raw.assign(display_fuel=[], vwap=[])

    raw["display_fuel"] = raw["fuel_type"].map(RAW_TO_TABLE_FUEL)
    raw = raw.dropna(subset=["display_fuel"])
    agg = (raw.groupby(["region", "display_fuel"], as_index=False)
              [["num", "denom"]].sum())
    agg["vwap"] = agg["num"] / agg["denom"]
    return agg


def _format_dollar(v) -> str:
    if v is None or pd.isna(v):
        return "&mdash;"
    return f"{int(round(v)):,}"


def _build_prices_stats_table(regions: list[str], prices_df: pd.DataFrame,
                              s_ts, e_ts, range_label: str) -> str:
    # Mean / Max / Min from raw prices in the window. Mean is the simple
    # time-weighted price average (no volume weighting), matching production.
    stats = (prices_df.groupby("regionid")["rrp"]
             .agg(["mean", "max", "min"]))
    vwap = _vwap_by_fuel_region(regions, s_ts, e_ts)
    vwap_pivot = (vwap.set_index(["display_fuel", "region"])["vwap"]
                       if not vwap.empty else pd.Series(dtype=float))

    # Header: region columns coloured to match the chart legend.
    head_cells = "".join(
        f'<th style="text-align:right;padding:10px 14px;'
        f'color:{REGION_COLORS[r]};font-weight:600;font-size:13px">{r}</th>'
        for r in regions
    )
    header = (f'<tr style="border-bottom:1px solid {BORDER}">'
              f'<th style="text-align:left;padding:10px 14px;font-weight:600;'
              f'font-size:13px;color:{INK}">Statistic</th>{head_cells}</tr>')

    def stat_row(label: str, getter, *, mean_row: bool = False) -> str:
        bg = "background:#f0f3e8;" if mean_row else ""
        label_color = TEAL if mean_row else INK
        weight = "600" if mean_row else "400"
        cells = "".join(
            f'<td style="text-align:right;padding:8px 14px;color:'
            f'{TEAL if mean_row else INK};font-weight:{weight};font-size:14px">'
            f'{_format_dollar(getter(r))}</td>'
            for r in regions
        )
        return (f'<tr style="{bg}border-bottom:1px solid {BORDER}">'
                f'<td style="padding:8px 14px;color:{label_color};'
                f'font-weight:{weight};font-size:14px">{label}</td>'
                f'{cells}</tr>')

    stat_rows_html = (
        stat_row("Mean", lambda r: stats.loc[r, "mean"] if r in stats.index else None,
                 mean_row=True)
        + stat_row("Max", lambda r: stats.loc[r, "max"] if r in stats.index else None)
        + stat_row("Min", lambda r: stats.loc[r, "min"] if r in stats.index else None)
    )

    # VWAP rows — only show fuels that actually have data in this window.
    vwap_rows_html = ""
    if not vwap.empty:
        for fuel in PRICE_TABLE_FUELS:
            if (vwap["display_fuel"] == fuel).any():
                vwap_rows_html += stat_row(
                    fuel,
                    lambda r, f=fuel: (vwap_pivot.loc[(f, r)]
                                        if (f, r) in vwap_pivot.index else None),
                )
        # Visual separator between simple stats and VWAP rows.
        sep = (f'<tr><td colspan="{len(regions)+1}" '
               f'style="border-top:2px solid {BORDER};padding:0;height:0"></td></tr>')
        vwap_rows_html = sep + vwap_rows_html

    return (_card_h3(f"Price statistics ($/MWh) &middot; {range_label}")
            + f'<table style="width:100%;border-collapse:collapse">'
            + f'<thead>{header}</thead>'
            + f'<tbody>{stat_rows_html}{vwap_rows_html}</tbody></table>'
            + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
              f'line-height:1.5">VWAPs are volume-weighted by 30-min generation; '
              f'Mean is the time-weighted price average.</p>'
            + _attribution())


def _build_tod_chart(regions: list[str], s_ts, e_ts, range_label: str) -> str:
    """Time of day: mean RAW price per hour-of-day for each selected region.

    Uses simple mean (no LOESS, no VWAP) because the cross-day averaging at
    each hour is itself a smoothing operation. Layering LOESS on top of
    hour means would compound the smoothing and bury the very peak/trough
    structure the TOD chart exists to show.
    """
    if (e_ts - s_ts) < timedelta(hours=24):
        # 1h range collapses to a single hour bucket — chart degenerates.
        return ""

    region_list = ",".join(f"'{r}'" for r in regions)
    # 30-min data is plenty for hour-of-day means and is cheaper to scan.
    table = "prices30" if (e_ts - s_ts) > timedelta(hours=24) else "prices5"
    df = q(
        f"""SELECT EXTRACT(HOUR FROM settlementdate) AS hour,
                   regionid, AVG(rrp) AS mean_price
              FROM {table}
             WHERE settlementdate >= ? AND settlementdate < ?
               AND regionid IN ({region_list})
             GROUP BY hour, regionid
             ORDER BY hour, regionid""",
        [s_ts, e_ts],
    )
    if df.empty:
        return ""

    pivot = df.pivot(index="hour", columns="regionid", values="mean_price")

    fig = go.Figure()
    for region in regions:
        if region not in pivot.columns:
            continue
        y = pivot[region].to_numpy(dtype=float)
        fig.add_trace(go.Scatter(
            x=pivot.index, y=y, name=region, mode="lines+markers",
            line=dict(color=REGION_COLORS[region], width=1.8),
            marker=dict(size=5),
            hovertemplate="%{x:02d}:00 &middot; $%{y:.0f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=280, margin=dict(l=48, r=12, t=8, b=42),
        legend=dict(orientation="v", yanchor="middle", y=0.5,
                    xanchor="left", x=1.005, font=dict(size=10),
                    bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   tickmode="array", tickvals=list(range(0, 24, 3)),
                   ticktext=[f"{h:02d}:00" for h in range(0, 24, 3)],
                   title=dict(text="Hour of day (NEM time)",
                              font=dict(size=10, color=MUTED))),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   autorange=True,
                   title=dict(text="$/MWh", font=dict(size=10, color=MUTED))),
    )
    title = f"Time of day &middot; mean raw price by hour &middot; {range_label}"
    div_id = f"plot-tod-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (
        _card_h3(title)
        + f'<div id="{div_id}" style="height:280px"></div>'
        + f'<script>(function(){{var f={fig_json};'
          f'Plotly.newPlot("{div_id}",f.data,f.layout,'
          f'{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
          f'line-height:1.5">Hourly means over the selected window. '
          f'Per-region means are in the stats table above.</p>'
        + _attribution()
    )


def _prices_analysis_content(regions: list[str], range_slug: str, smooth: bool,
                              start: str | None, end: str | None) -> str:
    s_ts, e_ts, table, range_label = _range_window(range_slug, start, end)
    region_list = ",".join(f"'{r}'" for r in regions)

    df = q(
        f"""SELECT settlementdate, regionid, rrp FROM {table}
            WHERE regionid IN ({region_list})
              AND settlementdate >= ? AND settlementdate < ?
            ORDER BY settlementdate""",
        [s_ts, e_ts],
    )
    if df.empty:
        return ('<div class="placeholder"><p><strong>No price data for '
                f'{", ".join(r[:-1] for r in regions)} in {range_label}.</strong></p></div>')

    pivot = df.pivot_table(index="settlementdate", columns="regionid",
                           values="rrp", aggfunc="mean").sort_index()

    fig = go.Figure()
    for region in regions:
        if region not in pivot.columns:
            continue
        raw = pivot[region].to_numpy(dtype=float)
        y_display = _smooth_region(raw) if smooth else raw
        fig.add_trace(go.Scatter(
            x=pivot.index, y=y_display, name=region, mode="lines",
            line=dict(color=REGION_COLORS[region], width=1.8),
            hovertemplate="%{x|%-d %b %H:%M} $%{y:.0f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=340, margin=dict(l=48, r=12, t=8, b=32),
        legend=dict(orientation="v", yanchor="middle", y=0.5,
                    xanchor="left", x=1.005, font=dict(size=10),
                    bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED)),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   autorange=True,
                   title=dict(text="$/MWh", font=dict(size=10, color=MUTED))),
    )
    note = " &middot; LOESS smoothed" if smooth else " &middot; raw"
    region_list_str = ", ".join(r[:-1] for r in regions)
    chart_title = f"{region_list_str} spot prices &middot; {range_label}{note}"
    div_id = f"plot-prices-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    chart_html = (
        _card_h3(chart_title)
        + f'<div id="{div_id}" style="height:340px"></div>'
        + f'<script>(function(){{var f={fig_json};'
          f'Plotly.newPlot("{div_id}",f.data,f.layout,'
          f'{PLOTLY_CFG});}})();</script>'
        + _attribution()
    )

    stats_html = _build_prices_stats_table(regions, df, s_ts, e_ts, range_label)
    tod_html = _build_tod_chart(regions, s_ts, e_ts, range_label)

    tod_card = f'<div class="card">{tod_html}</div>' if tod_html else ""
    return (f'<div class="prices-stack">'
            f'<div class="card">{chart_html}</div>'
            f'<div class="card">{stats_html}</div>'
            f'{tod_card}'
            f'</div>')


# ----------------------------------------------------------------------------
# Price bands — reuses production's compute/render functions verbatim
# ----------------------------------------------------------------------------

BAND_ORDER_LOCAL = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']
BAND_COLORS = {
    'Below $0':    "#af3029",  # red
    '$0-$300':     "#66800b",  # green
    '$301-$1000':  "#bc5215",  # orange
    'Above $1000': "#a02f6f",  # magenta
}


def _build_accurate_band_data(regions: list[str], s_ts, e_ts) -> pd.DataFrame:
    """Per-interval revenue band breakdown.

    Replaces production's shortcut formula (band_avg_price × hours_in_band ×
    window_avg_demand) with the exact form: Σ(price_i × demand_i × 0.5)
    paired at the 30-min interval level. Bucketing CASE mirrors production
    exactly so % time and band average price stay byte-identical (only
    revenue changes).

    Cross-checked against direct SQL aggregation: sum of per-band revenue
    across bands = direct Σ(p×d×0.5) for the same window, to the dollar
    (verified for NSW1+VIC1 over 30d window 17 Apr → 17 May 2026, no prices
    fell in production's [$300, $301) gap).

    Returns a DataFrame with columns: regionid, band, n, band_avg_price,
    revenue_dollars, energy_mwh, total_n, pct_time, contribution,
    pct_contribution. Sorted by region order then band order.
    """
    region_list = ",".join(f"'{r}'" for r in regions)
    df = q(
        f"""WITH joined AS (
                SELECT p.regionid, p.rrp, d.demand,
                       CASE
                         WHEN p.rrp < 0                       THEN 'Below $0'
                         WHEN p.rrp >= 0    AND p.rrp < 300   THEN '$0-$300'
                         WHEN p.rrp >= 301  AND p.rrp < 1000  THEN '$301-$1000'
                         WHEN p.rrp >= 1000                   THEN 'Above $1000'
                         ELSE NULL
                       END AS band
                  FROM prices30 p
                  JOIN demand30 d
                    ON p.settlementdate = d.settlementdate
                   AND p.regionid       = d.regionid
                 WHERE p.settlementdate >= ? AND p.settlementdate < ?
                   AND p.regionid IN ({region_list})
            )
            SELECT regionid, band,
                   COUNT(*)                       AS n,
                   AVG(rrp)                       AS band_avg_price,
                   SUM(rrp * demand * 0.5)        AS revenue_dollars,
                   SUM(demand * 0.5)              AS energy_mwh
              FROM joined
             WHERE band IS NOT NULL
             GROUP BY regionid, band""",
        [s_ts, e_ts],
    )
    if df.empty:
        return df

    totals = df.groupby("regionid")["n"].sum().rename("total_n")
    df = df.merge(totals, on="regionid")
    df["pct_time"] = df["n"] / df["total_n"] * 100
    df["contribution"] = (df["n"] / df["total_n"]) * df["band_avg_price"]
    region_means = df.groupby("regionid")["contribution"].sum().rename("region_mean")
    df = df.merge(region_means, on="regionid")
    df["pct_contribution"] = (df["contribution"] / df["region_mean"] * 100).where(
        df["region_mean"] != 0, 0
    )

    df["_r"] = df["regionid"].apply(lambda r: REGION_ORDER.index(r)
                                              if r in REGION_ORDER else 999)
    df["_b"] = df["band"].apply(lambda b: BAND_ORDER_LOCAL.index(b)
                                           if b in BAND_ORDER_LOCAL else 999)
    return (df.sort_values(["_r", "_b"])
              .drop(columns=["_r", "_b"])
              .reset_index(drop=True))


def _format_revenue(dollars: float) -> str:
    """$5m for sub-$1bn, $1.5bn above. Mirrors production's revenue_str logic."""
    millions = dollars / 1_000_000
    if abs(millions) >= 1000:
        return f"${millions/1000:,.1f}bn"
    return f"${millions:,.0f}m"


def _build_accurate_band_detail_df(regions: list[str], s_ts, e_ts) -> pd.DataFrame:
    """Format the accurate band data into the production-table column layout
    so the existing _band_detail_df_to_html renderer handles it."""
    raw = _build_accurate_band_data(regions, s_ts, e_ts)
    if raw.empty:
        return raw
    rows = []
    prev_region = None
    for _, r in raw.iterrows():
        region_display = r["regionid"] if r["regionid"] != prev_region else ""
        prev_region = r["regionid"]
        rows.append({
            "Region":         region_display,
            "Price Band":     r["band"],
            "% of Time":      f"{r['pct_time']:.1f}%",
            "Avg Price":      f"${r['band_avg_price']:.0f}",
            "Revenue":        _format_revenue(r["revenue_dollars"]),
            "Contribution":   f"${r['contribution']:.1f}",
            "% Contribution": f"{r['pct_contribution']:.1f}%",
        })
    return pd.DataFrame(rows)


_DOLLAR_RE = __import__("re").compile(r"\$(-?\d+(?:\.\d+)?)")


def _add_thousands(s: str) -> str:
    """Insert comma thousands separators into any $-prefixed number in s.

    Production's `build_band_detail_table` formats values as bare $1234 / $1234.5
    strings; this re-formats them as $1,234 / $1,234.5. Leaves the 'm'/'bn'
    Revenue suffixes untouched."""
    def repl(m: "re.Match") -> str:
        num = m.group(1)
        if "." in num:
            i, d = num.split(".")
            return f"${int(i):,}.{d}"
        return f"${int(num):,}"
    return _DOLLAR_RE.sub(repl, str(s))


def _band_detail_df_to_html(df: pd.DataFrame) -> str:
    """Render production's band-detail DataFrame as a Flexoki-styled HTML
    table. Numeric columns are right-aligned (header and cells); $ values
    get thousands separators."""
    from aemo_dashboard.prices.price_bands import BAND_COLORS

    NUMERIC_COLS = {"% of Time", "Avg Price", "Revenue",
                    "Contribution", "% Contribution"}

    def align_for(col: str) -> str:
        return "right" if col in NUMERIC_COLS else "left"

    head = "".join(
        f'<th style="text-align:{align_for(c)};padding:8px 12px;'
        f'font-weight:600;font-size:13px;color:{INK}">{c}</th>'
        for c in df.columns
    )

    body_rows = []
    for _, row in df.iterrows():
        cells = []
        for col, val in row.items():
            align = align_for(col)
            display = _add_thousands(val) if col in NUMERIC_COLS else val
            extra = ""
            if col == "Price Band" and val in BAND_COLORS:
                dot = (f'<span style="display:inline-block;width:8px;height:8px;'
                       f'background:{BAND_COLORS[val]};border-radius:50%;'
                       f'margin-right:6px;vertical-align:middle"></span>')
                display = f"{dot}{val}"
            if col == "Region" and val and val in REGION_COLORS:
                extra = f"color:{REGION_COLORS[val]};font-weight:600;"
            cells.append(
                f'<td style="padding:6px 12px;text-align:{align};{extra}">{display}</td>'
            )
        body_rows.append(
            f'<tr style="border-bottom:1px solid {BORDER}">{"".join(cells)}</tr>'
        )
    return (
        f'<table style="width:100%;border-collapse:collapse;font-size:13px">'
        f'<thead><tr style="border-bottom:1px solid {BORDER}">{head}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody></table>'
    )


def _prices_bands_content(regions: list[str], range_slug: str,
                          start: str | None, end: str | None) -> str:
    """Butterfly + detail table for price-band decomposition. Reuses
    production's compute_price_bands / build_band_charts / build_band_detail_table.
    Smoothing has no effect on this subtab — bands are histogram-style on
    raw prices and smoothing would distort the % time-in-band counts."""
    from aemo_dashboard.prices.price_bands import (
        compute_price_bands, build_band_charts, build_band_detail_table,
    )

    s_ts, e_ts, table, range_label = _range_window(range_slug, start, end)
    region_list = ",".join(f"'{r}'" for r in regions)
    df = q(
        f"""SELECT settlementdate, regionid, rrp FROM {table}
            WHERE regionid IN ({region_list})
              AND settlementdate >= ? AND settlementdate < ?
            ORDER BY settlementdate""",
        [s_ts, e_ts],
    )
    if df.empty:
        return ('<div class="placeholder"><p><strong>No price data for '
                f'{", ".join(r[:-1] for r in regions)} in {range_label}.</strong></p></div>')

    # Production functions expect uppercase column names.
    prod_df = df.rename(columns={
        "regionid": "REGIONID", "rrp": "RRP",
        "settlementdate": "SETTLEMENTDATE",
    })

    _, bands_df = compute_price_bands(prod_df, regions)
    if bands_df is None or bands_df.empty:
        return '<div class="placeholder">No band data in selected window.</div>'

    fig = build_band_charts(bands_df, range_label)
    div_id = f"plot-butterfly-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    fig_height = fig.layout.height or 320
    butterfly_html = (
        _card_h3(f"Price band contribution &middot; {range_label}")
        + f'<div id="{div_id}" style="height:{fig_height}px"></div>'
        + f'<script>(function(){{var f={fig_json};'
          f'Plotly.newPlot("{div_id}",f.data,f.layout,'
          f'{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;line-height:1.5">'
          f'Left bars = % of time each band was active. Right bars = each band\'s '
          f'contribution to the flat-load average ($/MWh). Bar widths on both '
          f'sides are in percent so they\'re visually comparable.</p>'
        + _attribution()
    )

    # Cross-region contribution stack — one bar per region, bands stacked
    # within. Total bar height = region's mean price. Lets the user compare
    # *across* regions in a single chart (the butterfly's per-region subplots
    # don't make that comparison easy).
    band_data = _build_accurate_band_data(regions, s_ts, e_ts)
    stack_html = ""
    if not band_data.empty:
        stack_fig = go.Figure()
        for band in BAND_ORDER_LOCAL:
            sub = band_data[band_data["band"] == band]
            if sub.empty:
                continue
            stack_fig.add_trace(go.Bar(
                x=sub["regionid"], y=sub["contribution"],
                name=band, marker=dict(color=BAND_COLORS[band]),
                text=[f"${v:.0f}" if abs(v) >= 1 else "" for v in sub["contribution"]],
                textposition="inside", insidetextanchor="middle",
                textfont=dict(size=11, color="white"),
                hovertemplate=(f"{band}<br>%{{x}} &middot; $%{{y:.1f}}/MWh"
                               "<extra></extra>"),
            ))
        # Annotate total mean price above each bar.
        totals = band_data.groupby("regionid")["contribution"].sum().reset_index()
        for _, row in totals.iterrows():
            stack_fig.add_annotation(
                x=row["regionid"], y=row["contribution"],
                text=f"<b>${row['contribution']:.0f}</b>",
                showarrow=False, yshift=12,
                font=dict(size=12, color=INK),
            )
        stack_fig.update_layout(
            paper_bgcolor=PAPER, plot_bgcolor=PAPER,
            barmode="relative", height=320,
            margin=dict(l=48, r=12, t=20, b=44),
            legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                        xanchor="center", x=0.5, font=dict(size=11),
                        bgcolor=PAPER),
            xaxis=dict(showgrid=False, tickfont=dict(size=12, color=INK)),
            yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                       title=dict(text="$/MWh contribution",
                                  font=dict(size=10, color=MUTED)),
                       zeroline=True, zerolinecolor=BORDER, zerolinewidth=1),
        )
        stack_div = f"plot-bandstack-{int(datetime.now().timestamp() * 1000)}"
        stack_json = _plot_json(stack_fig)
        stack_html = (
            _card_h3(f"Price contribution by band &middot; {range_label}")
            + f'<div id="{stack_div}" style="height:320px"></div>'
            + f'<script>(function(){{var f={stack_json};'
              f'Plotly.newPlot("{stack_div}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
              f'line-height:1.5">Total bar height = region\'s mean spot price. '
              f'Segments show how each band pulls the mean up or down.</p>'
            + _attribution()
        )

    # Revenue calc upgraded vs production: per-interval Σ(price × demand × 0.5)
    # rather than the shortcut (band_avg × hours × window_avg_demand). See
    # /tmp/verify_band_revenue.py for the cross-check report; sum across bands
    # matches direct Σ(p×d×0.5) to the dollar. Same bucketing as production so
    # % time and avg price columns stay byte-identical.
    detail_df = _build_accurate_band_detail_df(regions, s_ts, e_ts)
    if detail_df is None or detail_df.empty:
        detail_html = ""
    else:
        detail_html = (_card_h3(f"Price band details &middot; {range_label}")
                       + _band_detail_df_to_html(detail_df)
                       + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
                         f'line-height:1.5">Revenue = Σ(price × demand × interval) '
                         f'paired at the 30-min level.</p>'
                       + _attribution())

    return (f'<div class="prices-stack">'
            f'<div class="card">{butterfly_html}</div>'
            + (f'<div class="card">{stack_html}</div>' if stack_html else '')
            + (f'<div class="card">{detail_html}</div>' if detail_html else '')
            + '</div>')


# ----------------------------------------------------------------------------
# /prices routes — root redirects to /prices/analysis; subtabs share state
# ----------------------------------------------------------------------------

@app.get("/prices", response_class=HTMLResponse)
def prices_root(region: str = "", range: str = "", smooth: str = "",
                start: str | None = None, end: str | None = None
                ) -> RedirectResponse:
    """Land on Analysis subtab, carrying any inbound query params."""
    params = {}
    if region: params["region"] = region
    if range:  params["range"]  = range
    if smooth: params["smooth"] = smooth
    if start:  params["start"]  = start
    if end:    params["end"]    = end
    target = _build_url("/prices/analysis", **params)
    return RedirectResponse(url=target)


@app.get("/prices/{sub}", response_class=HTMLResponse)
def prices_sub(sub: str, request: Request,
               region: str = "NSW1,VIC1",
               range: str = "30d",
               smooth: str = "on",
               start: str | None = None,
               end: str | None = None) -> HTMLResponse:
    _, subtabs = TAB_LOOKUP["prices"]
    sub_slugs = {s for s, _ in subtabs}
    if sub not in sub_slugs:
        return HTMLResponse(status_code=404, content="Not found")

    regions = [r for r in (region or "").split(",") if r in REGION_ORDER]
    if not regions:
        regions = ["NSW1"]
    region_param = ",".join(regions)

    valid_ranges = {slug for slug, _ in RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "30d"
    smooth_on = smooth != "off"

    base_params = {"region": region_param, "range": range,
                   "smooth": "on" if smooth_on else "off"}
    if start: base_params["start"] = start
    if end:   base_params["end"] = end

    # Selectors point at the current subtab so swap+URL update stay in subtab.
    base_url = f"/prices/{sub}"
    selectors = _render_selector_strip(
        _render_region_pills(base_url, region_param,
                             {k: v for k, v in base_params.items() if k != "region"},
                             multi=True),
        _render_range_pills(base_url, range,
                            {k: v for k, v in base_params.items()
                             if k not in ("range", "start", "end")},
                            start=start or "", end=end or ""),
        _render_smooth_pills(base_url, smooth_on,
                             {k: v for k, v in base_params.items() if k != "smooth"}),
    )

    # Subtab pills carry the selector state so switching tabs keeps filters.
    subtab_html = _render_subtab_nav("prices", subtabs, sub, carry_params=base_params)

    if sub == "analysis":
        content = _prices_analysis_content(regions, range, smooth_on, start, end)
    else:  # sub == "bands"
        content = _prices_bands_content(regions, range, start, end)

    body = _render_tab_body(subtab_html, selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /generation-mix — Stack subtab (single region + NEM, auto-resolution)
# ----------------------------------------------------------------------------
#
# Tiering matches the iOS API at aemo_dashboard/api/routers/generation.py:
#   ≤24h  → 5-min   (raw cadence, 288 points)
#   ≤7d   → 30-min  (use pre-aggregate table, ~336 points)
#   >7d   → 1-day   (daily means via time_bucket, keeps long views readable)
# Production gen_dash always resamples to 5-min which is why YTD/All views
# choke. time_bucket() at query time pushes the aggregation into DuckDB.

GENMIX_REGION_LIST = ["NEM", "NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

# From the iOS router. Per-region map of interconnector → flow direction.
# "from" = the raw meteredmwflow is signed as flow *leaving* the region, so
# we negate to render imports as positive in the stack.
INTERCONNECTOR_MAP = {
    "NSW1": {"NSW1-QLD1": "from", "VIC1-NSW1": "to",   "N-Q-MNSP1": "from"},
    "QLD1": {"NSW1-QLD1": "to",   "N-Q-MNSP1": "to"},
    "VIC1": {"VIC1-NSW1": "from", "V-SA": "from",      "V-S-MNSP1": "from", "T-V-MNSP1": "to"},
    "SA1":  {"V-SA": "to",        "V-S-MNSP1": "to"},
    "TAS1": {"T-V-MNSP1": "from"},
}

# Stacking order (bottom-to-top on the chart). Picks up from the production
# convention: thermal at the base, zero-marginal-cost on top, battery slotted
# between hydro and gas to show its peak-supply role.
STACK_FUEL_ORDER = ["Coal", "Hydro", "Wind", "Solar", "Rooftop Solar",
                    "Battery (discharge)", "Gas"]


def _pick_gen_resolution(span_seconds: float) -> tuple[str, str, str, str]:
    """Returns (res_label, ddb_interval, util_table, trans_table)."""
    hours = span_seconds / 3600
    if hours <= 24.5:
        return ("5min", "5 minutes",
                "generation_by_fuel_5min", "transmission5")
    if hours <= 24 * 7.5:
        return ("30min", "30 minutes",
                "generation_by_fuel_30min", "transmission30")
    return ("1d", "1 day",
            "generation_by_fuel_30min", "transmission30")


def _generation_stack_content(region: str, range_slug: str,
                              start: str | None, end: str | None) -> str:
    """Stacked area chart of generation by fuel for one region (or NEM)."""
    s_ts, e_ts, _, range_label = _range_window(range_slug, start, end)
    span_s = (e_ts - s_ts).total_seconds()
    res_label, ddb_interval, util_table, trans_table = _pick_gen_resolution(span_s)

    is_nem = region == "NEM"
    physical_regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    if is_nem:
        fuel_region_clause = (
            "region IN ('NSW1','QLD1','VIC1','SA1','TAS1')"
        )
        roof_region_clause = (
            "regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')"
        )
    else:
        fuel_region_clause = f"region = '{region}'"
        roof_region_clause = f"regionid = '{region}'"

    # --- Utility-scale generation, bucketed -------------------------------
    util_sql = f"""
        WITH labeled AS (
            SELECT settlementdate,
                   CASE WHEN fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        WHEN fuel_type = 'Water' THEN 'Hydro'
                        ELSE fuel_type END AS fuel,
                   total_generation_mw AS gen_mw
              FROM {util_table}
             WHERE {fuel_region_clause}
               AND settlementdate >= ? AND settlementdate < ?
               AND fuel_type NOT IN ('Biomass', 'Other')
        ),
        per_period AS (
            SELECT settlementdate, fuel, SUM(gen_mw) AS mw
              FROM labeled
             GROUP BY settlementdate, fuel
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               fuel,
               AVG(mw) AS mw
          FROM per_period
         GROUP BY bucket, fuel
         ORDER BY bucket, fuel
    """
    util_df = q(util_sql, [s_ts, e_ts])
    if util_df.empty:
        return (f'<div class="placeholder">'
                f'<p><strong>No generation data for {_region_display(region)} '
                f'in {range_label}.</strong></p></div>')

    util_pivot = (util_df.pivot(index="bucket", columns="fuel", values="mw")
                          .fillna(0))

    # --- Rooftop (30-min table; forward-fill onto util grid) --------------
    roof_sql = f"""
        WITH per_period AS (
            SELECT settlementdate, SUM(GREATEST(power, 0)) AS mw
              FROM rooftop30
             WHERE {roof_region_clause}
               AND settlementdate >= ? AND settlementdate < ?
             GROUP BY settlementdate
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               AVG(mw) AS mw
          FROM per_period
         GROUP BY bucket
         ORDER BY bucket
    """
    roof_df = q(roof_sql, [s_ts, e_ts])
    if not roof_df.empty:
        roof_aligned = (roof_df.set_index("bucket")["mw"]
                              .reindex(util_pivot.index, method="ffill")
                              .fillna(0))
    else:
        roof_aligned = pd.Series(0.0, index=util_pivot.index)

    # --- Transmission (single physical region only) -----------------------
    trans_aligned = pd.Series(0.0, index=util_pivot.index)
    if not is_nem:
        ic_map = INTERCONNECTOR_MAP.get(region, {})
        if ic_map:
            ic_ids = list(ic_map.keys())
            ic_in = ",".join(f"'{i}'" for i in ic_ids)
            from_set = [k for k, v in ic_map.items() if v == "from"]
            from_in = ",".join(f"'{i}'" for i in from_set)
            sign = (f"CASE WHEN interconnectorid IN ({from_in}) "
                    f"THEN -meteredmwflow ELSE meteredmwflow END"
                    if from_set else "meteredmwflow")
            trans_sql = f"""
                WITH per_period AS (
                    SELECT settlementdate, SUM({sign}) AS net_mw
                      FROM {trans_table}
                     WHERE interconnectorid IN ({ic_in})
                       AND settlementdate >= ? AND settlementdate < ?
                     GROUP BY settlementdate
                )
                SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
                       AVG(net_mw) AS mw
                  FROM per_period
                 GROUP BY bucket
                 ORDER BY bucket
            """
            try:
                trans_df = q(trans_sql, [s_ts, e_ts])
                if not trans_df.empty:
                    trans_aligned = (trans_df.set_index("bucket")["mw"]
                                            .reindex(util_pivot.index,
                                                     method="ffill")
                                            .fillna(0))
            except Exception:
                pass

    # --- Build the stacked traces -----------------------------------------
    fig = go.Figure()
    BATTERY_COLOR = FUEL_COLORS["Battery Storage"]  # FLEXOKI purple
    TRANS_COLOR   = "#A02F6F"                        # FLEXOKI_ACCENT['magenta']

    # Track which legendgroup names have already shown in the legend so the
    # second half (battery charging, transmission exports) reuses the parent
    # group without duplicating.
    legend_shown: set[str] = set()

    def add_trace(name: str, series: pd.Series, color: str,
                  stackgroup: str, legendgroup: str | None = None):
        # Skip empty traces (no positive values for "positive" stack, etc.)
        if stackgroup == "positive" and series.sum() <= 0: return
        if stackgroup == "negative" and series.sum() >= 0: return
        grp = legendgroup or name
        show = grp not in legend_shown
        legend_shown.add(grp)
        fig.add_trace(go.Scatter(
            x=series.index, y=series / 1000, name=name,
            stackgroup=stackgroup, mode="lines",
            legendgroup=grp, showlegend=show,
            line=dict(width=0.3, color=color),
            fillcolor=color,
            hovertemplate=f"{name}: %{{y:.2f}} GW<extra></extra>",
        ))

    # Stack order chosen so Transmission flows read as one series both sides
    # of zero — imports are the first positive trace (sits on the zero line)
    # and exports are the first negative trace (sits just below zero); battery
    # charging stacks below transmission exports so the two negatives are
    # cleanly separated. Everything else sits above transmission imports.

    # Positive stack (bottom → top): Transmission · Coal · Hydro · Wind ·
    # Solar · Rooftop · Battery discharge · Gas
    if not is_nem and trans_aligned.abs().sum() > 0:
        add_trace("Transmission", trans_aligned.clip(lower=0),
                  TRANS_COLOR, "positive", legendgroup="Transmission")
    for fuel in ("Coal", "Hydro", "Wind", "Solar"):
        if fuel in util_pivot.columns:
            add_trace(fuel, util_pivot[fuel].clip(lower=0),
                      FUEL_COLORS.get(fuel, MUTED), "positive")
    if roof_aligned.sum() > 0:
        add_trace("Rooftop Solar", roof_aligned,
                  FUEL_COLORS["Rooftop Solar"], "positive")
    if "Battery Storage" in util_pivot.columns:
        batt = util_pivot["Battery Storage"]
        add_trace("Battery", batt.clip(lower=0), BATTERY_COLOR,
                  "positive", legendgroup="Battery")
    if "Gas" in util_pivot.columns:
        add_trace("Gas", util_pivot["Gas"].clip(lower=0),
                  FUEL_COLORS["Gas"], "positive")

    # Negative stack (closest-to-zero first): Transmission exports first,
    # then Battery charging below
    if not is_nem and trans_aligned.abs().sum() > 0:
        add_trace("Transmission", trans_aligned.clip(upper=0),
                  TRANS_COLOR, "negative", legendgroup="Transmission")
    if "Battery Storage" in util_pivot.columns:
        batt = util_pivot["Battery Storage"]
        add_trace("Battery", batt.clip(upper=0), BATTERY_COLOR,
                  "negative", legendgroup="Battery")

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=440, margin=dict(l=48, r=12, t=8, b=72),
        legend=dict(orientation="h", yanchor="top", y=-0.14,
                    xanchor="center", x=0.5, font=dict(size=10),
                    bgcolor=PAPER),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED)),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   title=dict(text="GW", font=dict(size=10, color=MUTED)),
                   zeroline=True, zerolinecolor=BORDER, zerolinewidth=1),
    )
    title = (f"{_region_display(region)} generation stack &middot; "
             f"{range_label} &middot; {res_label} buckets")
    div_id = f"plot-genstack-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (
        f'<div class="prices-stack">'
        f'<div class="card">'
        f'{_card_h3(title)}'
        f'<div id="{div_id}" style="height:440px"></div>'
        f'<script>(function(){{var f={fig_json};'
        f'Plotly.newPlot("{div_id}",f.data,f.layout,'
        f'{PLOTLY_CFG});}})();</script>'
        f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        f'Bucket size auto-picked from range: 5-min for ≤24h, 30-min for ≤7d, '
        f'daily means above. Transmission imports/exports shown for single '
        f'regions only (interconnectors net to zero across NEM).'
        f'</p>'
        + _attribution()
        + f'</div>'
        f'</div>'
    )


def _generation_fuel_stats(region: str, s_ts, e_ts) -> tuple[list[dict], dict]:
    """By-fuel volume + VWAP table data, plus summary metrics.

    Always 30-min aligned (generation_by_fuel_30min × prices30 ×
    transmission30 × rooftop30). For NEM, weights each fuel's contribution
    by its source region's price — i.e. Gas in NSW is priced at NSW's RRP,
    Gas in VIC at VIC's RRP. The NEM-wide flat price uses demand-weighted
    price across the five regions.

    Returns (stats_list, summary):
      stats_list rows: fuel, avg_gw, volume_gwh, share, vwap
      summary keys: avg_total_gw, total_gwh, lwap, flat_price,
                    battery_charge {avg_gw,gwh,vwap}|None,
                    trans_export {avg_gw,gwh,vwap}|None, net_demand_gwh
    """
    is_nem = region == "NEM"
    physical = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    if is_nem:
        region_list = ",".join(f"'{r}'" for r in physical)
        region_filter = f"region IN ({region_list})"
        roof_region_filter = f"regionid IN ({region_list})"
        price_region_filter = f"regionid IN ({region_list})"
    else:
        region_filter = f"region = '{region}'"
        roof_region_filter = f"regionid = '{region}'"
        price_region_filter = f"regionid = '{region}'"

    gen_sql = f"""
        WITH gen AS (
            SELECT settlementdate, region,
                   CASE WHEN fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        WHEN fuel_type = 'Water'           THEN 'Hydro'
                        WHEN fuel_type = 'Battery Storage' THEN 'Battery'
                        ELSE fuel_type END AS fuel,
                   total_generation_mw AS gen_mw
              FROM generation_by_fuel_30min
             WHERE {region_filter}
               AND settlementdate >= ? AND settlementdate < ?
               AND fuel_type NOT IN ('Biomass', 'Other')
        ),
        agg AS (
            SELECT settlementdate, region, fuel, SUM(gen_mw) AS gen_mw
              FROM gen
             GROUP BY 1, 2, 3
        )
        SELECT a.settlementdate, a.region, a.fuel, a.gen_mw, p.rrp
          FROM agg a
          JOIN prices30 p ON p.settlementdate = a.settlementdate
                          AND p.regionid       = a.region
    """
    gen = q(gen_sql, [s_ts, e_ts])

    # Rooftop (positive only by construction)
    roof_sql = f"""
        SELECT r.settlementdate, r.regionid AS region,
               'Rooftop Solar' AS fuel,
               SUM(GREATEST(r.power, 0)) AS gen_mw,
               p.rrp
          FROM rooftop30 r
          JOIN prices30 p ON p.settlementdate = r.settlementdate
                          AND p.regionid       = r.regionid
         WHERE {roof_region_filter.replace('regionid', 'r.regionid')}
           AND r.settlementdate >= ? AND r.settlementdate < ?
         GROUP BY 1, 2, p.rrp
    """
    roof = q(roof_sql, [s_ts, e_ts])
    if not roof.empty:
        gen = pd.concat([gen, roof], ignore_index=True)

    # Transmission (single region only). imports = positive, exports = negative.
    trans = pd.DataFrame()
    if not is_nem:
        ic_map = INTERCONNECTOR_MAP.get(region, {})
        if ic_map:
            ic_ids = list(ic_map.keys())
            ic_in = ",".join(f"'{i}'" for i in ic_ids)
            from_set = [k for k, v in ic_map.items() if v == "from"]
            from_in = ",".join(f"'{i}'" for i in from_set)
            sign = (f"CASE WHEN interconnectorid IN ({from_in}) "
                    f"THEN -meteredmwflow ELSE meteredmwflow END"
                    if from_set else "meteredmwflow")
            trans_sql = f"""
                WITH t AS (
                    SELECT settlementdate, SUM({sign}) AS net_mw
                      FROM transmission30
                     WHERE interconnectorid IN ({ic_in})
                       AND settlementdate >= ? AND settlementdate < ?
                     GROUP BY settlementdate
                )
                SELECT t.settlementdate, t.net_mw, p.rrp
                  FROM t JOIN prices30 p
                    ON p.settlementdate = t.settlementdate
                   AND p.regionid       = '{region}'
            """
            try:
                trans = q(trans_sql, [s_ts, e_ts])
            except Exception:
                trans = pd.DataFrame()

    if gen.empty:
        return [], {"avg_total_gw": 0, "total_gwh": 0, "lwap": 0,
                    "flat_price": 0, "battery_charge": None,
                    "trans_export": None, "net_demand_gwh": 0}

    n_intervals = gen["settlementdate"].nunique()

    # Per-fuel rows in merit order. Battery shows discharge only here;
    # charging is a separate footer row.
    merit = ["Coal", "Wind", "Solar", "Rooftop Solar",
             "Battery", "Hydro", "Gas"]
    stats: list[dict] = []
    for fuel in merit:
        fuel_rows = gen[(gen["fuel"] == fuel) & (gen["gen_mw"] > 0)]
        if fuel_rows.empty:
            continue
        total_mw = float(fuel_rows["gen_mw"].sum())
        vwap = float((fuel_rows["gen_mw"] * fuel_rows["rrp"]).sum() / total_mw)
        stats.append({
            "fuel": fuel,
            "avg_gw": total_mw / n_intervals / 1000,
            "volume_gwh": total_mw * 0.5 / 1000,
            "vwap": vwap,
            "share": 0.0,
        })

    # Add transmission imports
    if not trans.empty:
        imports = trans[trans["net_mw"] > 0]
        if not imports.empty:
            total_mw = float(imports["net_mw"].sum())
            vwap = float((imports["net_mw"] * imports["rrp"]).sum() / total_mw)
            stats.append({
                "fuel": "Transmission",
                "avg_gw": total_mw / n_intervals / 1000,
                "volume_gwh": total_mw * 0.5 / 1000,
                "vwap": vwap,
                "share": 0.0,
            })

    sum_gwh = sum(s["volume_gwh"] for s in stats)
    if sum_gwh > 0:
        for s in stats:
            s["share"] = s["volume_gwh"] / sum_gwh * 100

    # Per-interval total supply (positive contributions only)
    supply_per_interval = (gen[gen["gen_mw"] > 0]
                             .groupby("settlementdate")["gen_mw"].sum())
    if not trans.empty:
        imports = (trans[trans["net_mw"] > 0]
                     .groupby("settlementdate")["net_mw"].sum())
        supply_per_interval = supply_per_interval.add(imports, fill_value=0)
    avg_total_gw = float(supply_per_interval.mean() / 1000) if not supply_per_interval.empty else 0
    total_supply_mwh = float(supply_per_interval.sum() * 0.5)

    # Single price series for LWAP + flat_price.
    if is_nem:
        nem_p_sql = f"""
            SELECT p.settlementdate,
                   SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) AS rrp
              FROM prices30 p
              JOIN demand30 d
                ON p.settlementdate = d.settlementdate
               AND p.regionid       = d.regionid
             WHERE {price_region_filter.replace('regionid', 'p.regionid')}
               AND p.settlementdate >= ? AND p.settlementdate < ?
             GROUP BY p.settlementdate
        """
        price_df = q(nem_p_sql, [s_ts, e_ts])
    else:
        price_df = q(
            f"""SELECT settlementdate, rrp FROM prices30
                WHERE {price_region_filter}
                  AND settlementdate >= ? AND settlementdate < ?""",
            [s_ts, e_ts],
        )
    price_series = price_df.set_index("settlementdate")["rrp"]
    aligned = price_series.reindex(supply_per_interval.index)
    if supply_per_interval.sum() > 0:
        lwap = float((aligned * supply_per_interval).sum() / supply_per_interval.sum())
    else:
        lwap = 0.0
    flat_price = float(price_series.mean()) if not price_series.empty else 0.0

    # Battery charging — negative battery rows folded to positive volumes.
    bc_summary = None
    batt_charge = gen[(gen["fuel"] == "Battery") & (gen["gen_mw"] < 0)]
    if not batt_charge.empty:
        bc_abs = batt_charge["gen_mw"].abs()
        bc_mw = float(bc_abs.sum())
        bc_vwap = float((bc_abs * batt_charge["rrp"]).sum() / bc_mw) if bc_mw else 0.0
        bc_summary = {"avg_gw": bc_mw / n_intervals / 1000,
                       "gwh": bc_mw * 0.5 / 1000, "vwap": bc_vwap}

    # Transmission exports (single region)
    te_summary = None
    if not trans.empty:
        exports = trans[trans["net_mw"] < 0]
        if not exports.empty:
            te_abs = exports["net_mw"].abs()
            te_mw = float(te_abs.sum())
            te_vwap = float((te_abs * exports["rrp"]).sum() / te_mw) if te_mw else 0.0
            te_summary = {"avg_gw": te_mw / n_intervals / 1000,
                           "gwh": te_mw * 0.5 / 1000, "vwap": te_vwap}

    net_demand_gwh = total_supply_mwh / 1000
    if bc_summary: net_demand_gwh -= bc_summary["gwh"]
    if te_summary: net_demand_gwh -= te_summary["gwh"]

    summary = {
        "avg_total_gw":  avg_total_gw,
        "total_gwh":     total_supply_mwh / 1000,
        "lwap":          lwap,
        "flat_price":    flat_price,
        "battery_charge": bc_summary,
        "trans_export":   te_summary,
        "net_demand_gwh": net_demand_gwh,
    }
    return stats, summary


def _render_fuel_table(stats: list[dict], summary: dict, range_label: str) -> str:
    """Five-column table: Fuel · Avg GW · GWh · Share % · VWAP $/MWh.
    Footer adds Total Supply, optional Battery Charge / Transmission Exports,
    Net Demand, and a Flat load $/MWh row (time-weighted price)."""
    if not stats:
        return ('<div class="placeholder">'
                '<p>No generation data for the selected range.</p></div>')

    def n(v, dec=1):
        if v is None or pd.isna(v):
            return "&mdash;"
        return f"{v:,.{dec}f}"

    def d(v):
        if v is None or pd.isna(v):
            return "&mdash;"
        return f"{int(round(v)):,}"

    head = (
        f'<tr style="border-bottom:1.5px solid {MUTED}">'
        f'<th style="text-align:left;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Fuel</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Avg GW</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">GWh</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Share %</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">VWAP $/MWh</th>'
        f'</tr>'
    )
    body_rows = []
    for s in stats:
        body_rows.append(
            f'<tr style="border-bottom:0.5px solid {BORDER}">'
            f'<td style="padding:6px 12px;color:{INK};font-weight:500">{s["fuel"]}</td>'
            f'<td style="text-align:right;padding:6px 12px">{n(s["avg_gw"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">{n(s["volume_gwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">{n(s["share"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">{d(s["vwap"])}</td>'
            f'</tr>'
        )

    def total_row(label, gw, gwh, vwap, *, is_total=False):
        border = (f"border-top:1.5px solid {MUTED};" if is_total else "")
        weight = "600" if is_total else "400"
        color = INK if is_total else INK
        return (
            f'<tr style="{border}border-bottom:0.5px solid {BORDER}">'
            f'<td style="padding:8px 12px;color:{color};font-weight:{weight}">{label}</td>'
            f'<td style="text-align:right;padding:8px 12px;font-weight:{weight}">{n(gw)}</td>'
            f'<td style="text-align:right;padding:8px 12px;font-weight:{weight}">{n(gwh)}</td>'
            f'<td></td>'
            f'<td style="text-align:right;padding:8px 12px;font-weight:{weight}">{d(vwap)}</td>'
            f'</tr>'
        )

    foot_rows = [total_row("Total Supply", summary["avg_total_gw"],
                           summary["total_gwh"], summary["lwap"], is_total=True)]
    bc = summary.get("battery_charge")
    if bc:
        foot_rows.append(total_row("Battery Charge", bc["avg_gw"], bc["gwh"], bc["vwap"]))
    te = summary.get("trans_export")
    if te:
        foot_rows.append(total_row("Transmission Exports", te["avg_gw"], te["gwh"], te["vwap"]))
    foot_rows.append(
        f'<tr style="border-top:1.5px solid {MUTED};border-bottom:0.5px solid {BORDER}">'
        f'<td style="padding:8px 12px;color:{INK};font-weight:600">Net Demand</td>'
        f'<td></td>'
        f'<td style="text-align:right;padding:8px 12px;font-weight:600">{n(summary["net_demand_gwh"])}</td>'
        f'<td></td><td></td>'
        f'</tr>'
    )
    foot_rows.append(
        f'<tr style="border-bottom:0.5px solid {BORDER}">'
        f'<td style="padding:8px 12px;color:{INK};font-weight:600">Flat load $/MWh</td>'
        f'<td></td><td></td><td></td>'
        f'<td style="text-align:right;padding:8px 12px;font-weight:600">{d(summary["flat_price"])}</td>'
        f'</tr>'
    )

    return (
        _card_h3(f"Generation by fuel &middot; {range_label}")
        + f'<table style="width:100%;border-collapse:collapse;font-size:13px">'
        + f'<thead>{head}</thead>'
        + f'<tbody>{"".join(body_rows)}{"".join(foot_rows)}</tbody></table>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;line-height:1.5">'
        f'VWAP = Σ(price × generation) / Σ(generation) at 30-min resolution. '
        f'For NEM, each region\'s output is priced at that region\'s RRP. '
        f'Flat load = simple time-weighted price.'
        f'</p>'
        + _attribution()
    )


def _generation_price_chart(region: str, range_slug: str,
                            s_ts, e_ts, range_label: str) -> str:
    """Companion price chart for the Stack subtab. LOESS-smoothed for short
    windows where wiggle dominates; raw for longer. Single region = one line;
    NEM = all five regional lines (no single 'NEM price' exists)."""
    is_nem = region == "NEM"
    span_h = (e_ts - s_ts).total_seconds() / 3600
    table = "prices5" if span_h <= 24.5 else "prices30"

    if is_nem:
        region_list = "('NSW1','QLD1','VIC1','SA1','TAS1')"
    else:
        region_list = f"('{region}')"

    df = q(
        f"""SELECT settlementdate, regionid, rrp FROM {table}
            WHERE regionid IN {region_list}
              AND settlementdate >= ? AND settlementdate < ?
            ORDER BY settlementdate""",
        [s_ts, e_ts],
    )
    if df.empty:
        return ""

    pivot = df.pivot_table(index="settlementdate", columns="regionid",
                            values="rrp", aggfunc="mean").sort_index()
    apply_smooth = span_h <= 24 * 7.5  # LOESS up to ~7 days

    fig = go.Figure()
    regions_to_plot = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"] if is_nem else [region]
    for r in regions_to_plot:
        if r not in pivot.columns:
            continue
        raw = pivot[r].to_numpy(dtype=float)
        y_disp = _smooth_region(raw) if apply_smooth else raw
        fig.add_trace(go.Scatter(
            x=pivot.index, y=y_disp, name=r, mode="lines",
            line=dict(color=REGION_COLORS[r], width=1.6),
            hovertemplate="%{x|%-d %b %H:%M} $%{y:.0f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=220, margin=dict(l=48, r=12, t=8, b=28),
        legend=dict(orientation="v", yanchor="middle", y=0.5,
                    xanchor="left", x=1.005, font=dict(size=10),
                    bgcolor="rgba(0,0,0,0)"),
        showlegend=is_nem,
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED)),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   autorange=True,
                   title=dict(text="$/MWh", font=dict(size=10, color=MUTED))),
    )
    note = " &middot; LOESS smoothed" if apply_smooth else " &middot; raw"
    region_label = _region_display(region)
    title = f"{region_label} spot prices &middot; {range_label}{note}"
    div_id = f"plot-genprice-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (
        _card_h3(title)
        + f'<div id="{div_id}" style="height:220px"></div>'
        + f'<script>(function(){{var f={fig_json};'
        f'Plotly.newPlot("{div_id}",f.data,f.layout,'
        f'{PLOTLY_CFG});}})();</script>'
        + _attribution()
    )


def _generation_tod_content(region: str, range_slug: str,
                            start: str | None, end: str | None) -> str:
    """Hour-of-day generation stack with average-price line underneath.

    Always 30-min aligned. Same stack ordering as the Stack subtab so the
    eye reads the two views the same way. Price is the simple mean of the
    selected window's prices per hour-of-day (matches production); no LOESS
    smoothing because hour-of-day means already average across days.
    """
    from plotly.subplots import make_subplots

    s_ts, e_ts, _, range_label = _range_window(range_slug, start, end)
    is_nem = region == "NEM"

    if is_nem:
        fuel_region_clause = "region IN ('NSW1','QLD1','VIC1','SA1','TAS1')"
        roof_region_clause = "regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')"
    else:
        fuel_region_clause = f"region = '{region}'"
        roof_region_clause = f"regionid = '{region}'"

    # Utility generation, mean MW per (hour, fuel) across the window.
    util_sql = f"""
        WITH labeled AS (
            SELECT settlementdate,
                   CASE WHEN fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        WHEN fuel_type = 'Water'           THEN 'Hydro'
                        WHEN fuel_type = 'Battery Storage' THEN 'Battery Storage'
                        ELSE fuel_type END AS fuel,
                   total_generation_mw AS gen_mw
              FROM generation_by_fuel_30min
             WHERE {fuel_region_clause}
               AND settlementdate >= ? AND settlementdate < ?
               AND fuel_type NOT IN ('Biomass', 'Other')
        ),
        per_period AS (
            SELECT settlementdate, fuel, SUM(gen_mw) AS mw
              FROM labeled
             GROUP BY settlementdate, fuel
        )
        SELECT EXTRACT(HOUR FROM settlementdate) AS hour, fuel, AVG(mw) AS mw
          FROM per_period
         GROUP BY hour, fuel
         ORDER BY hour, fuel
    """
    util_df = q(util_sql, [s_ts, e_ts])
    if util_df.empty:
        return (f'<div class="placeholder"><p><strong>No generation data for '
                f'{_region_display(region)} in {range_label}.</strong></p></div>')

    util_pivot = (util_df.pivot(index="hour", columns="fuel", values="mw")
                          .fillna(0))

    # Rooftop, mean MW per hour
    roof_sql = f"""
        WITH per_period AS (
            SELECT settlementdate, SUM(GREATEST(power, 0)) AS mw
              FROM rooftop30
             WHERE {roof_region_clause}
               AND settlementdate >= ? AND settlementdate < ?
             GROUP BY settlementdate
        )
        SELECT EXTRACT(HOUR FROM settlementdate) AS hour, AVG(mw) AS mw
          FROM per_period
         GROUP BY hour
         ORDER BY hour
    """
    roof_df = q(roof_sql, [s_ts, e_ts])
    roof_aligned = (roof_df.set_index("hour")["mw"]
                          .reindex(util_pivot.index).fillna(0)
                    if not roof_df.empty
                    else pd.Series(0.0, index=util_pivot.index))

    # Transmission, mean MW per hour (single region only)
    trans_aligned = pd.Series(0.0, index=util_pivot.index)
    if not is_nem:
        ic_map = INTERCONNECTOR_MAP.get(region, {})
        if ic_map:
            ic_ids = list(ic_map.keys())
            ic_in = ",".join(f"'{i}'" for i in ic_ids)
            from_set = [k for k, v in ic_map.items() if v == "from"]
            from_in = ",".join(f"'{i}'" for i in from_set)
            sign = (f"CASE WHEN interconnectorid IN ({from_in}) "
                    f"THEN -meteredmwflow ELSE meteredmwflow END"
                    if from_set else "meteredmwflow")
            trans_sql = f"""
                WITH per_period AS (
                    SELECT settlementdate, SUM({sign}) AS net_mw
                      FROM transmission30
                     WHERE interconnectorid IN ({ic_in})
                       AND settlementdate >= ? AND settlementdate < ?
                     GROUP BY settlementdate
                )
                SELECT EXTRACT(HOUR FROM settlementdate) AS hour, AVG(net_mw) AS mw
                  FROM per_period
                 GROUP BY hour
                 ORDER BY hour
            """
            try:
                trans_df = q(trans_sql, [s_ts, e_ts])
                if not trans_df.empty:
                    trans_aligned = (trans_df.set_index("hour")["mw"]
                                            .reindex(util_pivot.index).fillna(0))
            except Exception:
                pass

    # Price per hour-of-day. Single region = its RRP mean; NEM = demand-weighted.
    if is_nem:
        price_sql = """
            WITH nem AS (
                SELECT p.settlementdate,
                       SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) AS rrp
                  FROM prices30 p
                  JOIN demand30 d
                    ON p.settlementdate = d.settlementdate
                   AND p.regionid       = d.regionid
                 WHERE p.regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')
                   AND p.settlementdate >= ? AND p.settlementdate < ?
                 GROUP BY p.settlementdate
            )
            SELECT EXTRACT(HOUR FROM settlementdate) AS hour, AVG(rrp) AS rrp
              FROM nem
             GROUP BY hour
             ORDER BY hour
        """
        price_df = q(price_sql, [s_ts, e_ts])
    else:
        price_df = q(
            f"""SELECT EXTRACT(HOUR FROM settlementdate) AS hour,
                       AVG(rrp) AS rrp
                  FROM prices30
                 WHERE regionid = '{region}'
                   AND settlementdate >= ? AND settlementdate < ?
                 GROUP BY hour
                 ORDER BY hour""",
            [s_ts, e_ts],
        )

    # --- Build the figure: stack on top, price line underneath -----------
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.7, 0.3],
    )

    BATTERY_COLOR = FUEL_COLORS["Battery Storage"]
    TRANS_COLOR   = "#A02F6F"
    legend_shown: set[str] = set()

    def add_trace(name, series, color, stackgroup, legendgroup=None):
        if stackgroup == "positive" and series.sum() <= 0: return
        if stackgroup == "negative" and series.sum() >= 0: return
        grp = legendgroup or name
        show = grp not in legend_shown
        legend_shown.add(grp)
        fig.add_trace(go.Scatter(
            x=series.index, y=series / 1000, name=name,
            stackgroup=stackgroup, mode="lines",
            legendgroup=grp, showlegend=show,
            line=dict(width=0.3, color=color),
            fillcolor=color,
            hovertemplate=f"{name}: %{{y:.2f}} GW<extra></extra>",
        ), row=1, col=1)

    # Positive stack: Transmission imports first, then merit order, then Gas
    if not is_nem and trans_aligned.abs().sum() > 0:
        add_trace("Transmission", trans_aligned.clip(lower=0),
                  TRANS_COLOR, "positive", legendgroup="Transmission")
    for fuel in ("Coal", "Hydro", "Wind", "Solar"):
        if fuel in util_pivot.columns:
            add_trace(fuel, util_pivot[fuel].clip(lower=0),
                      FUEL_COLORS.get(fuel, MUTED), "positive")
    if roof_aligned.sum() > 0:
        add_trace("Rooftop Solar", roof_aligned,
                  FUEL_COLORS["Rooftop Solar"], "positive")
    if "Battery Storage" in util_pivot.columns:
        batt = util_pivot["Battery Storage"]
        add_trace("Battery", batt.clip(lower=0), BATTERY_COLOR,
                  "positive", legendgroup="Battery")
    if "Gas" in util_pivot.columns:
        add_trace("Gas", util_pivot["Gas"].clip(lower=0),
                  FUEL_COLORS["Gas"], "positive")

    # Negative stack: Transmission exports first, Battery charging below
    if not is_nem and trans_aligned.abs().sum() > 0:
        add_trace("Transmission", trans_aligned.clip(upper=0),
                  TRANS_COLOR, "negative", legendgroup="Transmission")
    if "Battery Storage" in util_pivot.columns:
        batt = util_pivot["Battery Storage"]
        add_trace("Battery", batt.clip(upper=0), BATTERY_COLOR,
                  "negative", legendgroup="Battery")

    # Price line in the bottom subplot (raw mean per hour)
    if not price_df.empty:
        fig.add_trace(go.Scatter(
            x=price_df["hour"], y=price_df["rrp"],
            name="Price", mode="lines+markers",
            line=dict(width=2, color="#D4A000"),
            marker=dict(size=5),
            showlegend=False,
            hovertemplate="%{x:02d}:00 $%{y:.0f}/MWh<extra></extra>",
        ), row=2, col=1)

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=520, margin=dict(l=48, r=12, t=8, b=44),
        legend=dict(orientation="h", yanchor="top", y=-0.10,
                    xanchor="center", x=0.5, font=dict(size=10),
                    bgcolor=PAPER),
    )
    fig.update_yaxes(
        showgrid=False, zeroline=True, zerolinecolor=BORDER, zerolinewidth=1,
        tickfont=dict(size=10, color=MUTED),
        title=dict(text="GW", font=dict(size=10, color=MUTED)),
        row=1, col=1,
    )
    fig.update_yaxes(
        showgrid=False,
        tickfont=dict(size=10, color="#D4A000"),
        title=dict(text="$/MWh", font=dict(size=10, color="#D4A000")),
        row=2, col=1,
    )
    # Hour ticks on the bottom (shared) axis
    fig.update_xaxes(showgrid=False,
                     tickfont=dict(size=10, color=MUTED),
                     tickmode="array",
                     tickvals=list(range(0, 24, 3)),
                     ticktext=[f"{h:02d}:00" for h in range(0, 24, 3)],
                     row=1, col=1)
    fig.update_xaxes(showgrid=False,
                     tickfont=dict(size=10, color=MUTED),
                     tickmode="array",
                     tickvals=list(range(0, 24, 3)),
                     ticktext=[f"{h:02d}:00" for h in range(0, 24, 3)],
                     title=dict(text="Hour of day (NEM time)",
                                font=dict(size=10, color=MUTED)),
                     row=2, col=1)

    title = (f"{_region_display(region)} generation by hour of day "
             f"&middot; {range_label}")
    div_id = f"plot-gentod-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (
        f'<div class="prices-stack"><div class="card">'
        + _card_h3(title)
        + f'<div id="{div_id}" style="height:520px"></div>'
        + f'<script>(function(){{var f={fig_json};'
        f'Plotly.newPlot("{div_id}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        f'Top: mean MW per fuel for each hour of day across the selected window. '
        f'Bottom: mean spot price per hour ('
        f'{"demand-weighted across regions" if is_nem else "regional RRP"}). '
        f'No smoothing — the hour-of-day averaging already collapses cross-day noise.'
        f'</p>'
        + _attribution()
        + '</div></div>'
    )


# Per-interconnector colors — Flexoki LINE_COLORS palette (per the
# flexoki-plotting skill). The skill's 5-series palette is Blue/Orange/
# Cyan/Green/Magenta deliberately, because Red and Orange sit too close
# on the hue wheel to discriminate at line widths. For the 6th interconnector
# (T-V-MNSP1) we use Flexoki Purple, which is non-adjacent to all the others
# and never co-occurs with Battery (the only other place we use this hex).
INTERCONNECTOR_COLORS = {
    "NSW1-QLD1":  "#205EA6",   # Flexoki Blue 600
    "VIC1-NSW1":  "#BC5215",   # Flexoki Orange 600
    "V-SA":       "#24837B",   # Flexoki Cyan 600
    "V-S-MNSP1":  "#66800B",   # Flexoki Green 600
    "N-Q-MNSP1":  "#A02F6F",   # Flexoki Magenta 600
    "T-V-MNSP1":  "#5E409D",   # Flexoki Purple 600 (was Red — too close to Orange)
}


def _generation_transmission_content(region: str, range_slug: str,
                                     start: str | None, end: str | None) -> str:
    """Per-interconnector flow series + hour-of-day, both signed so that
    positive = import to the selected region. NEM has no transmission view
    (interconnectors net to zero); placeholder is shown instead."""
    if region == "NEM":
        return ('<div class="placeholder">'
                '<p><strong>Transmission isn\'t shown for the NEM-wide view.</strong></p>'
                '<p>Pick a state region to see its interconnector flows.</p>'
                '</div>')
    ic_map = INTERCONNECTOR_MAP.get(region, {})
    if not ic_map:
        return ('<div class="placeholder">'
                f'<p><strong>No interconnectors mapped for {region}.</strong></p>'
                '</div>')

    s_ts, e_ts, _, range_label = _range_window(range_slug, start, end)
    span_h = (e_ts - s_ts).total_seconds() / 3600
    if span_h <= 24.5:
        ts_table, ddb_interval, res_label = "transmission5", "5 minutes", "5min"
    elif span_h <= 24 * 7.5:
        ts_table, ddb_interval, res_label = "transmission30", "30 minutes", "30min"
    else:
        ts_table, ddb_interval, res_label = "transmission30", "1 day", "1d"

    ic_ids = list(ic_map.keys())
    ic_in  = ",".join(f"'{i}'" for i in ic_ids)
    # Sign correction per the production convention: 'from' interconnectors
    # report raw flow as leaving the region, so we negate to render imports
    # as positive.
    case_sign = " ".join(
        f"WHEN interconnectorid = '{ic}' THEN "
        f"{'-meteredmwflow' if direction == 'from' else 'meteredmwflow'}"
        for ic, direction in ic_map.items()
    )

    # Time series, bucketed
    ts_sql = f"""
        WITH labeled AS (
            SELECT settlementdate, interconnectorid,
                   CASE {case_sign} END AS signed_flow
              FROM {ts_table}
             WHERE interconnectorid IN ({ic_in})
               AND settlementdate >= ? AND settlementdate < ?
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               interconnectorid,
               AVG(signed_flow) AS mw
          FROM labeled
         GROUP BY bucket, interconnectorid
         ORDER BY bucket, interconnectorid
    """
    ts_df = q(ts_sql, [s_ts, e_ts])

    # Time of day, always 30-min data
    tod_sql = f"""
        WITH labeled AS (
            SELECT settlementdate, interconnectorid,
                   CASE {case_sign} END AS signed_flow
              FROM transmission30
             WHERE interconnectorid IN ({ic_in})
               AND settlementdate >= ? AND settlementdate < ?
        )
        SELECT EXTRACT(HOUR FROM settlementdate) AS hour,
               interconnectorid,
               AVG(signed_flow) AS mw
          FROM labeled
         GROUP BY hour, interconnectorid
         ORDER BY hour, interconnectorid
    """
    tod_df = q(tod_sql, [s_ts, e_ts])

    if ts_df.empty:
        return ('<div class="placeholder">'
                f'<p><strong>No transmission data for {region} in {range_label}.</strong></p>'
                '</div>')

    def build_line_fig(df, height, x_field, x_axis_args, hover_x):
        fig = go.Figure()
        pivot = df.pivot(index=x_field, columns="interconnectorid", values="mw")
        for ic in ic_ids:
            if ic not in pivot.columns:
                continue
            fig.add_trace(go.Scatter(
                x=pivot.index, y=pivot[ic] / 1000, mode="lines", name=ic,
                line=dict(width=2.5, color=INTERCONNECTOR_COLORS.get(ic, MUTED)),
                hovertemplate=f"{ic}<br>{hover_x} %{{y:.2f}} GW<extra></extra>",
            ))
        fig.add_hline(y=0, line_dash="dash", line_color=BORDER,
                      line_width=1, opacity=0.6)
        fig.update_layout(
            paper_bgcolor=PAPER, plot_bgcolor=PAPER,
            height=height, margin=dict(l=48, r=12, t=8, b=44),
            legend=dict(orientation="h", yanchor="top", y=-0.14,
                        xanchor="center", x=0.5, font=dict(size=10),
                        bgcolor=PAPER),
            xaxis=x_axis_args,
            yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                       title=dict(text="GW", font=dict(size=10, color=MUTED)),
                       zeroline=False),
        )
        return fig

    ts_fig = build_line_fig(
        ts_df, height=300, x_field="bucket",
        x_axis_args=dict(showgrid=False,
                         tickfont=dict(size=10, color=MUTED)),
        hover_x="%{x|%-d %b %H:%M}",
    )
    tod_fig = build_line_fig(
        tod_df, height=280, x_field="hour",
        x_axis_args=dict(showgrid=False,
                         tickfont=dict(size=10, color=MUTED),
                         tickmode="array",
                         tickvals=list(range(0, 24, 3)),
                         ticktext=[f"{h:02d}:00" for h in range(0, 24, 3)],
                         title=dict(text="Hour of day (NEM time)",
                                    font=dict(size=10, color=MUTED))),
        hover_x="%{x:02d}:00",
    )

    ts_div  = f"plot-trans-ts-{int(datetime.now().timestamp() * 1000)}"
    tod_div = f"plot-trans-tod-{int(datetime.now().timestamp() * 1000)}-2"
    ts_json  = _plot_json(ts_fig)
    tod_json = _plot_json(tod_fig)
    rd = _region_display(region)
    ts_title = (f"{rd} transmission flows &middot; {range_label} "
                f"&middot; {res_label} buckets")
    tod_title = f"{rd} transmission by hour of day &middot; {range_label}"

    return (
        '<div class="prices-stack">'
        + '<div class="card">'
        + _card_h3(ts_title)
        + f'<div id="{ts_div}" style="height:300px"></div>'
        + f'<script>(function(){{var f={ts_json};'
          f'Plotly.newPlot("{ts_div}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        + f'Positive = import to {rd}, negative = export. One line per '
        + 'interconnector. Zero-crossings mark direction changes.</p>'
        + _attribution()
        + '</div>'
        + '<div class="card">'
        + _card_h3(tod_title)
        + f'<div id="{tod_div}" style="height:280px"></div>'
        + f'<script>(function(){{var f={tod_json};'
          f'Plotly.newPlot("{tod_div}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        + 'Mean flow per hour of day across the selected window.</p>'
        + _attribution()
        + '</div>'
        + '</div>'
    )


_trends_adapter_instance = None

# AEMO's five physical NEM regions. Used to filter out the sub-region IDs
# (QLDC/QLDN/QLDS/TASN/TASS) that appear in rooftop30 for 2020-2025 but
# vanish in 2026, double-counting their parents in the NEM aggregate.
NEM_PHYSICAL_REGIONS = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}


def _get_trends_adapter():
    """Module-level singleton that wraps production's PenetrationTab without
    re-running its auto-update loop. Holds the underlying cache between
    requests so chart regen is fast after the first call.

    Also fixes a production bug in NEM rooftop aggregation — see comment on
    the _load_rooftop_30min override below."""
    global _trends_adapter_instance
    if _trends_adapter_instance is not None:
        return _trends_adapter_instance
    from aemo_dashboard.penetration.penetration_tab import PenetrationTab

    class _Adapter(PenetrationTab):
        def _update_charts(self, event=None):
            # Suppress the auto-rebuild that __init__ + watchers trigger.
            # The caller invokes _create_* methods directly with the desired
            # region/fuel/smoothing values set first.
            pass

        def _load_rooftop_30min(self, start_date, end_date):
            """Filter out non-physical sub-region IDs before the parent's
            NEM aggregation pivots all columns.

            rooftop30 historically (2020-2025) contains both parent regionids
            (NSW1/QLD1/VIC1/SA1/TAS1) AND sub-region IDs (QLDC/QLDN/QLDS/TASN/
            TASS). Production's NEM sum is `df.sum(axis=1)` over all columns,
            which double-counts QLD and TAS in those years. Starting 2026,
            the sub-region IDs stop appearing, so the NEM 2026 series ends
            up correctly summed but looks lower than the over-counted 2025
            series.

            This override drops the sub-region columns so the NEM sum is
            consistent across all years."""
            df = super()._load_rooftop_30min(start_date, end_date)
            if not df.empty and "settlementdate" in df.columns:
                keep_cols = ["settlementdate"] + [
                    c for c in df.columns if c in NEM_PHYSICAL_REGIONS
                ]
                df = df[keep_cols]
            return df

    _trends_adapter_instance = _Adapter()
    return _trends_adapter_instance


def _generation_trends_content(region: str, fuel: str = "vre") -> str:
    """Renewable-penetration trend charts, reusing production's PenetrationTab.

    Three Plotly figures, stacked one per card. The fuel selector (VRE /
    Wind / Solar / Rooftop) affects which series the production chart code
    isolates. Region selector applies; smoothing is fixed at EWM 30 days.
    """
    try:
        adapter = _get_trends_adapter()
    except ImportError:
        return ('<div class="placeholder">'
                '<p><strong>Trends module unavailable.</strong></p></div>')
    except Exception as exc:
        return (f'<div class="placeholder">'
                f'<p><strong>Couldn\'t load Trends data.</strong></p>'
                f'<p>{exc}</p></div>')

    adapter.region_select.value = region
    adapter.fuel_select.value = TRENDS_FUEL_TO_PROD.get(fuel, "VRE")
    adapter.smoothing_select.value = "EWM (30 days, balanced)"

    def _render_chart(fig, slug, title):
        if fig is None:
            return ('<div class="card"><div class="placeholder">'
                    f'<p><strong>{title}: no data available.</strong></p>'
                    '</div></div>')
        # Strip the figure's own title — our card header carries it instead,
        # in the brand teal small-caps style used everywhere else.
        fig.update_layout(title=None, margin=dict(l=48, r=12, t=8, b=44))
        div_id = f"plot-{slug}-{int(datetime.now().timestamp() * 1000)}"
        fig_json = _plot_json(fig)
        return ('<div class="card">'
                + _card_h3(title)
                + f'<div id="{div_id}" style="height:420px"></div>'
                + f'<script>(function(){{var f={fig_json};'
                f'Plotly.newPlot("{div_id}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
                + _attribution()
                + '</div>')

    rd = _region_display(region)
    fuel_label = TRENDS_FUEL_TO_PROD.get(fuel, "VRE")
    try:
        vre_fig = adapter._create_vre_production_chart()
        by_fuel_fig = adapter._create_vre_by_fuel_chart()
        thermal_fig = adapter._create_thermal_vs_renewables_chart()
    except Exception as exc:
        return (f'<div class="placeholder">'
                f'<p><strong>Couldn\'t build Trends charts.</strong></p>'
                f'<p>{exc}</p></div>')

    return (
        '<div class="prices-stack">'
        + _render_chart(vre_fig, "vre-prod",
                         f"{rd} {fuel_label} production · annualised TWh, "
                         f"last 3 years overlaid")
        + _render_chart(by_fuel_fig, "vre-fuel",
                         f"{rd} renewable generation by fuel")
        + _render_chart(thermal_fig, "thermal-vs-renew",
                         f"{rd} thermal vs renewables share")
        + '<div class="card">'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        + 'All three charts use a fixed 30-day exponentially-weighted moving '
        + 'average (EWM) for smoothing — without it, daily fluctuations bury '
        + 'the multi-year trend. The Fuel selector affects the top chart\'s '
        + 'series selection: <b>VRE</b> aggregates Wind + Solar + Rooftop, the '
        + 'individual fuels isolate just that source. The other two charts '
        + 'don\'t change with the Fuel selector.'
        + '</p>'
        + '</div>'
        + '</div>'
    )


def _generation_yr_on_yr_content(region: str, range_slug: str,
                                 start: str | None, end: str | None) -> str:
    """Period-on-period fuel + price comparison.

    Reuses the iOS API helpers from
    `aemo_dashboard.api.routers.generation_comparison` verbatim — same
    annualised-TWh + generation-weighted VWAP semantics, same Battery-spread
    metric, same TWAP for the total row. Renders as an HTML table with
    sign-coloured deltas (Flexoki green/red)."""
    # The iOS endpoint only supports the named periods (7d/30d/ytd/1y).
    # 24h is handled directly here as a sub-day window vs same-day-last-year
    # — the comparison is mostly cosmetic at that scale but the summary
    # table is still useful. 1h is dropped from the pill set above; any
    # URL that still passes 1h gets treated as 24h.
    period_map = {
        "7d": "7d", "30d": "30d", "ytd": "ytd", "1y": "1y",
        "all": "1y", "custom": "ytd",
    }
    if range_slug in ("1h", "24h"):
        effective = "24h"
    else:
        effective = period_map.get(range_slug, "ytd")
    period_note = (f' &middot; range "{range_slug.upper()}" → '
                   f'"{effective.upper()}"'
                   if range_slug != effective and range_slug != "custom"
                   else "")

    try:
        from aemo_dashboard.api.routers.generation_comparison import (
            _resolve_window, _safe_year_minus_one, _period_days,
            _fetch_fuel_window, _fetch_twap, _aggregate_groups,
            GROUP_ORDER, GROUP_DISPLAY,
        )
    except ImportError:
        return ('<div class="placeholder">'
                'Year-on-year module unavailable.</div>')

    import duckdb
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        end_curr = conn.execute(
            "SELECT MAX(settlementdate) FROM scada30").fetchone()[0]
        if end_curr is None:
            return '<div class="placeholder">No data.</div>'
        if effective == "24h":
            # 24-hour window ending at the freshest scada30 timestamp.
            start_curr = end_curr - timedelta(hours=24)
        else:
            start_curr, end_curr = _resolve_window(effective, end_curr)
        end_prev   = _safe_year_minus_one(end_curr)
        start_prev = _safe_year_minus_one(start_curr)
        days_curr  = _period_days(start_curr, end_curr)
        days_prev  = _period_days(start_prev, end_prev)

        cur_raw = _fetch_fuel_window(conn, start_curr, end_curr, region)
        prv_raw = _fetch_fuel_window(conn, start_prev, end_prev, region)
        twap_curr = _fetch_twap(conn, start_curr, end_curr, region)
        twap_prev = _fetch_twap(conn, start_prev, end_prev, region)
    finally:
        conn.close()

    cur_groups = _aggregate_groups(cur_raw, days_curr)
    prv_groups = _aggregate_groups(prv_raw, days_prev)

    GREEN, RED = "#66800B", "#AF3029"

    def fmt_num(v, dec=1):
        if v is None: return "&mdash;"
        return f"{v:,.{dec}f}"

    def fmt_delta(v, dec=1):
        if v is None: return "&mdash;"
        sign = "+" if v > 0 else ("" if v == 0 else "")
        color = GREEN if v > 0 else (RED if v < 0 else MUTED)
        return f'<span style="color:{color};font-weight:600">{sign}{v:,.{dec}f}</span>'

    body_rows = []
    for gkey in GROUP_ORDER:
        c, p = cur_groups[gkey], prv_groups[gkey]
        if c["_mwh"] == 0 and p["_mwh"] == 0:
            continue
        d_twh = (c["twh"] - p["twh"]) if (c["twh"] is not None and p["twh"] is not None) else None
        d_vwap = (c["vwap"] - p["vwap"]) if (c["vwap"] is not None and p["vwap"] is not None) else None
        label = GROUP_DISPLAY[gkey]
        if c["is_spread"]:
            label += "*"
        body_rows.append(
            f'<tr style="border-bottom:0.5px solid {BORDER}">'
            f'<td style="padding:6px 12px;color:{INK};font-weight:500">{label}</td>'
            f'<td style="text-align:right;padding:6px 12px">{fmt_num(c["twh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">{fmt_num(p["twh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">{fmt_delta(d_twh)}</td>'
            f'<td style="text-align:right;padding:6px 12px">{fmt_num(c["vwap"], 0)}</td>'
            f'<td style="text-align:right;padding:6px 12px">{fmt_num(p["vwap"], 0)}</td>'
            f'<td style="text-align:right;padding:6px 12px">{fmt_delta(d_vwap, 0)}</td>'
            f'</tr>'
        )

    # Total row — TWh sums, demand-weighted TWAP for price
    total_twh_cur = sum((g["twh"] or 0.0) for g in cur_groups.values())
    total_twh_prv = sum((g["twh"] or 0.0) for g in prv_groups.values())
    d_total_twh = total_twh_cur - total_twh_prv
    d_twap = ((twap_curr - twap_prev)
              if (twap_curr is not None and twap_prev is not None) else None)
    total_label = "NEM total" if region == "NEM" else f"{_region_display(region)} total"
    body_rows.append(
        f'<tr style="border-top:1.5px solid {MUTED};border-bottom:0.5px solid {BORDER}">'
        f'<td style="padding:8px 12px;color:{INK};font-weight:600">{total_label}</td>'
        f'<td style="text-align:right;padding:8px 12px;font-weight:600">{fmt_num(total_twh_cur)}</td>'
        f'<td style="text-align:right;padding:8px 12px;font-weight:600">{fmt_num(total_twh_prv)}</td>'
        f'<td style="text-align:right;padding:8px 12px">{fmt_delta(d_total_twh)}</td>'
        f'<td style="text-align:right;padding:8px 12px;font-weight:600">{fmt_num(twap_curr, 0)}</td>'
        f'<td style="text-align:right;padding:8px 12px;font-weight:600">{fmt_num(twap_prev, 0)}</td>'
        f'<td style="text-align:right;padding:8px 12px">{fmt_delta(d_twap, 0)}</td>'
        f'</tr>'
    )

    head = (
        f'<tr style="border-bottom:1.5px solid {MUTED}">'
        f'<th style="text-align:left;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Fuel</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Current TWh</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Prior TWh</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Δ TWh</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Current $/MWh</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Prior $/MWh</th>'
        f'<th style="text-align:right;padding:8px 12px;font-weight:600;font-size:13px;color:{INK}">Δ $/MWh</th>'
        f'</tr>'
    )

    period_labels = {"24h": "Last 24h", "7d": "Last 7 days",
                     "30d": "Last 30 days", "ytd": "Year to date",
                     "1y": "Last 12 months"}
    period_label = period_labels.get(effective, effective)
    title = (f"{_region_display(region)} year on year &middot; "
             f"{period_label}{period_note}")

    cur_win = f"{start_curr:%-d %b %Y} → {end_curr:%-d %b %Y}"
    prv_win = f"{start_prev:%-d %b %Y} → {end_prev:%-d %b %Y}"

    return (
        '<div class="prices-stack"><div class="card">'
        + _card_h3(title)
        + '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        + f'<thead>{head}</thead><tbody>{"".join(body_rows)}</tbody></table>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;line-height:1.5">'
        + f'Current: <strong>{cur_win}</strong> &middot; Prior: <strong>{prv_win}</strong>. '
        + 'TWh annualised over each window. $/MWh per fuel is generation-weighted VWAP '
        + '(*Battery row shows discharge − charge spread). Total row $/MWh is '
        + 'demand-weighted TWAP across all intervals. Δ in green when positive, red when negative.'
        + '</p>'
        + _attribution()
        + '</div></div>'
    )


@app.get("/generation-mix", response_class=HTMLResponse)
def generation_mix_root(region: str = "", range: str = "",
                        start: str | None = None, end: str | None = None
                        ) -> RedirectResponse:
    """Lands the user on Yr-on-yr — most useful single-page view for an
    analyst doing period-on-period reasoning."""
    params = {}
    if region: params["region"] = region
    if range:  params["range"]  = range
    if start:  params["start"]  = start
    if end:    params["end"]    = end
    return RedirectResponse(url=_build_url("/generation-mix/yr-on-yr", **params))


@app.get("/trends", response_class=HTMLResponse)
def legacy_trends_redirect(region: str = "", range: str = "",
                           start: str | None = None, end: str | None = None
                           ) -> RedirectResponse:
    """Permanent redirect for the legacy /trends URL. Trends is now a
    subtab of Generation mix."""
    params = {}
    if region: params["region"] = region
    if range:  params["range"]  = range
    if start:  params["start"]  = start
    if end:    params["end"]    = end
    return RedirectResponse(
        url=_build_url("/generation-mix/trends", **params),
        status_code=308,  # permanent, preserves query params
    )


@app.get("/generation-mix/{sub}", response_class=HTMLResponse)
def generation_mix_sub(sub: str, request: Request,
                       region: str = "NEM",
                       range: str = "24h",
                       start: str | None = None,
                       end: str | None = None,
                       fuel: str = "vre") -> HTMLResponse:
    _, subtabs = TAB_LOOKUP["generation-mix"]
    sub_slugs = {s for s, _ in subtabs}
    if sub not in sub_slugs:
        return HTMLResponse(status_code=404, content="Not found")

    if region not in GENMIX_REGION_LIST:
        region = "NEM"
    valid_ranges = {slug for slug, _ in RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "24h"
    valid_fuels = {slug for slug, _ in TRENDS_FUEL_OPTIONS}
    if fuel not in valid_fuels:
        fuel = "vre"

    base_params = {"region": region, "range": range}
    if start: base_params["start"] = start
    if end:   base_params["end"] = end
    if fuel != "vre": base_params["fuel"] = fuel  # only carry if non-default

    base_url = f"/generation-mix/{sub}"
    # Trends uses Region + Fuel (no Range/Smooth — full multi-year history,
    # fixed 30-day EWM smoothing). All other subtabs use Region + Range.
    if sub == "trends":
        # Build params carrying fuel so region pills preserve it.
        trends_carry = {k: v for k, v in base_params.items()
                        if k not in ("region", "range", "start", "end")}
        if "fuel" not in trends_carry:
            trends_carry["fuel"] = fuel
        selectors = _render_selector_strip(
            _render_region_pills(base_url, region, trends_carry,
                                 regions=GENMIX_REGION_LIST),
            _render_fuel_pills(base_url, fuel,
                               {k: v for k, v in base_params.items()
                                if k not in ("fuel",)}),
        )
    else:
        # 1H is meaningless on the yr-on-yr subtab (annualised comparison
        # against the same hour last year is just noise). Hide that pill;
        # 24H stays and works as a real 24-hour window via the period
        # handling in _generation_yr_on_yr_content.
        range_options = ([o for o in RANGE_OPTIONS if o[0] != "1h"]
                         if sub == "yr-on-yr" else RANGE_OPTIONS)
        selectors = _render_selector_strip(
            _render_region_pills(base_url, region,
                                 {k: v for k, v in base_params.items() if k != "region"},
                                 regions=GENMIX_REGION_LIST),
            _render_range_pills(base_url, range,
                                {k: v for k, v in base_params.items()
                                 if k not in ("range", "start", "end")},
                                start=start or "", end=end or "",
                                options=range_options),
        )
    subtab_html = _render_subtab_nav("generation-mix", subtabs, sub,
                                      carry_params=base_params)

    sub_label = dict(subtabs)[sub]
    if sub == "stack":
        s_ts, e_ts, _, range_label = _range_window(range, start, end)
        stack_card = _generation_stack_content(region, range, start, end)
        price_chart_html = _generation_price_chart(
            region, range, s_ts, e_ts, range_label)
        stats, summary = _generation_fuel_stats(region, s_ts, e_ts)
        fuel_table_html = _render_fuel_table(stats, summary, range_label)
        content = (
            f'{stack_card}'  # stack returns its own prices-stack wrapper
            + f'<div class="prices-stack">'
            + (f'<div class="card">{price_chart_html}</div>' if price_chart_html else '')
            + f'<div class="card">{fuel_table_html}</div>'
            + f'</div>'
        )
    elif sub == "tod":
        content = _generation_tod_content(region, range, start, end)
    elif sub == "transmission":
        content = _generation_transmission_content(region, range, start, end)
    elif sub == "yr-on-yr":
        content = _generation_yr_on_yr_content(region, range, start, end)
    elif sub == "trends":
        content = _generation_trends_content(region, fuel)
    else:
        content = _placeholder("Generation mix", sub_label)

    body = _render_tab_body(subtab_html, selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /evening-peak — period-on-period 4-panel comparison (17:00–22:00)
# ----------------------------------------------------------------------------

EVENING_PEAK_REGION_LIST = ["NEM", "NSW1", "QLD1", "SA1", "TAS1", "VIC1"]


def _evening_peak_content(region: str, range_slug: str,
                           start: str | None, end: str | None) -> str:
    """4-panel comparison of evening peak (17:00–22:00) fuel mix and price
    vs the same calendar window one year ago. Uses production's
    `get_evening_data` helper so the numbers match the existing dashboard.
    """
    from datetime import date
    from aemo_dashboard.evening_peak.evening_analysis import (
        get_evening_data, get_latest_data_date,
        FUEL_ORDER as EP_FUEL_ORDER,
        FUEL_COLORS as EP_FUEL_COLORS,
    )
    from plotly.subplots import make_subplots

    end_date = get_latest_data_date()

    # Resolve period from the standard range pills. Anything <7d falls back
    # to 30 days because the comparison needs a meaningful window.
    if range_slug == "custom" and start and end:
        s_d = date.fromisoformat(start)
        e_d = date.fromisoformat(end)
        end_date = e_d
        period_days = max(7, (e_d - s_d).days + 1)
    elif range_slug == "ytd":
        period_days = (end_date - date(end_date.year, 1, 1)).days + 1
    else:
        period_days = {"7d": 7, "30d": 30, "1y": 365,
                       "all": 365, "1h": 30, "24h": 30}.get(range_slug, 30)

    end_excl = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = (end_date - timedelta(days=period_days - 1)).strftime("%Y-%m-%d")
    pcp_end = end_date - timedelta(days=365)
    pcp_end_excl = (pcp_end + timedelta(days=1)).strftime("%Y-%m-%d")
    pcp_start = (pcp_end - timedelta(days=period_days - 1)).strftime("%Y-%m-%d")

    try:
        ty_data, ty_prices, _ = get_evening_data(start_str, end_excl, region)
        ly_data, ly_prices, _ = get_evening_data(pcp_start, pcp_end_excl, region)
    except Exception as exc:
        return (f'<div class="placeholder">'
                f'<p><strong>Couldn\'t load evening peak data.</strong></p>'
                f'<p>{exc}</p></div>')

    if ty_data.empty or ly_data.empty:
        return ('<div class="placeholder">'
                f'<p><strong>No evening peak data for {_region_display(region)} '
                f'over {period_days} days.</strong></p></div>')

    # Common ordering and y-axis cap across the two area panels
    times = ty_data.index.tolist()
    max_total_mw = max(
        ty_data[[c for c in EP_FUEL_ORDER if c in ty_data.columns]].clip(lower=0).sum(axis=1).max(),
        ly_data[[c for c in EP_FUEL_ORDER if c in ly_data.columns]].clip(lower=0).sum(axis=1).max(),
    )
    y_cap_gw = (max_total_mw / 1000) * 1.05

    rd = _region_display(region)
    ty_label = f"{datetime.strptime(start_str, '%Y-%m-%d'):%-d %b %Y} → {end_date:%-d %b %Y}"
    ly_label = f"{datetime.strptime(pcp_start, '%Y-%m-%d'):%-d %b %Y} → {pcp_end:%-d %b %Y}"

    # Use a real middle-dot — &middot; is HTML and Plotly subplot titles render
    # as SVG; HTML entities pass through as literal text.
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            f"<b>{rd} this year</b> · {ty_label}",
            f"<b>{rd} prior year</b> · {ly_label}",
            "Demand-weighted price by time of day",
            "Average MW change by fuel (this year − prior)",
        ),
        horizontal_spacing=0.10, vertical_spacing=0.24,
        row_heights=[0.5, 0.5],
    )

    legend_shown: set[str] = set()

    def add_area(data_df, row, col, stack_id: str):
        """Stack fuels in a single subplot. Each subplot uses unique stackgroup
        names so Plotly doesn't try to stack across the two side-by-side panels."""
        x = list(range(len(times)))
        for fuel in EP_FUEL_ORDER:
            if fuel not in data_df.columns:
                continue
            values = data_df[fuel].reindex(times).fillna(0).values
            if np.allclose(values, 0):
                continue
            color = EP_FUEL_COLORS.get(fuel, MUTED)
            show = fuel not in legend_shown
            legend_shown.add(fuel)
            if fuel == "Net Imports":
                pos = np.maximum(values, 0)
                neg = np.minimum(values, 0)
                if pos.any():
                    fig.add_trace(go.Scatter(
                        x=x, y=pos / 1000, mode="lines",
                        stackgroup=f"{stack_id}-pos",
                        line=dict(width=0.3, color=color),
                        fillcolor=color,
                        name=fuel, legendgroup=fuel, showlegend=show,
                        hovertemplate=f"{fuel}: %{{customdata:.0f}} MW<extra></extra>",
                        customdata=pos,
                    ), row=row, col=col)
                if neg.any():
                    show_neg = (fuel not in legend_shown or not pos.any()) and show
                    fig.add_trace(go.Scatter(
                        x=x, y=neg / 1000, mode="lines",
                        stackgroup=f"{stack_id}-neg",
                        line=dict(width=0.3, color=color),
                        fillcolor=color,
                        name=fuel, legendgroup=fuel, showlegend=show_neg,
                        hovertemplate=f"{fuel}: %{{customdata:.0f}} MW<extra></extra>",
                        customdata=neg,
                    ), row=row, col=col)
            else:
                fig.add_trace(go.Scatter(
                    x=x, y=values / 1000, mode="lines",
                    stackgroup=f"{stack_id}-pos",
                    line=dict(width=0.3, color=color),
                    fillcolor=color,
                    name=fuel, legendgroup=fuel, showlegend=show,
                    hovertemplate=f"{fuel}: %{{customdata:.0f}} MW<extra></extra>",
                    customdata=values,
                ), row=row, col=col)

    add_area(ty_data, row=1, col=1, stack_id="ty")
    add_area(ly_data, row=1, col=2, stack_id="ly")

    # Price comparison — TY vs LY (both demand-weighted) + average dashed lines
    times_ly = ly_data.index.tolist()
    ty_price_vals = ty_prices.reindex(times).fillna(method="ffill").values
    ly_price_vals = ly_prices.reindex(times_ly).fillna(method="ffill").values
    ty_avg = float(np.nanmean(ty_price_vals)) if len(ty_price_vals) else 0
    ly_avg = float(np.nanmean(ly_price_vals)) if len(ly_price_vals) else 0
    fig.add_trace(go.Scatter(
        x=list(range(len(times))), y=ty_price_vals,
        mode="lines+markers", name=f"This year (avg ${ty_avg:.0f})",
        line=dict(color="#205EA6", width=2.5),
        marker=dict(size=6),
        showlegend=True, legendgroup="price-ty",
        hovertemplate="This year: $%{y:.0f}/MWh<extra></extra>",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=list(range(len(times_ly))), y=ly_price_vals,
        mode="lines+markers", name=f"Prior year (avg ${ly_avg:.0f})",
        line=dict(color="#A02F6F", width=2.5, dash="dash"),
        marker=dict(size=6),
        showlegend=True, legendgroup="price-ly",
        hovertemplate="Prior year: $%{y:.0f}/MWh<extra></extra>",
    ), row=2, col=1)
    # Average reference lines through the peak window — value-annotated below.
    # NB: the actual add_annotation calls happen AFTER the layout block,
    # because update_annotations(font=...) there clobbers every annotation's
    # font settings if we add them first.
    fig.add_hline(y=ty_avg, line=dict(color="#205EA6", width=1, dash="dot"),
                  opacity=0.7, row=2, col=1)
    fig.add_hline(y=ly_avg, line=dict(color="#A02F6F", width=1, dash="dot"),
                  opacity=0.7, row=2, col=1)

    # Waterfall — average MW change per fuel
    fuels_present = [f for f in EP_FUEL_ORDER
                     if f in ty_data.columns or f in ly_data.columns]
    fuel_labels = []
    fuel_deltas = []
    # NB: use distinct local names — earlier in this function `ty_avg`/`ly_avg`
    # hold the period-average *prices* used by the avg-price annotations below.
    # Shadowing them here caused the labels to display the last fuel's mean MW.
    for fuel in fuels_present:
        ty_fuel_mw = ty_data[fuel].mean() if fuel in ty_data.columns else 0
        ly_fuel_mw = ly_data[fuel].mean() if fuel in ly_data.columns else 0
        delta = ty_fuel_mw - ly_fuel_mw
        if abs(delta) < 1:  # < 1 MW change, suppress
            continue
        fuel_labels.append(fuel)
        fuel_deltas.append(delta)
    measures = ["relative"] * len(fuel_deltas) + ["total"]
    fuel_labels.append("Net change")
    fuel_deltas.append(sum(fuel_deltas))
    fig.add_trace(go.Waterfall(
        x=fuel_labels, y=fuel_deltas, measure=measures,
        increasing=dict(marker=dict(color="#66800B")),
        decreasing=dict(marker=dict(color="#AF3029")),
        totals=dict(marker=dict(color="#5E409D")),
        textposition="outside",
        text=[f"{int(round(v)):+,d}" for v in fuel_deltas],
        cliponaxis=False,
        hovertemplate="%{x}: %{y:+,.0f} MW<extra></extra>",
        showlegend=False,
    ), row=2, col=2)
    # Compute the running cumulative range so we can give the y-axis enough
    # headroom for the outside text labels. Without this the +X labels above
    # the tallest bars crowd the subplot title.
    running = 0.0
    cum_max = 0.0
    cum_min = 0.0
    for v in fuel_deltas[:-1]:        # exclude the explicit "total" entry
        running += v
        cum_max = max(cum_max, running)
        cum_min = min(cum_min, running)
    cum_max = max(cum_max, fuel_deltas[-1])
    cum_min = min(cum_min, fuel_deltas[-1])
    pad = max(abs(cum_max), abs(cum_min)) * 0.25
    wf_y_range = [cum_min - pad, cum_max + pad]

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=820, margin=dict(l=48, r=12, t=50, b=72),
        legend=dict(orientation="h", yanchor="top", y=-0.05,
                    xanchor="center", x=0.5, font=dict(size=10),
                    bgcolor=PAPER),
    )
    fig.update_annotations(font=dict(size=12, color=INK))

    # Format axes
    tick_positions = list(range(len(times)))
    tick_labels = times  # "17:00", "17:30", ..., "21:30"
    for r in (1, 2):
        if r == 1:
            for c in (1, 2):
                fig.update_xaxes(showgrid=False, tickfont=dict(size=10, color=MUTED),
                                 tickmode="array", tickvals=tick_positions, ticktext=tick_labels,
                                 row=r, col=c)
                fig.update_yaxes(showgrid=False, tickfont=dict(size=10, color=MUTED),
                                 title=dict(text="GW", font=dict(size=10, color=MUTED)),
                                 range=[0, y_cap_gw], row=r, col=c)
    fig.update_xaxes(showgrid=False, tickfont=dict(size=10, color=MUTED),
                     tickmode="array", tickvals=tick_positions, ticktext=tick_labels,
                     row=2, col=1)
    fig.update_yaxes(showgrid=False, tickfont=dict(size=10, color=MUTED),
                     title=dict(text="$/MWh", font=dict(size=10, color=MUTED)),
                     row=2, col=1)
    fig.update_xaxes(showgrid=False, tickfont=dict(size=10, color=MUTED),
                     row=2, col=2)
    fig.update_yaxes(showgrid=False, tickfont=dict(size=10, color=MUTED),
                     title=dict(text="MW change", font=dict(size=10, color=MUTED)),
                     zeroline=True, zerolinecolor=BORDER, zerolinewidth=1,
                     range=wf_y_range,
                     row=2, col=2)

    # Add the avg-price labels NOW (after update_annotations) so their custom
    # colours survive. Use data coords + row/col so Plotly resolves the right
    # subplot. Production's matplotlib version puts the label at the last
    # x-position with a small y offset so it sits ON the dashed reference line.
    last_x = len(times) - 1
    fig.add_annotation(
        text=f"This year avg ${ty_avg:.0f}",
        x=last_x, y=ty_avg,
        xanchor="right", yanchor="top",   # below the TY line
        font=dict(color="#205EA6", size=11, family=PLOTLY_FONT["family"]),
        bgcolor=PAPER, showarrow=False,
        row=2, col=1,
    )
    fig.add_annotation(
        text=f"Prior year avg ${ly_avg:.0f}",
        x=last_x, y=ly_avg,
        xanchor="right", yanchor="bottom",  # above the LY line
        font=dict(color="#A02F6F", size=11, family=PLOTLY_FONT["family"]),
        bgcolor=PAPER, showarrow=False,
        row=2, col=1,
    )

    title = f"Evening peak (17:00–22:00) · {rd} · {period_days} days"
    div_id = f"plot-ep-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)

    # ---- Second card: 100% stacked bar — one bar per period ---------------
    # Aggregate the whole peak window into a single number per fuel per period,
    # then express as % of positive-only total. Two bars side-by-side gives a
    # cleaner at-a-glance comparison than two stacked-area charts.
    def _fuel_pct(data_df):
        pos = {f: float(data_df[f].clip(lower=0).mean())
               for f in EP_FUEL_ORDER if f in data_df.columns}
        tot = sum(pos.values())
        return {f: (v / tot * 100 if tot > 0 else 0) for f, v in pos.items()}

    ty_pct = _fuel_pct(ty_data)
    ly_pct = _fuel_pct(ly_data)
    pct_categories = ["This year", "Prior year"]

    pct_fig = go.Figure()
    for fuel in EP_FUEL_ORDER:
        ty_v = ty_pct.get(fuel, 0)
        ly_v = ly_pct.get(fuel, 0)
        if ty_v == 0 and ly_v == 0:
            continue
        color = EP_FUEL_COLORS.get(fuel, MUTED)
        # Label segment with % when it's wide enough to fit; otherwise leave blank
        # and let the hover/legend identify it.
        pct_fig.add_trace(go.Bar(
            x=pct_categories, y=[ty_v, ly_v],
            name=fuel,
            marker=dict(color=color),
            text=[(f"{ty_v:.0f}%" if ty_v >= 3 else ""),
                  (f"{ly_v:.0f}%" if ly_v >= 3 else "")],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=11),
            hovertemplate=f"{fuel}: %{{y:.1f}}%<extra></extra>",
        ))

    pct_fig.update_layout(
        barmode="stack",
        bargap=0.55,
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=400, margin=dict(l=48, r=12, t=12, b=44),
        legend=dict(orientation="h", yanchor="top", y=-0.06,
                    xanchor="center", x=0.5, font=dict(size=10),
                    bgcolor=PAPER),
        xaxis=dict(showgrid=False, tickfont=dict(size=13, color=INK)),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   range=[0, 100], ticksuffix="%",
                   title=dict(text="% of generation",
                              font=dict(size=10, color=MUTED))),
    )
    pct_div = f"plot-eppct-{int(datetime.now().timestamp() * 1000)}-2"
    pct_json = _plot_json(pct_fig)

    return (
        '<div class="prices-stack">'
        + '<div class="card">'
        + _card_h3(title)
        + f'<div id="{div_id}" style="height:820px"></div>'
        + f'<script>(function(){{var f={fig_json};'
        f'Plotly.newPlot("{div_id}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        + 'Top: average MW per fuel for each 30-min interval of the evening peak window. '
        + 'Bottom left: demand-weighted price by time of day (this year vs the same window '
        + 'one year ago, with dashed average-reference lines). Bottom right: change in '
        + 'average MW per fuel (positive = more this year). All averages over the selected '
        + 'number of days.'
        + '</p>'
        + _attribution()
        + '</div>'
        + '<div class="card">'
        + _card_h3(f"% composition · this year vs prior year")
        + f'<div id="{pct_div}" style="height:400px"></div>'
        + f'<script>(function(){{var f={pct_json};'
        f'Plotly.newPlot("{pct_div}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        + 'Each bar sums to 100% of positive generation across the whole evening peak '
        + 'window. Segments labelled in-place when wide enough; hover for exact values.'
        + '</p>'
        + _attribution()
        + '</div>'
        + '</div>'
    )


@app.get("/evening-peak", response_class=HTMLResponse)
def evening_peak_page(request: Request,
                      region: str = "NEM",
                      range: str = "30d",
                      start: str | None = None,
                      end: str | None = None) -> HTMLResponse:
    if region not in EVENING_PEAK_REGION_LIST:
        region = "NEM"
    valid_ranges = {slug for slug, _ in RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "30d"

    base_params = {"region": region, "range": range}
    if start: base_params["start"] = start
    if end:   base_params["end"] = end
    base_url = "/evening-peak"
    selectors = _render_selector_strip(
        _render_region_pills(base_url, region,
                             {k: v for k, v in base_params.items() if k != "region"},
                             regions=EVENING_PEAK_REGION_LIST),
        _render_range_pills(base_url, range,
                            {k: v for k, v in base_params.items()
                             if k not in ("range", "start", "end")},
                            start=start or "", end=end or ""),
    )
    content = _evening_peak_content(region, range, start, end)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /gas — STTM ex-post price + volume (single page, three stacked cards)
# ----------------------------------------------------------------------------
#
# Data lives in sttm_expost (gas_date, hub in {SYD,BRI,ADL}, expost_price,
# network_allocation). Volumes (network_allocation) are GJ/day → /1000 = TJ.
# We synthesise an "AVG" pseudo-hub = mean(SYD,BRI,ADL) per day for price.
# Volume charts always use the physical hubs (AVG isn't a real meter).
#
# The price chart respects the Range pills. The two demand charts are
# day-of-year overlays of the last 3 calendar years and intentionally ignore
# Range — the comparison only makes sense on a full annual cycle.

GAS_PRICE_CLIP = 100.0  # $/GJ — clip display only; raw price stays in hover


def _load_gas_prices() -> pd.DataFrame:
    """All STTM ex-post prices + computed STTM Avg row per day."""
    df = q("""SELECT gas_date, hub, expost_price AS price
              FROM sttm_expost
              ORDER BY gas_date""")
    if df.empty:
        return df
    df["gas_date"] = pd.to_datetime(df["gas_date"])
    pivot = df.pivot(index="gas_date", columns="hub", values="price")
    avg = pivot[["SYD", "ADL", "BRI"]].mean(axis=1)
    avg_df = pd.DataFrame({"gas_date": avg.index, "hub": "AVG",
                           "price": avg.values})
    return (pd.concat([df, avg_df], ignore_index=True)
              .sort_values("gas_date").reset_index(drop=True))


def _load_gas_volumes() -> pd.DataFrame:
    """Daily TJ per hub (only days with non-null network_allocation)."""
    df = q("""SELECT gas_date, hub, network_allocation
              FROM sttm_expost
              WHERE network_allocation IS NOT NULL
              ORDER BY gas_date""")
    if df.empty:
        return df
    df["gas_date"] = pd.to_datetime(df["gas_date"])
    df["demand_tj"] = df["network_allocation"] / 1000.0
    return df


# Year-overlay styling: current year = solid + heaviest, prior = dashed,
# two-back = dotted + lightest. Matches the production sttm volume tab.
_YEAR_WIDTHS = {0: 2.0, 1: 1.5, 2: 1.0}
_YEAR_DASH = {0: "solid", 1: "dash", 2: "dot"}


def _build_gas_price_fig(prices: pd.DataFrame, hubs: list[str],
                        start: pd.Timestamp, end: pd.Timestamp,
                        range_label: str) -> go.Figure:
    fig = go.Figure()
    if prices.empty:
        fig.add_annotation(text="No STTM price data available",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False,
                           font=dict(size=14, color=MUTED))
    else:
        win = prices[(prices["gas_date"] >= start)
                     & (prices["gas_date"] <= end)].copy()
        win["price_display"] = win["price"].clip(upper=GAS_PRICE_CLIP)
        hub_label = dict(GAS_HUBS)
        # Plot in the canonical hub order so legend ordering is stable.
        for code, label in GAS_HUBS:
            if code not in hubs:
                continue
            sub = win[win["hub"] == code]
            if sub.empty:
                continue
            fig.add_trace(go.Scatter(
                x=sub["gas_date"], y=sub["price_display"],
                mode="lines",
                line=dict(color=GAS_HUB_COLORS[code],
                          width=1.5 if code != "AVG" else 1.2,
                          dash="solid" if code != "AVG" else "dot"),
                name=hub_label[code],
                customdata=sub["price"],
                hovertemplate=(f"{label}<br>%{{x|%d %b %Y}}<br>"
                               "$%{customdata:.2f}/GJ<extra></extra>"),
            ))

        # Stats annotation in top-left, scoped to selected hubs + window.
        stats_lines = []
        for code, label in GAS_HUBS:
            if code not in hubs:
                continue
            sub = win[win["hub"] == code]
            if sub.empty:
                continue
            latest = sub.iloc[-1]
            mean_p = sub["price"].mean()
            stats_lines.append(
                f"<b>{label}</b>: ${latest['price']:.2f} "
                f"(latest) &middot; ${mean_p:.2f} (period avg)"
            )
        if stats_lines:
            fig.add_annotation(
                text="<br>".join(stats_lines),
                xref="paper", yref="paper",
                x=0.01, y=0.99, xanchor="left", yanchor="top",
                showarrow=False,
                font=dict(size=11, color=INK),
                bgcolor=PAPER, bordercolor=BORDER, borderwidth=1,
                opacity=0.92,
            )
        if (win["price"] > GAS_PRICE_CLIP).any():
            fig.add_annotation(
                text=("Display capped at $100/GJ &mdash; hover for raw price"),
                xref="paper", yref="paper",
                x=0.99, y=0.99, xanchor="right", yanchor="top",
                showarrow=False,
                font=dict(size=10, color=MUTED),
            )

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=460,
        margin=dict(l=52, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        xaxis=dict(gridcolor=BORDER, gridwidth=0.5, showspikes=False),
        yaxis=dict(title=dict(text="$/GJ", font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    return fig


def _build_gas_total_demand_fig(vol_df: pd.DataFrame) -> go.Figure:
    """Total STTM demand summed across SYD+BRI+ADL, last 3 years overlaid on
    day-of-year. 7-day rolling mean. Range pills do not apply."""
    fig = go.Figure()
    if vol_df.empty:
        fig.add_annotation(text="No STTM volume data available",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False,
                           font=dict(size=14, color=MUTED))
    else:
        daily = vol_df.groupby("gas_date")["demand_tj"].sum().reset_index()
        daily = daily.sort_values("gas_date")
        daily["ma7"] = daily["demand_tj"].rolling(7, min_periods=1).mean()
        daily["year"] = daily["gas_date"].dt.year
        daily["doy"] = daily["gas_date"].dt.dayofyear
        cur_yr = int(daily["year"].max())
        years = sorted(daily["year"].unique())[-3:]
        palette = {0: "#205EA6", 1: "#BC5215", 2: "#24837B"}
        for yr in years:
            age = cur_yr - yr
            sub = daily[daily["year"] == yr]
            fig.add_trace(go.Scatter(
                x=sub["doy"], y=sub["ma7"], mode="lines",
                line=dict(color=palette.get(age, MUTED),
                          width=_YEAR_WIDTHS.get(age, 1.0),
                          dash=_YEAR_DASH.get(age, "solid")),
                name=str(yr),
                hovertemplate=(f"{yr}<br>Day %{{x}}<br>"
                               "%{y:.1f} TJ/day<extra></extra>"),
            ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=380,
        margin=dict(l=52, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.20,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        xaxis=dict(title=dict(text="Day of year",
                              font=dict(size=11, color=MUTED)),
                   range=[1, 366], dtick=30,
                   gridcolor=BORDER, gridwidth=0.5, showspikes=False),
        yaxis=dict(title=dict(text="TJ/day (7-day MA)",
                              font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    return fig


def _build_gas_hub_demand_fig(vol_df: pd.DataFrame,
                              physical_hubs: list[str]) -> go.Figure:
    """Per-hub demand, last 3 years overlaid on day-of-year. 7-day rolling
    mean. Trace colour = hub, dash style = year age."""
    fig = go.Figure()
    if vol_df.empty or not physical_hubs:
        fig.add_annotation(text="No STTM volume data for the selected hubs",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False,
                           font=dict(size=14, color=MUTED))
    else:
        v = vol_df[vol_df["hub"].isin(physical_hubs)].copy()
        v = v.sort_values(["hub", "gas_date"])
        v["ma7"] = v.groupby("hub")["demand_tj"].transform(
            lambda s: s.rolling(7, min_periods=1).mean()
        )
        v["year"] = v["gas_date"].dt.year
        v["doy"] = v["gas_date"].dt.dayofyear
        cur_yr = int(v["year"].max())
        years = sorted(v["year"].unique())[-3:]
        hub_label = dict(GAS_HUBS)
        for code in physical_hubs:
            hub_v = v[v["hub"] == code]
            if hub_v.empty:
                continue
            for yr in years:
                sub = hub_v[hub_v["year"] == yr]
                if sub.empty:
                    continue
                age = cur_yr - yr
                fig.add_trace(go.Scatter(
                    x=sub["doy"], y=sub["ma7"], mode="lines",
                    line=dict(color=GAS_HUB_COLORS[code],
                              width=_YEAR_WIDTHS.get(age, 1.0),
                              dash=_YEAR_DASH.get(age, "solid")),
                    name=f"{hub_label[code]} {yr}",
                    hovertemplate=(f"{hub_label[code]} {yr}<br>"
                                   "Day %{x}<br>%{y:.1f} TJ/day<extra></extra>"),
                ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=420,
        margin=dict(l=52, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.22,
                    xanchor="center", x=0.5, font=dict(size=10),
                    bgcolor=PAPER),
        xaxis=dict(title=dict(text="Day of year",
                              font=dict(size=11, color=MUTED)),
                   range=[1, 366], dtick=30,
                   gridcolor=BORDER, gridwidth=0.5, showspikes=False),
        yaxis=dict(title=dict(text="TJ/day (7-day MA)",
                              font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    return fig


def _gas_range_window(range_slug: str,
                      prices: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    """Resolve a Range pill to (start, end, label) using the data's own max
    date — NOT today — so the window aligns with the last published gas_date."""
    days = GAS_RANGE_DAYS.get(range_slug, GAS_RANGE_DAYS["1y"])
    if prices.empty:
        today = pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None).date())
        return today - timedelta(days=365), today, "1 year"
    end = prices["gas_date"].max()
    if days is None:
        start = prices["gas_date"].min()
        label = "all data"
    else:
        start = end - timedelta(days=days)
        label = dict(GAS_RANGE_OPTIONS).get(range_slug, "1Y").lower()
        label = {"3m": "3 months", "6m": "6 months", "1y": "1 year",
                 "2y": "2 years", "5y": "5 years"}.get(range_slug, label)
    return start, end, label


def _gas_content(hubs: list[str], range_slug: str) -> str:
    prices = _load_gas_prices()
    vols = _load_gas_volumes()
    start, end, range_label = _gas_range_window(range_slug, prices)

    # Price card
    price_fig = _build_gas_price_fig(prices, hubs, start, end, range_label)
    price_id = f"plot-gas-price-{int(datetime.now().timestamp() * 1000)}"
    price_json = _plot_json(price_fig)
    price_html = (
        _card_h3(f"STTM ex-post gas price &middot; {range_label}")
        + f'<div id="{price_id}" style="height:460px"></div>'
        + f'<script>(function(){{var f={price_json};'
          f'Plotly.newPlot("{price_id}",f.data,f.layout,'
          f'{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
          f'line-height:1.5">Daily ex-post prices ($/GJ) for the AEMO Short '
          f'Term Trading Market hubs. STTM Avg is the unweighted mean of '
          f'SYD, BRI, ADL.</p>'
        + _attribution("AEMO STTM")
    )

    # Volume cards — physical hubs only (AVG is synthetic, no meter).
    physical = [c for c in hubs if c in ("SYD", "BRI", "ADL")]
    if not physical:
        physical = ["SYD", "BRI", "ADL"]

    total_fig = _build_gas_total_demand_fig(vols)
    total_id = f"plot-gas-total-{int(datetime.now().timestamp() * 1000)}"
    total_json = _plot_json(total_fig)
    total_html = (
        _card_h3("Total STTM gas demand &middot; last 3 years")
        + f'<div id="{total_id}" style="height:380px"></div>'
        + f'<script>(function(){{var f={total_json};'
          f'Plotly.newPlot("{total_id}",f.data,f.layout,'
          f'{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
          f'line-height:1.5">Sum across all three hubs, 7-day rolling mean. '
          f'Years overlaid on day-of-year so seasonal shape is comparable.</p>'
        + _attribution("AEMO STTM")
    )

    hub_fig = _build_gas_hub_demand_fig(vols, physical)
    hub_id = f"plot-gas-hub-{int(datetime.now().timestamp() * 1000)}"
    hub_json = _plot_json(hub_fig)
    hub_html = (
        _card_h3("STTM gas demand by hub &middot; last 3 years")
        + f'<div id="{hub_id}" style="height:420px"></div>'
        + f'<script>(function(){{var f={hub_json};'
          f'Plotly.newPlot("{hub_id}",f.data,f.layout,'
          f'{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
          f'line-height:1.5">Hub selection above filters this chart '
          f'(physical hubs only). Colour = hub, line weight/dash = year age.</p>'
        + _attribution("AEMO STTM")
    )

    return (f'<div class="prices-stack">'
            f'<div class="card">{price_html}</div>'
            f'<div class="card">{total_html}</div>'
            f'<div class="card">{hub_html}</div>'
            f'</div>')


@app.get("/gas", response_class=HTMLResponse)
def gas_page(request: Request,
             hub: str = "SYD,BRI,ADL,AVG",
             range: str = "1y") -> HTMLResponse:
    hub_codes = [c for c, _ in GAS_HUBS]
    hubs = [h for h in (hub or "").split(",") if h in hub_codes]
    if not hubs:
        hubs = hub_codes
    hub_param = ",".join(c for c in hub_codes if c in hubs)

    valid_ranges = {slug for slug, _ in GAS_RANGE_OPTIONS}
    if range not in valid_ranges:
        range = "1y"

    base_params = {"hub": hub_param, "range": range}
    base_url = "/gas"
    selectors = _render_selector_strip(
        _render_gas_hub_pills(base_url, hub_param,
                              {k: v for k, v in base_params.items()
                               if k != "hub"}),
        _render_gas_range_pills(base_url, range,
                                {k: v for k, v in base_params.items()
                                 if k != "range"}),
    )
    content = _gas_content(hubs, range)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /generators — multi-level grouped Tabulator over DUID aggregates
# ----------------------------------------------------------------------------
#
# Query goes always to 30-min data, joins generation_30min × prices_30min ×
# duid_mapping. Per-DUID aggregates roll up into a nested tree where every
# parent row carries its own totals (so collapsed groups don't go blank).
#
# Discharge-only convention for batteries: SUM(GREATEST(scadavalue, 0))
# everywhere. For non-battery fuels scadavalue is already ≥0 so the floor
# is a no-op, but it keeps the pricing math sane for batteries.
#
# Effective capacity is inline: storage_mwh/24 for batteries, capacity_mw
# otherwise. Aggregated up through the tree by summing.

PIVOT_RANGE_DEFS = {
    "7d":  (timedelta(days=7),    "last 7 days"),
    "30d": (timedelta(days=30),   "last 30 days"),
    "90d": (timedelta(days=90),   "last 90 days"),
    "1y":  (timedelta(days=365),  "last 12 months"),
    "5y":  (timedelta(days=1825), "last 5 years"),
}


def _pivot_range_window(range_slug: str, start: str | None = None,
                        end: str | None = None
                        ) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    """Resolve (start, end, label). Uses the data's max ts as the end for
    presets so the window aligns with the freshest aggregate; honours
    explicit start/end for range=custom."""
    end_row = q("SELECT MAX(settlementdate) AS ts FROM generation_30min")
    if end_row.empty or end_row["ts"].iloc[0] is None:
        end_ts = pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None))
    else:
        end_ts = pd.Timestamp(end_row["ts"].iloc[0])

    if range_slug in PIVOT_RANGE_DEFS:
        delta, label = PIVOT_RANGE_DEFS[range_slug]
        return end_ts - delta, end_ts, label
    if range_slug == "ytd":
        return (pd.Timestamp(year=end_ts.year, month=1, day=1), end_ts,
                "year to date")
    if range_slug == "custom" and start and end:
        s = pd.Timestamp(start)
        e = pd.Timestamp(end) + timedelta(days=1)
        return s, e, f"{start} → {end}"
    if range_slug == "custom":
        # Custom selected but no dates yet — default to last 30 days
        return end_ts - timedelta(days=30), end_ts, "last 30 days"
    return end_ts - timedelta(days=365), end_ts, "last 12 months"


def _pivot_query(s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                 regions: list[str], fuels_raw: list[str]) -> pd.DataFrame:
    """One DUID-level aggregate row over the window. Filters happen in SQL
    so we never bring more than the window's worth of pre-filtered DUIDs
    back. Discharge-only for batteries (GREATEST(scada, 0))."""
    region_in = ", ".join(f"'{r}'" for r in regions)
    fuel_in = ", ".join(f"'{f}'" for f in fuels_raw)
    sql = f"""
      SELECT
        COALESCE(NULLIF(d.region, ''), 'Unknown')        AS region,
        COALESCE(d.fuel, 'Unknown')                       AS fuel_raw,
        COALESCE(NULLIF(d.owner, ''), 'Unknown')          AS owner,
        COALESCE(NULLIF(d."site name", ''), g.duid)       AS station_name,
        g.duid                                            AS duid,
        FIRST(d.capacity_mw)                              AS capacity_mw,
        FIRST(d.storage_mwh)                              AS storage_mwh,
        SUM(GREATEST(g.scadavalue, 0)) * 0.5              AS gen_mwh,
        SUM(GREATEST(g.scadavalue, 0) * COALESCE(p.rrp, 0) * 0.5) AS revenue,
        COUNT(*)                                          AS n_intervals
      FROM generation_30min g
      LEFT JOIN duid_mapping d ON g.duid = d.duid
      LEFT JOIN prices_30min p
        ON g.settlementdate = p.settlementdate
        AND d.region = p.regionid
      WHERE g.settlementdate >= ? AND g.settlementdate < ?
        AND COALESCE(NULLIF(d.region, ''), 'Unknown') IN ({region_in})
        AND COALESCE(d.fuel, 'Unknown') IN ({fuel_in})
      GROUP BY d.region, d.fuel, d.owner, d."site name", g.duid
    """
    df = q(sql, [s_ts, e_ts])
    if df.empty:
        return df

    # Display fuel (collapse CCGT/OCGT/Gas other → Gas, Water → Hydro).
    raw_to_display = {raw: disp for disp, raws in PIVOT_FUEL_TO_RAW.items()
                      for raw in raws}
    df["fuel"] = df["fuel_raw"].map(lambda f: raw_to_display.get(f, "Other"))

    # Effective capacity: batteries = storage_mwh/24, else capacity_mw.
    is_battery = df["fuel"] == "Battery Storage"
    df["effective_cap_mw"] = np.where(
        is_battery & df["storage_mwh"].notna(),
        df["storage_mwh"] / 24.0,
        df["capacity_mw"].fillna(0),
    )
    df["capacity_mw"] = df["capacity_mw"].fillna(0)
    return df


# ── Tree builder ────────────────────────────────────────────────────────────
# Build nested rows: groups → station → DUID. At every level we carry
# generation, revenue, capacity totals so a collapsed parent still shows
# numbers. Price and utilisation are derived AT the parent level from the
# pooled sums (not averaged from children).

def _agg_node(rows: pd.DataFrame, hours: float, label: str,
              kind: str, ctx: dict) -> dict:
    """Aggregate one set of leaf rows into a single tree node payload.
    `kind` is 'group' / 'station' / 'duid' so the front end can style
    parents vs leaves. `ctx` carries the filter context for click handlers."""
    gen_mwh = float(rows["gen_mwh"].sum())
    revenue = float(rows["revenue"].sum())
    cap_mw = float(rows["effective_cap_mw"].sum()) if kind != "duid" \
             else float(rows["effective_cap_mw"].iloc[0])
    price = (revenue / gen_mwh) if gen_mwh > 0 else None
    util_denom = cap_mw * hours
    util_pct = (gen_mwh / util_denom * 100) if util_denom > 0 else None
    n_duids = int(rows["duid"].nunique()) if kind != "duid" else 1
    return {
        "label":   label,
        "kind":    kind,
        "ctx":     ctx,
        "gen_gwh": round(gen_mwh / 1000.0),
        "rev_m":   round(revenue / 1_000_000.0),
        "price":   round(price) if price is not None else None,
        "util":    round(util_pct) if util_pct is not None else None,
        "cap_mw":  round(cap_mw),
        "n_items": n_duids,
    }


def _build_pivot_tree(df: pd.DataFrame, group_dims: list[str],
                      hours: float) -> list[dict]:
    """Recursive group → station → DUID tree. `group_dims` is the ordered
    list of chosen group columns (subset of region/fuel/owner)."""
    if df.empty:
        return []
    # Always station as the level above DUID.
    def recurse(sub: pd.DataFrame, remaining: list[str], ctx: dict,
                depth: int) -> list[dict]:
        if not remaining:
            # Group by station, then DUID under each station.
            station_nodes = []
            for station, srows in sub.groupby("station_name", sort=False):
                station_ctx = dict(ctx, station=station)
                duid_children = []
                for _, drow in srows.sort_values("gen_mwh",
                                                 ascending=False).iterrows():
                    duid_ctx = dict(station_ctx, duid=drow["duid"])
                    one = pd.DataFrame([drow])
                    duid_children.append(_agg_node(
                        one, hours, drow["duid"], "duid", duid_ctx))
                node = _agg_node(srows, hours, station, "station", station_ctx)
                node["_children"] = duid_children
                station_nodes.append(node)
            station_nodes.sort(key=lambda n: n["gen_gwh"], reverse=True)
            return station_nodes

        dim = remaining[0]
        rest = remaining[1:]
        nodes = []
        # Use display-fuel name for the fuel dim, otherwise the raw value.
        for key, grp in sub.groupby(dim, sort=False):
            sub_ctx = dict(ctx, **{dim: key})
            children = recurse(grp, rest, sub_ctx, depth + 1)
            node = _agg_node(grp, hours, str(key), "group", sub_ctx)
            node["_children"] = children
            nodes.append(node)
        nodes.sort(key=lambda n: n["gen_gwh"], reverse=True)
        return nodes

    return recurse(df, group_dims, {}, 0)


# ── Content + route ─────────────────────────────────────────────────────────

def _pivot_content(g_dims: list[str], cols: list[str], range_slug: str,
                   regions: list[str], fuels: list[str],
                   start: str | None = None, end: str | None = None) -> str:
    s_ts, e_ts, range_label = _pivot_range_window(range_slug, start, end)
    hours = max((e_ts - s_ts).total_seconds() / 3600.0, 1e-9)

    fuels_raw = []
    for disp in fuels:
        fuels_raw.extend(PIVOT_FUEL_TO_RAW.get(disp, []))
    if not fuels_raw:
        # Empty fuel filter ⇒ no data; bail with a friendly card.
        return ('<div class="prices-stack"><div class="card">'
                + _card_h3("Generators")
                + '<p style="padding:14px;color:#878580">'
                  'No fuels selected.</p>'
                + '</div></div>')

    df = _pivot_query(s_ts, e_ts, regions, fuels_raw)
    if df.empty:
        return ('<div class="prices-stack"><div class="card">'
                + _card_h3(f"Generators &middot; {range_label}")
                + '<p style="padding:14px;color:#878580">'
                  f'No generation data for the selected window '
                  f'({s_ts:%d %b %Y} → {e_ts:%d %b %Y}).</p>'
                + '</div></div>')

    tree = _build_pivot_tree(df, g_dims, hours)

    title_chain = " → ".join(
        dict(PIVOT_DIMS)[d] for d in g_dims) or "—"
    n_duids = df["duid"].nunique()
    n_stations = df["station_name"].nunique()
    subtitle = (f"{title_chain} → Station → DUID &middot; "
                f"{n_stations:,} stations / {n_duids:,} DUIDs &middot; "
                f"{range_label} ({s_ts:%d %b %Y} → {e_ts:%d %b %Y})")

    # The shell's PIVOT_TABULATOR_JS owns the construction (timed off
    # htmx:afterSettle so layout is done). We just stash the data + the
    # column-toggle selection as globals and emit the container shells;
    # the shell handler picks them up.
    tree_json = json.dumps(tree, default=lambda v: None)
    cols_json = json.dumps(cols)

    # Carry the current pivot range/start/end to the cell formatter so the
    # anchors point at /station-analysis with the same window — the user
    # doesn't lose their place when they drill in.
    range_q = json.dumps(range_slug)
    start_q = json.dumps(start or "")
    end_q   = json.dumps(end or "")

    table_html = f"""
<div id="pivot-tabulator" style="margin-top:8px"></div>
<script>
  window._pivotData = {tree_json};
  window._pivotCols = {cols_json};
  window._pivotRange = {range_q};
  window._pivotStart = {start_q};
  window._pivotEnd   = {end_q};
  if (typeof window._buildPivotTable === "function") {{
    requestAnimationFrame(window._buildPivotTable);
  }}
</script>
"""

    return ('<div class="prices-stack">'
            '<div class="card">'
            + _card_h3(f"Generators &middot; {range_label}")
            + f'<p style="color:{MUTED};font-size:12px;margin:0 0 8px 14px">'
              f'{subtitle}</p>'
            + table_html
            + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
              f'line-height:1.5">Click any name (underlined on hover) to '
              f'open its charts and stats. Group rows open as fleet '
              f'selections; multi-region fleets use a demand-weighted '
              f'price reference.</p>'
            + _attribution()
            + '</div>'
            + '</div>')


@app.get("/generators", response_class=HTMLResponse)
def generators_page(request: Request,
               g1: str = "fuel", g2: str = "region", g3: str = "",
               cols: str = "gwh,rev,price,util,cap",
               range: str = "1y",
               start: str | None = None, end: str | None = None,
               region: str = "NSW1,QLD1,SA1,TAS1,VIC1",
               fuel: str = "Coal,Gas,Hydro,Wind,Solar,Battery Storage,"
                            "Biomass,Other",
               ) -> HTMLResponse:
    valid_dims = {slug for slug, _ in PIVOT_DIMS}

    # Pick ordered group dims, dropping empties + de-duping silently.
    g_dims: list[str] = []
    for slot in (g1, g2, g3):
        if slot in valid_dims and slot not in g_dims:
            g_dims.append(slot)
    if not g_dims:
        g_dims = ["fuel"]

    valid_cols = {slug for slug, _, _ in PIVOT_COLUMNS}
    selected_cols = [c for c in (cols or "").split(",") if c in valid_cols]
    if not selected_cols:
        selected_cols = ["gwh", "rev", "price"]

    valid_ranges = {slug for slug, _ in PIVOT_RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "1y"

    region_list = [r for r in (region or "").split(",") if r in REGION_ORDER]
    if not region_list:
        region_list = REGION_ORDER[:]
    region_param = ",".join(r for r in REGION_ORDER if r in region_list)

    valid_fuels = {code for code, _ in PIVOT_FUEL_GROUPS}
    fuel_list = [f for f in (fuel or "").split(",") if f in valid_fuels]
    if not fuel_list:
        fuel_list = [code for code, _ in PIVOT_FUEL_GROUPS]
    fuel_param = ",".join(code for code, _ in PIVOT_FUEL_GROUPS
                          if code in fuel_list)

    base_params = {
        "g1": g_dims[0] if len(g_dims) > 0 else "",
        "g2": g_dims[1] if len(g_dims) > 1 else "",
        "g3": g_dims[2] if len(g_dims) > 2 else "",
        "cols": ",".join(selected_cols),
        "range": range,
        "region": region_param,
        "fuel": fuel_param,
    }
    if start: base_params["start"] = start
    if end:   base_params["end"] = end
    base_url = "/generators"

    def _other(*excl: str) -> dict:
        return {k: v for k, v in base_params.items() if k not in excl}

    selectors = _render_selector_strip(
        _render_pivot_group_pills(base_url, 1,
                                   g_dims[0] if len(g_dims) > 0 else None,
                                   set(),
                                   _other("g1")),
        _render_pivot_group_pills(base_url, 2,
                                   g_dims[1] if len(g_dims) > 1 else None,
                                   {g_dims[0]} if g_dims else set(),
                                   _other("g2")),
        _render_pivot_group_pills(base_url, 3,
                                   g_dims[2] if len(g_dims) > 2 else None,
                                   set(g_dims[:2]),
                                   _other("g3")),
        _render_pivot_range_pills(base_url, range,
                                  _other("range", "start", "end"),
                                  start=start or "", end=end or ""),
        _render_region_pills(base_url, region_param,
                             _other("region"), multi=True),
        _render_pivot_fuel_pills(base_url, fuel_list, _other("fuel")),
        _render_pivot_column_pills(base_url, selected_cols, _other("cols")),
    )
    content = _pivot_content(g_dims, selected_cols, range,
                             region_list, fuel_list, start, end)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /station-analysis — single-DUID or whole-station time series + TOD
# ----------------------------------------------------------------------------
#
# Production colors (kept verbatim so screenshots compare 1:1):
#   Generation: #2ca02c (green)   Price: #d62728 (red)
# Tier rule: 5-min for windows ≤7 days, 30-min otherwise — keeps the chart
# under ~50k points without losing detail in the recent-week range that
# operators look at most.

STATION_GEN_COLOR = "#2ca02c"
STATION_PRICE_COLOR = "#d62728"
# Includes every pivot range slug so clicking through from the pivot table
# carries the period over without translation. Adds 24H + All for station
# convenience.
STATION_RANGE_OPTIONS = [("24h", "24H"), ("7d", "7D"), ("30d", "30D"),
                         ("90d", "90D"), ("ytd", "YTD"), ("1y", "1Y"),
                         ("5y", "5Y"), ("all", "All")]


def _station_range_window(range_slug: str, start: str | None,
                          end: str | None
                          ) -> tuple[pd.Timestamp, pd.Timestamp,
                                     str, str, str, str]:
    """Resolve (s_ts, e_ts, gen_table, price_table, label, resample).
    Resolution tiering:
      ≤7 days  → 5-min source, no resample          → ~2k points
      7–30d    → 30-min source, no resample         → ~1.4k points
      >30 days → 30-min source, resampled to daily  → 30–1,825 points
    Keeps every chart under ~2k points per series; the 30-day cliff is where
    the per-interval line starts looking like noise on a wide canvas."""
    end_row = q("SELECT MAX(settlementdate) AS ts FROM generation_5min")
    if end_row.empty or end_row["ts"].iloc[0] is None:
        end_ts = pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None))
    else:
        end_ts = pd.Timestamp(end_row["ts"].iloc[0])

    presets = {
        "24h": (timedelta(hours=24),  "last 24h"),
        "7d":  (timedelta(days=7),    "last 7 days"),
        "30d": (timedelta(days=30),   "last 30 days"),
        "90d": (timedelta(days=90),   "last 90 days"),
        "1y":  (timedelta(days=365),  "last 12 months"),
        "5y":  (timedelta(days=1825), "last 5 years"),
    }
    if range_slug in presets:
        delta, label = presets[range_slug]
        s_ts = end_ts - delta
    elif range_slug == "ytd":
        s_ts = pd.Timestamp(year=end_ts.year, month=1, day=1)
        label = "year to date"
    elif range_slug == "all":
        start_row = q("SELECT MIN(settlementdate) AS ts FROM generation_30min")
        s_ts = (pd.Timestamp(start_row["ts"].iloc[0])
                if not start_row.empty else pd.Timestamp(2020, 2, 1))
        label = "all data"
    elif range_slug == "custom" and start and end:
        s_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end) + timedelta(days=1)
        label = f"{start} → {end}"
    else:
        s_ts = end_ts - timedelta(days=30)
        label = "last 30 days"

    span = end_ts - s_ts
    if span <= timedelta(days=7):
        return s_ts, end_ts, "generation_5min", "prices_5min", label, "none"
    if span <= timedelta(days=30):
        return s_ts, end_ts, "generation_30min", "prices_30min", label, "none"
    return s_ts, end_ts, "generation_30min", "prices_30min", label, "day"


def _resolve_station_subject(duid: str | None, station: str | None,
                             fuel: str | None = None,
                             region: str | None = None,
                             owner: str | None = None
                             ) -> tuple[list[str], str, str, dict]:
    """Resolve URL params → (duid_list, kind, label, meta).
    `kind` is 'duid' (single unit), 'station' (multi-unit station sum),
    'fleet' (any fuel/region/owner combination), or 'none'.
    `meta` carries: regions (list), is_multi_region, fuel, owner, station_name
    (where applicable), capacity_mw (effective, summed), is_battery, label."""
    if duid:
        df = q("""SELECT duid, region, "site name" AS station_name,
                         owner, fuel, capacity_mw, storage_mwh
                  FROM duid_mapping
                  WHERE duid = ?""", [duid])
        if df.empty:
            return [], "none", duid, {}
        r = df.iloc[0]
        is_battery = r["fuel"] == "Battery Storage"
        cap = (float(r["storage_mwh"]) / 24.0
               if is_battery and pd.notna(r["storage_mwh"])
               else float(r["capacity_mw"] or 0))
        meta = {
            "regions": [r["region"]], "is_multi_region": False,
            "region": r["region"], "fuel": r["fuel"],
            "owner": r["owner"], "station_name": r["station_name"],
            "capacity_mw": cap, "n_units": 1,
            "is_battery": bool(is_battery),
            "capacity_label": (
                f"Effective capacity {cap:.0f} MW (storage/24h)"
                if is_battery else f"Max capacity {cap:.0f} MW"),
        }
        return [duid], "duid", duid, meta
    if station:
        df = q("""SELECT duid, region, "site name" AS station_name,
                         owner, fuel, capacity_mw, storage_mwh
                  FROM duid_mapping
                  WHERE "site name" = ?""", [station])
        if df.empty:
            return [], "none", station, {}
        is_battery = (df["fuel"] == "Battery Storage").any()
        if is_battery:
            cap = float((df["storage_mwh"].fillna(0) / 24.0).sum())
        else:
            cap = float(df["capacity_mw"].fillna(0).sum())
        regions = df["region"].dropna().unique().tolist()
        meta = {
            "regions": regions,
            "is_multi_region": len(regions) > 1,
            "region": regions[0] if regions else "",
            "fuel": df["fuel"].iloc[0],
            "owner": df["owner"].iloc[0],
            "station_name": station,
            "capacity_mw": cap, "n_units": int(len(df)),
            "is_battery": bool(is_battery),
            "capacity_label": (
                f"Effective station capacity {cap:.0f} MW (storage/24h)"
                if is_battery else f"Station capacity {cap:.0f} MW"),
        }
        return df["duid"].tolist(), "station", station, meta

    # Fleet selection: any combination of fuel / region / owner.
    if fuel or region or owner:
        clauses = []
        params: list = []
        if fuel:
            # Use the display-fuel grouping so "Gas" picks up CCGT/OCGT/Gas other,
            # "Hydro" picks up "Water", etc. (Matches the Generators table.)
            raw = PIVOT_FUEL_TO_RAW.get(fuel, [fuel])
            placeholders = ",".join("?" * len(raw))
            clauses.append(f"fuel IN ({placeholders})")
            params.extend(raw)
        if region:
            regs = [r for r in region.split(",") if r]
            placeholders = ",".join("?" * len(regs))
            clauses.append(f"region IN ({placeholders})")
            params.extend(regs)
        if owner:
            clauses.append("owner = ?")
            params.append(owner)
        sql = ('SELECT duid, region, "site name" AS station_name, '
               'owner, fuel, capacity_mw, storage_mwh FROM duid_mapping '
               'WHERE ' + " AND ".join(clauses))
        df = q(sql, params)
        if df.empty:
            return [], "none", "", {}
        is_battery = (df["fuel"] == "Battery Storage").any()
        if is_battery:
            cap = float((df["storage_mwh"].fillna(0) / 24.0).sum())
        else:
            cap = float(df["capacity_mw"].fillna(0).sum())
        regions = sorted(df["region"].dropna().unique().tolist())

        # Smart label: pick the most specific filter combination.
        parts = []
        if fuel:   parts.append(fuel)
        if region: parts.append(region if "," not in region else "multi-region")
        if owner:  parts.append(owner)
        label = " · ".join(parts) if parts else "Fleet"
        n_units = int(len(df))

        meta = {
            "regions": regions,
            "is_multi_region": len(regions) > 1,
            "region": regions[0] if regions else "",
            "fuel": fuel or (df["fuel"].iloc[0] if not df.empty else ""),
            "owner": owner or "",
            "station_name": "",
            "capacity_mw": cap, "n_units": n_units,
            "is_battery": bool(is_battery),
            "capacity_label": (
                f"Fleet capacity {cap:.0f} MW"
                + (" (storage/24h)" if is_battery else "")),
            "fleet_filter_fuel": fuel,
            "fleet_filter_region": region,
            "fleet_filter_owner": owner,
        }
        return df["duid"].tolist(), "fleet", label, meta

    return [], "none", "", {}


def _price_join_sql(price_table: str, regions: list[str],
                    demand_table: str = "demand30") -> tuple[str, str, list]:
    """Build the per-interval price-source SQL for a selection of regions.

    Returns (cte_sql, price_col_expr, params).
      - cte_sql: a `price_src` CTE producing (settlementdate, price)
      - price_col_expr: how the outer SELECT refers to the price
      - params: bind values for the CTE

    Single-region selections take the simple path (no demand join).
    Multi-region selections compute the demand-weighted mean:
        price = Σ(rrp × demand) / Σ(demand)
    which is the standard NEM-wide reference price aggregation. The demand
    weights come from demand30; for 5-min source data we keep the 30-min
    demand weights since they're the only ones with the right cadence."""
    if len(regions) == 1:
        cte = f"""
          price_src AS (
            SELECT settlementdate, rrp AS price
            FROM {price_table}
            WHERE regionid = ?
          )
        """
        return cte, "price_src.price", [regions[0]]

    placeholders = ",".join("?" * len(regions))
    cte = f"""
      price_src AS (
        SELECT p.settlementdate,
               SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) AS price
        FROM {price_table} p
        JOIN {demand_table} d
          ON p.settlementdate = d.settlementdate
          AND p.regionid = d.regionid
        WHERE p.regionid IN ({placeholders})
        GROUP BY p.settlementdate
      )
    """
    return cte, "price_src.price", list(regions)


def _load_station_series(duids: list[str], regions: list[str],
                         s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                         gen_table: str, price_table: str,
                         resample: str = "none") -> pd.DataFrame:
    """Time-series rows for one DUID or summed across DUIDs of a station/fleet.
    SUM(scadavalue) across the selection. Price column is demand-weighted
    across `regions` when multi-region, else that region's RRP per interval.

    resample='day' first sums per-interval across DUIDs, then averages those
    per-interval MW values to a daily mean. Price aggregates as a simple mean
    over the day (mean of demand-weighted regional prices)."""
    if not duids:
        return pd.DataFrame()
    duid_in = ", ".join(f"'{d}'" for d in duids)
    price_cte, price_col, price_params = _price_join_sql(
        price_table, regions)

    if resample == "day":
        sql = f"""
            WITH {price_cte},
            per_int AS (
              SELECT g.settlementdate,
                     SUM(g.scadavalue) AS mw,
                     FIRST({price_col}) AS price
              FROM {gen_table} g
              LEFT JOIN price_src
                ON g.settlementdate = price_src.settlementdate
              WHERE g.duid IN ({duid_in})
                AND g.settlementdate >= ? AND g.settlementdate < ?
              GROUP BY g.settlementdate
            )
            SELECT date_trunc('day', settlementdate) AS settlementdate,
                   AVG(mw) AS scadavalue,
                   AVG(price) AS price
            FROM per_int
            GROUP BY date_trunc('day', settlementdate)
            ORDER BY settlementdate
        """
        return q(sql, price_params + [s_ts, e_ts])

    sql = f"""
        WITH {price_cte}
        SELECT g.settlementdate,
               SUM(g.scadavalue) AS scadavalue,
               FIRST({price_col}) AS price
        FROM {gen_table} g
        LEFT JOIN price_src
          ON g.settlementdate = price_src.settlementdate
        WHERE g.duid IN ({duid_in})
          AND g.settlementdate >= ? AND g.settlementdate < ?
        GROUP BY g.settlementdate
        ORDER BY g.settlementdate
    """
    return q(sql, price_params + [s_ts, e_ts])


def _load_station_tod(duids: list[str], regions: list[str],
                      s_ts: pd.Timestamp, e_ts: pd.Timestamp) -> pd.DataFrame:
    """Hour-of-day aggregates for the TOD chart. Always sourced from 30-min
    data (independent of whatever resolution the TS chart picked) so it stays
    useful on long windows where the TS itself has been resampled to daily.
    The hour-of-day buckets are computed in SQL — returns 24 rows. Price is
    demand-weighted across regions when multi-region."""
    if not duids:
        return pd.DataFrame()
    duid_in = ", ".join(f"'{d}'" for d in duids)
    price_cte, price_col, price_params = _price_join_sql(
        "prices_30min", regions)
    sql = f"""
        WITH {price_cte},
        per_int AS (
          SELECT g.settlementdate,
                 SUM(g.scadavalue) AS mw,
                 FIRST({price_col}) AS price
          FROM generation_30min g
          LEFT JOIN price_src
            ON g.settlementdate = price_src.settlementdate
          WHERE g.duid IN ({duid_in})
            AND g.settlementdate >= ? AND g.settlementdate < ?
          GROUP BY g.settlementdate
        )
        SELECT EXTRACT(HOUR FROM settlementdate)::INT AS hour,
               AVG(mw) AS mw,
               AVG(price) AS price
        FROM per_int
        GROUP BY EXTRACT(HOUR FROM settlementdate)
        ORDER BY hour
    """
    return q(sql, price_params + [s_ts, e_ts])


def _station_stats(duids: list[str], regions: list[str], is_battery: bool,
                   cap_mw: float, s_ts: pd.Timestamp,
                   e_ts: pd.Timestamp, gen_table: str,
                   price_table: str) -> dict:
    """Window aggregates matching the Generators table's convention exactly:
    discharge-only for everything (GREATEST(scada, 0)), capacity = effective
    capacity (storage/24 for batteries). Revenue uses the demand-weighted
    price for multi-region selections so the $/MWh stat lines up with the
    chart's price line."""
    if not duids:
        return {}
    duid_in = ", ".join(f"'{d}'" for d in duids)
    hours_factor = 0.5 if gen_table == "generation_30min" else 5.0 / 60.0
    price_cte, price_col, price_params = _price_join_sql(
        price_table, regions)
    sql = f"""
      WITH {price_cte}
      SELECT
        SUM(GREATEST(g.scadavalue, 0)) * {hours_factor}     AS gen_mwh,
        SUM(GREATEST(g.scadavalue, 0)
            * COALESCE({price_col}, 0) * {hours_factor})    AS revenue,
        COUNT(*)                                            AS n_intervals
      FROM {gen_table} g
      LEFT JOIN price_src
        ON g.settlementdate = price_src.settlementdate
      WHERE g.duid IN ({duid_in})
        AND g.settlementdate >= ? AND g.settlementdate < ?
    """
    row = q(sql, price_params + [s_ts, e_ts])
    if row.empty:
        return {}
    gen_mwh = float(row["gen_mwh"].iloc[0] or 0)
    revenue = float(row["revenue"].iloc[0] or 0)
    hours = (e_ts - s_ts).total_seconds() / 3600.0
    price = (revenue / gen_mwh) if gen_mwh > 0 else None
    util = ((gen_mwh / (cap_mw * hours) * 100)
            if cap_mw > 0 and hours > 0 else None)
    return {
        "gen_gwh": gen_mwh / 1000.0,
        "rev_m":   revenue / 1_000_000.0,
        "price":   price,
        "util":    util,
        "cap_mw":  cap_mw,
        "is_battery": is_battery,
    }


def _render_station_stats(stats: dict, range_label: str) -> str:
    """Stats strip: same metric set as the pivot row that brought the user
    here, so they can keep their place without bouncing between tabs."""
    if not stats:
        return ""

    def _fmt_int(v):
        if v is None:
            return '<span style="color:#878580">—</span>'
        return f"{round(v):,}"

    def _fmt_one(v, prefix=""):
        if v is None:
            return '<span style="color:#878580">—</span>'
        return f"{prefix}{round(v):,}"

    tiles = [
        ("GWh",      _fmt_int(stats["gen_gwh"])),
        ("Rev $M",   _fmt_int(stats["rev_m"])),
        ("$/MWh",    _fmt_one(stats["price"])),
        ("Util %",   _fmt_one(stats["util"])),
        ("Cap MW",   _fmt_int(stats["cap_mw"])),
    ]
    if stats.get("is_battery"):
        # Surface that this is an "effective" capacity so the util % is
        # interpretable. One-cycle-per-day = MWh_storage / 24.
        tiles[-1] = ("Cap MW (eff)", _fmt_int(stats["cap_mw"]))

    tile_html = "".join(
        f'<div style="background:{PAPER};border:1px solid {BORDER};'
        f'border-radius:6px;padding:8px 14px;min-width:96px">'
        f'<div style="font-size:10px;color:{MUTED};text-transform:uppercase;'
        f'letter-spacing:0.4px;font-weight:600">{label}</div>'
        f'<div style="font-size:18px;font-weight:600;color:{INK};'
        f'margin-top:2px">{val}</div>'
        f'</div>'
        for label, val in tiles
    )
    return (f'<div style="display:flex;flex-wrap:wrap;gap:8px;'
            f'margin:0 0 12px 0">{tile_html}</div>'
            f'<div style="font-size:11px;color:{MUTED};margin:0 0 8px 0">'
            f'{range_label} &middot; discharge-only convention for batteries '
            f'(matches Generators table)</div>')


def _battery_subject_stats(duids: list[str], storage_mwh: float,
                           s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                           gen_table: str, price_table: str) -> dict:
    """Battery-specific window aggregates: discharge AND charge sides
    separately (the standard _station_stats applies discharge-only).
    Each DUID is priced at its OWN region's RRP so the math stays correct
    for multi-region fleet selections."""
    if not duids:
        return {}
    duid_in = ", ".join(f"'{d}'" for d in duids)
    hours_factor = 0.5 if gen_table == "generation_30min" else 5.0 / 60.0
    sql = f"""
      SELECT
        SUM(GREATEST(g.scadavalue, 0)) * {hours_factor}     AS discharge_mwh,
        SUM(GREATEST(-g.scadavalue, 0)) * {hours_factor}    AS charge_mwh,
        SUM(GREATEST(g.scadavalue, 0)
            * COALESCE(p.rrp, 0) * {hours_factor})          AS discharge_rev,
        SUM(GREATEST(-g.scadavalue, 0)
            * COALESCE(p.rrp, 0) * {hours_factor})          AS charge_cost
      FROM {gen_table} g
      LEFT JOIN duid_mapping d ON g.duid = d.duid
      LEFT JOIN {price_table} p
        ON g.settlementdate = p.settlementdate
        AND d.region = p.regionid
      WHERE g.duid IN ({duid_in})
        AND g.settlementdate >= ? AND g.settlementdate < ?
    """
    row = q(sql, [s_ts, e_ts])
    if row.empty:
        return {}
    disch_mwh = float(row["discharge_mwh"].iloc[0] or 0)
    ch_mwh    = float(row["charge_mwh"].iloc[0] or 0)
    disch_rev = float(row["discharge_rev"].iloc[0] or 0)
    ch_cost   = float(row["charge_cost"].iloc[0] or 0)
    hours = (e_ts - s_ts).total_seconds() / 3600.0
    year_hours = 365.0 * 24.0
    disch_price = (disch_rev / disch_mwh) if disch_mwh > 0 else None
    ch_price = (ch_cost / ch_mwh) if ch_mwh > 0 else None
    spread = ((disch_price - ch_price)
              if (disch_price is not None and ch_price is not None) else None)
    spread_rev = disch_rev - ch_cost
    annual_spread = (spread_rev * year_hours / hours) if hours > 0 else 0
    spread_per_mwh_yr = (annual_spread / storage_mwh) if storage_mwh > 0 else None
    eff_cap = storage_mwh / 24.0
    util = ((disch_mwh / (eff_cap * hours) * 100)
            if eff_cap > 0 and hours > 0 else None)
    rt_eff = ((disch_mwh / ch_mwh * 100) if ch_mwh > 0 else None)
    return {
        "discharge_gwh":   disch_mwh / 1000.0,
        "charge_gwh":      ch_mwh / 1000.0,
        "discharge_rev_m": disch_rev / 1_000_000.0,
        "charge_cost_m":   ch_cost / 1_000_000.0,
        "discharge_price": disch_price,
        "charge_price":    ch_price,
        "spread":          spread,
        "spread_per_mwh_yr": spread_per_mwh_yr,
        "util":             util,
        "cap_mw_eff":       eff_cap,
        "storage_mwh":      storage_mwh,
        "rt_efficiency":    rt_eff,
    }


def _render_battery_subject_stats(stats: dict, range_label: str) -> str:
    """Battery stats strip — the full economics surface. Includes the
    headline investment metric ($/MWh-cap/yr) and the round-trip
    efficiency derived from the discharge/charge MWh ratio."""
    if not stats:
        return ""

    def _fmt_int(v, prefix=""):
        if v is None:
            return '<span style="color:#878580">—</span>'
        return f"{prefix}{round(v):,}"

    def _fmt(v, suffix=""):
        if v is None:
            return '<span style="color:#878580">—</span>'
        return f"{round(v):,}{suffix}"

    tiles = [
        ("Disch GWh",       _fmt(stats["discharge_gwh"])),
        ("Chrg GWh",        _fmt(stats["charge_gwh"])),
        ("Disch $M",        _fmt(stats["discharge_rev_m"])),
        ("Chrg $M",         _fmt(stats["charge_cost_m"])),
        ("Disch $/MWh",     _fmt(stats["discharge_price"])),
        ("Chrg $/MWh",      _fmt(stats["charge_price"])),
        ("Spread $/MWh",    _fmt(stats["spread"])),
        ("$/MWh-cap/yr",    _fmt(stats["spread_per_mwh_yr"])),
        ("Util %",          _fmt(stats["util"], "%")),
        ("RT eff %",        _fmt(stats["rt_efficiency"], "%")),
        ("Cap MW (eff)",    _fmt(stats["cap_mw_eff"])),
        ("Storage MWh",     _fmt(stats["storage_mwh"])),
    ]
    tile_html = "".join(
        f'<div style="background:{PAPER};border:1px solid {BORDER};'
        f'border-radius:6px;padding:8px 14px;min-width:96px">'
        f'<div style="font-size:10px;color:{MUTED};text-transform:uppercase;'
        f'letter-spacing:0.4px;font-weight:600">{label}</div>'
        f'<div style="font-size:18px;font-weight:600;color:{INK};'
        f'margin-top:2px">{val}</div>'
        f'</div>'
        for label, val in tiles
    )
    return (f'<div style="display:flex;flex-wrap:wrap;gap:8px;'
            f'margin:0 0 12px 0">{tile_html}</div>'
            f'<div style="font-size:11px;color:{MUTED};margin:0 0 8px 0">'
            f'{range_label} &middot; battery economics — discharge / charge '
            f'sides separately. <strong>$/MWh-cap/yr</strong> = annualised '
            f'(disch rev − chrg cost) ÷ storage MWh, the investment metric. '
            f'RT eff = round-trip efficiency (disch MWh ÷ chrg MWh).</div>')


def _price_axis_should_log(prices: pd.Series) -> bool:
    """Spike detector for the price axis. Switch to log when:
      - max > $1,000/MWh (genuine spike territory, well above normal $30–300),
      - OR max > 5× the 95th percentile (i.e. a tail event distorting the rest),
      - OR max > 20× the median (longer-window 'mostly cheap with a spike').
    Negative prices aren't an issue here — log axis silently drops them, and
    they're rare on a per-station time series anyway. Returns False on an
    empty / all-NaN series so the linear default is kept."""
    s = prices.dropna() if isinstance(prices, pd.Series) else pd.Series(prices)
    s = s[s > 0]
    if len(s) == 0:
        return False
    max_p = float(s.max())
    if max_p > 1000:
        return True
    p95 = float(s.quantile(0.95))
    if p95 > 0 and max_p / p95 > 5:
        return True
    med = float(s.median())
    if med > 0 and max_p / med > 20:
        return True
    return False


def _station_ts_chart(df: pd.DataFrame, meta: dict, label: str,
                      range_label: str,
                      resolution_label: str = "30-min") -> str:
    """Dual-axis time series. Generation on left (green), price on right
    (red), max-capacity reference line (dashed green) drawn across the
    full window. Production-matching colors. Price axis auto-switches to
    log when the window contains a spike (see _price_axis_should_log).
    Hover format adapts: daily resampled data drops the time component."""
    from plotly.subplots import make_subplots

    is_daily = resolution_label == "daily mean"
    hover_x_fmt = "%{x|%d %b %Y}" if is_daily else "%{x|%d %b %Y %H:%M}"
    gen_name = ("Daily mean generation (MW)" if is_daily
                else "Generation (MW)")
    price_name = ("Daily mean price ($/MWh)" if is_daily
                  else "Price ($/MWh)")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if df.empty:
        fig.add_annotation(
            text="No data for this window", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=MUTED),
        )
    else:
        fig.add_trace(go.Scatter(
            x=df["settlementdate"], y=df["scadavalue"],
            mode="lines", name=gen_name,
            line=dict(width=1.5, color=STATION_GEN_COLOR),
            hovertemplate=f"{hover_x_fmt}<br>%{{y:.1f}} MW<extra></extra>",
        ), secondary_y=False)

        # Capacity reference line only when the selection is a single region
        # (the line is meaningful then). For multi-region fleets the summed
        # cap dominates the y-axis and the chart loses shape — see the meta
        # `is_multi_region` flag set by _resolve_station_subject.
        cap = float(meta.get("capacity_mw") or 0)
        if cap > 0 and not meta.get("is_multi_region"):
            x_pad = [df["settlementdate"].iloc[0],
                     df["settlementdate"].iloc[-1]]
            fig.add_trace(go.Scatter(
                x=x_pad, y=[cap, cap], mode="lines",
                name=meta.get("capacity_label", f"Capacity {cap:.0f} MW"),
                line=dict(width=1.5, color=STATION_GEN_COLOR, dash="dash"),
                opacity=0.7,
                hovertemplate=f"{cap:.0f} MW<extra></extra>",
            ), secondary_y=False)

        fig.add_trace(go.Scatter(
            x=df["settlementdate"], y=df["price"],
            mode="lines", name=price_name,
            line=dict(width=1.2, color=STATION_PRICE_COLOR),
            hovertemplate=f"{hover_x_fmt}<br>$%{{y:.1f}}/MWh<extra></extra>",
        ), secondary_y=True)

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=500,
        margin=dict(l=60, r=60, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        showlegend=True,
    )
    fig.update_xaxes(gridcolor=BORDER, gridwidth=0.5, showspikes=False)
    fig.update_yaxes(
        title=dict(text="Generation (MW)",
                   font=dict(size=11, color=STATION_GEN_COLOR)),
        gridcolor=BORDER, gridwidth=0.5, zeroline=True,
        zerolinecolor=BORDER, secondary_y=False,
    )
    use_log = (not df.empty) and _price_axis_should_log(df["price"])
    fig.update_yaxes(
        title=dict(
            text="Price ($/MWh, log)" if use_log else "Price ($/MWh)",
            font=dict(size=11, color=STATION_PRICE_COLOR)),
        type="log" if use_log else "linear",
        showgrid=False, zeroline=False, secondary_y=True,
    )

    div_id = f"plot-station-ts-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"Output and price &middot; {range_label} "
                     f"&middot; {resolution_label}")
            + f'<div id="{div_id}" style="height:500px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + _attribution())


def _station_tod_chart(tod: pd.DataFrame, meta: dict, label: str) -> str:
    """Average MW + average $/MWh by hour of day (0–23). `tod` is the
    pre-aggregated 24-row dataframe from _load_station_tod (always 30-min
    source) so this chart stays useful even when the TS chart has been
    resampled to daily."""
    from plotly.subplots import make_subplots

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if tod.empty:
        fig.add_annotation(
            text="No data for this window", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=MUTED),
        )
    else:
        fig.add_trace(go.Scatter(
            x=tod["hour"], y=tod["mw"], mode="lines+markers",
            name="Avg generation (MW)",
            line=dict(width=2, color=STATION_GEN_COLOR),
            marker=dict(size=7, color=STATION_GEN_COLOR),
            hovertemplate="Hr %{x:02d}:00<br>%{y:.1f} MW<extra></extra>",
        ), secondary_y=False)
        # No capacity reference line on the TOD chart — averages can't reach
        # the cap unless the unit is 100%-loaded every interval, so the line
        # mostly just stretches the y-axis and flattens the data shape. The
        # capacity is still shown in the subtitle and the stats strip.
        fig.add_trace(go.Scatter(
            x=tod["hour"], y=tod["price"], mode="lines+markers",
            name="Avg price ($/MWh)",
            line=dict(width=2, color=STATION_PRICE_COLOR),
            marker=dict(size=7, color=STATION_PRICE_COLOR),
            hovertemplate="Hr %{x:02d}:00<br>$%{y:.1f}/MWh<extra></extra>",
        ), secondary_y=True)

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=420,
        margin=dict(l=60, r=60, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.20,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        showlegend=True,
    )
    fig.update_xaxes(
        title=dict(text="Hour of day", font=dict(size=11, color=MUTED)),
        tickvals=list(range(0, 24, 3)),
        gridcolor=BORDER, gridwidth=0.5,
    )
    fig.update_yaxes(
        title=dict(text="Average generation (MW)",
                   font=dict(size=11, color=STATION_GEN_COLOR)),
        gridcolor=BORDER, gridwidth=0.5, zeroline=True,
        zerolinecolor=BORDER, secondary_y=False,
    )
    # TOD prices are *averages* per hour, so spikes are already smoothed.
    # We only flip to log when the hourly means themselves blow out — which
    # happens for stations exposed to recurring evening-peak spikes.
    tod_prices = (tod["price"] if not tod.empty
                  else pd.Series([], dtype=float))
    use_log = _price_axis_should_log(tod_prices)
    fig.update_yaxes(
        title=dict(
            text="Average price ($/MWh, log)" if use_log
                 else "Average price ($/MWh)",
            font=dict(size=11, color=STATION_PRICE_COLOR)),
        type="log" if use_log else "linear",
        showgrid=False, zeroline=False, secondary_y=True,
    )

    div_id = f"plot-station-tod-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3("Average performance by hour of day")
            + f'<div id="{div_id}" style="height:420px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + _attribution())


def _station_placeholder() -> str:
    """Friendly empty state when no DUID/station is selected. Station
    Analysis is no longer in the top nav, but direct-URL/bookmark hits with
    no params still land here; this nudges the user to the Generators
    table (the only entry point) and shows the deep-link URL pattern."""
    return ('<div class="placeholder">'
            '<p><strong>No station selected</strong></p>'
            '<p>Open the <a href="/generators" '
            'style="color:#24837b">Generators</a> table and click any '
            'station or DUID name to land here. Direct URLs work too: '
            '<code>?duid=BW01</code> or <code>?station=Bayswater</code>.</p>'
            '</div>')


# ── Battery daily-trend card ────────────────────────────────────────────────
# Third chart on the Station Analysis page when the subject is a battery.
# Lets the user pick one or two daily-aggregated metrics and watch them
# evolve over the page's selected window. Use case: "spread shrinks as more
# batteries come online", "round-trip efficiency drift over time", etc.

_BATTERY_TREND_METRICS = [
    ("spread",      "Spread $/MWh",     "$/MWh"),
    ("cycles",      "Cycles/day",       "/day"),
    ("disch_gwh",   "Disch GWh/day",    "GWh"),
    ("ch_gwh",      "Chrg GWh/day",     "GWh"),
    ("disch_price", "Disch $/MWh",      "$/MWh"),
    ("ch_price",    "Chrg $/MWh",       "$/MWh"),
    ("net_rev_k",   "Net rev $K/day",   "$K"),
    ("rt_eff",      "RT efficiency %",  "%"),
    ("util_pct",    "Util % daily",     "%"),
]
_BATTERY_TREND_SMOOTHING = [
    ("raw",  "Raw"),
    ("7d",   "7-day"),
    ("30d",  "30-day"),
    ("90d",  "90-day"),
]


def _load_battery_daily_trend(duids: list[str],
                              s_ts: pd.Timestamp, e_ts: pd.Timestamp
                              ) -> pd.DataFrame:
    """Daily aggregates for battery trend charts. SUMs are over both DUIDs
    and intervals (one row per calendar day). Each DUID is priced at its
    own region's RRP via the duid_mapping join."""
    if not duids:
        return pd.DataFrame()
    duid_in = ", ".join(f"'{d}'" for d in duids)
    sql = f"""
      SELECT
        date_trunc('day', g.settlementdate) AS day,
        SUM(GREATEST(g.scadavalue, 0)) * 0.5  AS disch_mwh,
        SUM(GREATEST(-g.scadavalue, 0)) * 0.5 AS chrg_mwh,
        SUM(GREATEST(g.scadavalue, 0)
            * COALESCE(p.rrp, 0) * 0.5)        AS disch_rev,
        SUM(GREATEST(-g.scadavalue, 0)
            * COALESCE(p.rrp, 0) * 0.5)        AS chrg_cost,
        COUNT(*)                               AS n_rows
      FROM generation_30min g
      LEFT JOIN duid_mapping d ON g.duid = d.duid
      LEFT JOIN prices_30min p
        ON g.settlementdate = p.settlementdate
        AND d.region = p.regionid
      WHERE g.duid IN ({duid_in})
        AND g.settlementdate >= ? AND g.settlementdate < ?
      GROUP BY date_trunc('day', g.settlementdate)
      ORDER BY day
    """
    return q(sql, [s_ts, e_ts])


def _battery_trend_compute(df: pd.DataFrame, storage_mwh: float
                           ) -> pd.DataFrame:
    """Derive the daily metrics that the trend chart can plot."""
    if df.empty:
        return df
    out = df.copy()
    out["day"] = pd.to_datetime(out["day"])
    out["disch_price"] = np.where(
        out["disch_mwh"] > 0, out["disch_rev"] / out["disch_mwh"], np.nan)
    out["ch_price"] = np.where(
        out["chrg_mwh"] > 0, out["chrg_cost"] / out["chrg_mwh"], np.nan)
    out["spread"] = out["disch_price"] - out["ch_price"]
    out["disch_gwh"] = out["disch_mwh"] / 1000.0
    out["ch_gwh"] = out["chrg_mwh"] / 1000.0
    out["net_rev_k"] = (out["disch_rev"] - out["chrg_cost"]) / 1000.0
    out["rt_eff"] = np.where(
        out["chrg_mwh"] > 0, out["disch_mwh"] / out["chrg_mwh"] * 100,
        np.nan)
    if storage_mwh > 0:
        out["cycles"] = out["disch_mwh"] / storage_mwh
        # Daily util %: fraction of a "one-cycle-per-day" baseline used.
        out["util_pct"] = out["cycles"] * 100
    else:
        out["cycles"] = np.nan
        out["util_pct"] = np.nan
    return out


def _battery_trend_apply_smoothing(s: pd.Series, smoothing: str) -> pd.Series:
    if smoothing == "raw" or s.empty:
        return s
    win = {"7d": 7, "30d": 30, "90d": 90}.get(smoothing, 0)
    if win <= 0:
        return s
    return s.rolling(win, min_periods=max(1, win // 3)).mean()


def _render_battery_trend_metric_pills(base_url: str, label: str,
                                       param_name: str, active: str,
                                       other_params: dict,
                                       include_none: bool) -> str:
    pills = []
    if include_none:
        params = dict(other_params, **{param_name: ""})
        url = _build_url(base_url, **params)
        cls = "pill active" if not active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'(none)</button>'
        )
    for slug, mlabel, _unit in _BATTERY_TREND_METRICS:
        params = dict(other_params, **{param_name: slug})
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{mlabel}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">{label}</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _render_battery_trend_smoothing_pills(base_url: str, active: str,
                                          other_params: dict) -> str:
    pills = []
    for slug, mlabel in _BATTERY_TREND_SMOOTHING:
        params = dict(other_params, tsm=slug)
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{mlabel}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Smoothing</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _build_battery_trend_chart(df: pd.DataFrame, primary: str,
                               secondary: str, smoothing: str) -> str:
    """Dual-axis trend chart. Primary on the left axis (blue), optional
    secondary on the right axis (orange). Smoothing overlays a rolling
    mean on the raw daily line."""
    from plotly.subplots import make_subplots

    metric_lookup = {slug: (label, unit)
                     for slug, label, unit in _BATTERY_TREND_METRICS}
    primary_label, primary_unit = metric_lookup.get(primary,
                                                    (primary, ""))
    secondary_label = None
    if secondary:
        secondary_label, _ = metric_lookup.get(secondary, (secondary, ""))

    fig = make_subplots(specs=[[{"secondary_y": bool(secondary)}]])
    if df.empty or primary not in df.columns:
        fig.add_annotation(
            text="No daily data for this window",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=14, color=MUTED))
    else:
        # Primary series (blue)
        primary_color = "#205EA6"
        secondary_color = "#BC5215"
        y = df[primary]
        y_smooth = _battery_trend_apply_smoothing(y, smoothing)
        if smoothing == "raw":
            fig.add_trace(go.Scatter(
                x=df["day"], y=y, mode="lines", name=primary_label,
                line=dict(color=primary_color, width=1.5),
                hovertemplate=(f"{primary_label}: %{{y:.2f}}"
                               "<br>%{x|%d %b %Y}<extra></extra>"),
            ), secondary_y=False)
        else:
            # Raw lighter, smoothed bold
            fig.add_trace(go.Scatter(
                x=df["day"], y=y, mode="lines",
                name=f"{primary_label} (raw)",
                line=dict(color=primary_color, width=0.8),
                opacity=0.35,
                hovertemplate=(f"{primary_label} raw: %{{y:.2f}}"
                               "<br>%{x|%d %b %Y}<extra></extra>"),
            ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=df["day"], y=y_smooth, mode="lines",
                name=f"{primary_label} ({smoothing})",
                line=dict(color=primary_color, width=2.2),
                hovertemplate=(f"{primary_label} {smoothing}: %{{y:.2f}}"
                               "<br>%{x|%d %b %Y}<extra></extra>"),
            ), secondary_y=False)

        if secondary and secondary in df.columns:
            y2 = df[secondary]
            y2_smooth = _battery_trend_apply_smoothing(y2, smoothing)
            if smoothing == "raw":
                fig.add_trace(go.Scatter(
                    x=df["day"], y=y2, mode="lines", name=secondary_label,
                    line=dict(color=secondary_color, width=1.5),
                    hovertemplate=(f"{secondary_label}: %{{y:.2f}}"
                                   "<br>%{x|%d %b %Y}<extra></extra>"),
                ), secondary_y=True)
            else:
                fig.add_trace(go.Scatter(
                    x=df["day"], y=y2, mode="lines",
                    name=f"{secondary_label} (raw)",
                    line=dict(color=secondary_color, width=0.8),
                    opacity=0.35,
                    hovertemplate=(f"{secondary_label} raw: %{{y:.2f}}"
                                   "<br>%{x|%d %b %Y}<extra></extra>"),
                ), secondary_y=True)
                fig.add_trace(go.Scatter(
                    x=df["day"], y=y2_smooth, mode="lines",
                    name=f"{secondary_label} ({smoothing})",
                    line=dict(color=secondary_color, width=2.2),
                    hovertemplate=(f"{secondary_label} {smoothing}: "
                                   "%{y:.2f}<br>%{x|%d %b %Y}<extra></extra>"),
                ), secondary_y=True)

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=420,
        margin=dict(l=60, r=60, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        showlegend=True,
    )
    fig.update_xaxes(gridcolor=BORDER, gridwidth=0.5,
                     title=dict(text="", font=dict(color=MUTED)))
    fig.update_yaxes(
        title=dict(text=primary_label, font=dict(size=11, color="#205EA6")),
        gridcolor=BORDER, gridwidth=0.5, zeroline=False, secondary_y=False,
    )
    if secondary:
        fig.update_yaxes(
            title=dict(text=secondary_label,
                       font=dict(size=11, color="#BC5215")),
            showgrid=False, zeroline=False, secondary_y=True,
        )

    div_id = f"plot-batt-trend-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (f'<div id="{div_id}" style="height:420px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>')


def _battery_trend_card(duids: list[str], meta: dict,
                        s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                        primary: str, secondary: str,
                        smoothing: str, base_url: str,
                        other_params: dict) -> str:
    """Daily-trend card content: inline metric/smoothing pills + chart.

    Selectors live inside the card (not in the global selector strip) so
    they only appear for battery subjects and don't clutter the main bar."""
    raw_df = _load_battery_daily_trend(duids, s_ts, e_ts)
    storage_mwh = float(meta.get("capacity_mw") or 0) * 24.0
    df = _battery_trend_compute(raw_df, storage_mwh)

    # Pill bars; we strip trend1/trend2/tsm from "other params" per bar so
    # only the relevant param is in scope when clicking.
    pill_html = (
        '<div class="selector-strip" style="margin:0 0 12px 0">'
        + _render_battery_trend_metric_pills(
            base_url, "Primary", "trend1", primary,
            {k: v for k, v in other_params.items() if k != "trend1"},
            include_none=False)
        + _render_battery_trend_metric_pills(
            base_url, "Secondary", "trend2", secondary,
            {k: v for k, v in other_params.items() if k != "trend2"},
            include_none=True)
        + _render_battery_trend_smoothing_pills(
            base_url, smoothing,
            {k: v for k, v in other_params.items() if k != "tsm"})
        + '</div>'
    )

    chart_html = _build_battery_trend_chart(df, primary, secondary, smoothing)
    return (_card_h3("Daily trend")
            + pill_html
            + chart_html
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;'
              f'line-height:1.5">Daily aggregates from 30-min data. Pick a '
              f'primary metric for the left axis and optionally a secondary '
              f'for the right axis (e.g. <strong>Spread $/MWh</strong> vs '
              f'<strong>Cycles/day</strong>). Smoothing overlays a rolling '
              f'mean; raw stays visible as a faint line.</p>'
            + _attribution())


def _station_content(duids: list[str], kind: str, label: str, meta: dict,
                     range_slug: str, start: str | None,
                     end: str | None,
                     trend1: str = "spread", trend2: str = "",
                     tsm: str = "30d",
                     carry_params: dict | None = None) -> str:
    if not duids:
        return _station_placeholder()

    s_ts, e_ts, gen_table, price_table, range_label, resample = (
        _station_range_window(range_slug, start, end))
    regions = meta.get("regions") or ([meta["region"]]
                                      if meta.get("region") else [])
    df = _load_station_series(duids, regions, s_ts, e_ts,
                              gen_table, price_table, resample=resample)
    resolution_label = ("daily mean" if resample == "day"
                        else "5-min" if gen_table == "generation_5min"
                        else "30-min")

    cap = meta.get("capacity_mw", 0)
    fuel = meta.get("fuel", "—")
    owner = meta.get("owner", "—")
    n_units = meta.get("n_units", len(duids))
    if kind == "duid":
        kind_chip = "DUID"
    elif kind == "station":
        kind_chip = f"Station &middot; {n_units} units"
    else:
        kind_chip = f"Fleet &middot; {n_units} units"
    region_chip = ("multi-region" if meta.get("is_multi_region")
                   else (regions[0][:-1] if regions else "—"))
    subtitle_parts = [
        f"<strong>{label}</strong>",
        f"{kind_chip}",
        f"{fuel}",
        region_chip,
        (f"{cap:.0f} MW" if not meta.get("is_battery")
         else f"{cap:.0f} MW eff. ({float(cap)*24:.0f} MWh storage)"),
        owner or "—",
    ]
    subtitle = " &middot; ".join(subtitle_parts)

    # Battery selections get the full economics surface (charge + discharge
    # sides separately, plus $/MWh-cap/yr) — the discharge-only convention
    # from Generators glosses over what people actually care about on the
    # battery detail page.
    if meta.get("is_battery"):
        # capacity_mw is the effective (storage/24) MW for batteries; we
        # need the underlying storage in MWh for $/MWh-cap/yr.
        storage_mwh = float(meta.get("capacity_mw") or 0) * 24.0
        batt_stats = _battery_subject_stats(
            duids, storage_mwh, s_ts, e_ts, gen_table, price_table)
        stats_html = _render_battery_subject_stats(batt_stats, range_label)
    else:
        stats = _station_stats(duids, regions,
                               meta.get("is_battery", False),
                               float(meta.get("capacity_mw") or 0),
                               s_ts, e_ts, gen_table, price_table)
        stats_html = _render_station_stats(stats, range_label)

    ts_html = _station_ts_chart(df, meta, label, range_label, resolution_label)
    # TOD always sources from 30-min data (independent of TS resolution).
    tod_df = _load_station_tod(duids, regions, s_ts, e_ts)
    tod_html = _station_tod_chart(tod_df, meta, label)

    # Back link carries the current range. Routes to /batteries for
    # battery selections (the lollipop + ranking is where you came from)
    # and /generators otherwise.
    back_q = f"?range={range_slug}" if range_slug else ""
    if range_slug == "custom":
        if start: back_q += f"&start={start}"
        if end:   back_q += f"&end={end}"
    if meta.get("is_battery"):
        back_url = f"/batteries{back_q}"
        back_label = "← Batteries"
    else:
        back_url = f"/generators{back_q}"
        back_label = "← Generators"
    back_link = (f'<a href="{back_url}" '
                 f'hx-get="{back_url}" '
                 f'hx-target="#tab-body" hx-push-url="true" '
                 f'style="color:{TEAL};font-size:12px;text-decoration:none;'
                 f'display:inline-block;margin:0 0 8px 0">'
                 f'{back_label}</a>')

    # Battery-only third card: daily-trend chart with metric pickers.
    # carry_params from the route preserves every other URL param so the
    # trend pills don't reset region/range/etc. when clicked.
    trend_card_html = ""
    if meta.get("is_battery"):
        trend_inner = _battery_trend_card(
            duids, meta, s_ts, e_ts, trend1, trend2, tsm,
            "/station-analysis", carry_params or {})
        trend_card_html = f'<div class="card">{trend_inner}</div>'

    return ('<div class="prices-stack">'
            f'<div class="card">'
            + back_link
            + _card_h3(f"Station analysis &middot; {range_label}")
            + f'<p style="color:{MUTED};font-size:12px;margin:0 0 8px 14px">'
              f'{subtitle}</p>'
            + stats_html
            + ts_html
            + '</div>'
            f'<div class="card">' + tod_html + '</div>'
            + trend_card_html
            + '</div>')


@app.get("/station-analysis", response_class=HTMLResponse)
def station_page(request: Request,
                 duid: str | None = None,
                 station: str | None = None,
                 fuel: str | None = None,
                 region: str | None = None,
                 owner: str | None = None,
                 range: str = "30d",
                 start: str | None = None,
                 end: str | None = None,
                 trend1: str = "spread",
                 trend2: str = "",
                 tsm: str = "30d") -> HTMLResponse:
    duids, kind, label, meta = _resolve_station_subject(
        duid, station, fuel=fuel, region=region, owner=owner)

    valid_ranges = {slug for slug, _ in STATION_RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "30d"

    valid_trend_metrics = {s for s, _, _ in _BATTERY_TREND_METRICS}
    if trend1 not in valid_trend_metrics:
        trend1 = "spread"
    if trend2 and trend2 not in valid_trend_metrics:
        trend2 = ""
    valid_smoothing = {s for s, _ in _BATTERY_TREND_SMOOTHING}
    if tsm not in valid_smoothing:
        tsm = "30d"

    base_params = {}
    if duid:    base_params["duid"]    = duid
    if station: base_params["station"] = station
    if fuel:    base_params["fuel"]    = fuel
    if region:  base_params["region"]  = region
    if owner:   base_params["owner"]   = owner
    base_params["range"] = range
    if start: base_params["start"] = start
    if end:   base_params["end"] = end
    # Trend pills round-trip via URL; include them in the carry dict so
    # other pills don't drop them on click.
    base_params["trend1"] = trend1
    if trend2: base_params["trend2"] = trend2
    base_params["tsm"] = tsm

    base_url = "/station-analysis"
    selectors = _render_selector_strip(
        _render_range_pills(base_url, range,
                            {k: v for k, v in base_params.items()
                             if k not in ("range", "start", "end")},
                            start=start or "", end=end or "",
                            options=STATION_RANGE_OPTIONS),
    ) if duids else ""

    content = _station_content(duids, kind, label, meta, range, start, end,
                               trend1=trend1, trend2=trend2, tsm=tsm,
                               carry_params=base_params)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /futures — ASX electricity base-load futures (weekly settlement)
# ----------------------------------------------------------------------------
#
# Source: futures.csv (ASX Energy weekly base-load settlement prices). Four
# region series — NSW / QLD / SA / VIC — across quarterly contracts. Mirrors
# production's futures_tab.py with 4 chart cards on a single scrolling page
# instead of subtabs (consistent with how Gas was built).

import re as _re

FUTURES_CSV = Path("/Users/davidleitch/aemo_production/data/futures.csv")
FUTURES_REGIONS = ["NSW", "QLD", "SA", "VIC"]
FUTURES_REGION_COLORS = {
    "NSW": "#205EA6",   # FLEXOKI blue
    "QLD": "#BC5215",   # FLEXOKI orange
    "SA":  "#24837B",   # FLEXOKI cyan
    "VIC": "#66800B",   # FLEXOKI green
}
_FUTURES_COL_RE = _re.compile(
    r"(?:ASX Energy Contract\s+)?(\w+)\s+(\d{4})\s+Q(\d)"
    r"(?:\s+Base Load Futures Price.*)?$"
)


def _futures_strip_col(col: str) -> str:
    """Strip the long ASX header to the canonical short form 'NSW 2026 Q2'."""
    m = _FUTURES_COL_RE.match(col)
    return f"{m.group(1)} {m.group(2)} Q{m.group(3)}" if m else col


def _load_futures_df() -> pd.DataFrame:
    """Load futures.csv into a DataFrame indexed by date with short column
    names. Returns empty df if the file is missing."""
    if not FUTURES_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(FUTURES_CSV, parse_dates=["Time (UTC+10)"])
    df = df.rename(columns={"Time (UTC+10)": "date"}).set_index("date")
    df.columns = [_futures_strip_col(c) for c in df.columns]
    return df


def _parse_futures_contracts(columns) -> dict:
    """Return {region: {(year, quarter): canonical_column_name}}."""
    mapping: dict = {}
    pat = _re.compile(r"(\w+)\s+(\d{4})\s+Q(\d)")
    for col in columns:
        m = pat.match(col)
        if m:
            region, year, quarter = m.group(1), int(m.group(2)), int(m.group(3))
            mapping.setdefault(region, {})[(year, quarter)] = col
    return mapping


def _load_futures_spot_weekly() -> dict:
    """Weekly mean spot price per region for the 'futures vs spot' chart."""
    df = q("""
        SELECT date_trunc('week', settlementdate) AS week,
               REPLACE(regionid, '1', '') AS region,
               AVG(rrp) AS avg_price
        FROM prices_30min
        WHERE regionid IN ('NSW1','QLD1','SA1','VIC1')
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if df.empty:
        return {}
    out = {}
    for r in FUTURES_REGIONS:
        sub = df[df["region"] == r].set_index("week")["avg_price"]
        out[r] = sub
    return out


def _futures_quarter_label(year: int, q: int) -> str:
    return f"{year} Q{q}"


# ── Chart builders ──────────────────────────────────────────────────────────

def _build_forward_curve(futures_df: pd.DataFrame,
                         contracts: dict, region: str,
                         range_label: str) -> str:
    """Snapshot of {today, 3 months ago, 1 year ago} forward prices across
    each quarterly contract in the future. Shows how the forward curve has
    moved."""
    today = futures_df.index[-1]
    now_year, now_quarter = today.year, (today.month - 1) // 3 + 1
    snapshots = [
        ("Today",         today,                     "#205EA6", "solid"),
        ("3 months ago",  today - timedelta(days=91), "#BC5215", "dash"),
        ("1 year ago",    today - timedelta(days=365),"#24837B", "dot"),
    ]
    region_contracts = contracts.get(region, {})
    future_keys = sorted(
        [(y, qq) for y, qq in region_contracts
         if (y, qq) >= (now_year, now_quarter)])

    fig = go.Figure()
    for label, snap_date, color, dash in snapshots:
        idx = futures_df.index.searchsorted(snap_date)
        idx = min(idx, len(futures_df) - 1)
        row = futures_df.iloc[idx]
        actual_date = futures_df.index[idx]
        xs, ys = [], []
        for y, qq in future_keys:
            col = region_contracts[(y, qq)]
            v = row.get(col)
            if pd.notna(v):
                xs.append(_futures_quarter_label(y, qq))
                ys.append(v)
        if not xs:
            continue
        fig.add_trace(go.Scatter(
            x=xs, y=ys, mode="lines+markers",
            name=f"{label} ({actual_date.strftime('%d %b %Y')})",
            line=dict(color=color, width=2.5, dash=dash),
            marker=dict(size=7),
            hovertemplate="%{x}: $%{y:.1f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=440,
        margin=dict(l=60, r=24, t=20, b=60),
        legend=dict(orientation="h", yanchor="bottom", y=-0.20,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        xaxis=dict(showgrid=False, tickfont=dict(color=INK)),
        yaxis=dict(title=dict(text="$/MWh",
                              font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    div_id = f"plot-fwd-curve-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"{region} forward curve")
            + f'<div id="{div_id}" style="height:440px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0">'
              f'Three snapshots of the {region} forward curve — today vs '
              f'3 months ago vs 1 year ago. X-axis = contract quarter.</p>'
            + _attribution("Global-Roam"))


def _cal_year_average(df: pd.DataFrame, contracts: dict, year: int
                      ) -> pd.Series:
    """Average of Q1-Q4 forward prices for a given calendar year."""
    cols = [contracts.get((year, qq)) for qq in range(1, 5)]
    cols = [c for c in cols if c is not None and c in df.columns]
    if not cols:
        return pd.Series(np.nan, index=df.index)
    return df[cols].mean(axis=1)


def _build_forward_expectations(futures_df: pd.DataFrame,
                                contracts: dict, region: str) -> str:
    """Cal+1 and Cal+2 forward averages over time. Both lines share the
    chart; endpoint markers carry the value as text."""
    region_contracts = contracts.get(region, {})
    today = futures_df.index[-1]
    cal1_year = today.year + 1
    cal2_year = today.year + 2
    cal1 = _cal_year_average(futures_df, region_contracts, cal1_year)
    cal2 = _cal_year_average(futures_df, region_contracts, cal2_year)
    cutoff = pd.Timestamp("2024-01-01")
    cal1 = cal1[cal1.index >= cutoff].dropna()
    cal2 = cal2[cal2.index >= cutoff].dropna()

    fig = go.Figure()
    for series, label, color in [
        (cal1, f"Cal {cal1_year} (Cal+1)", "#205EA6"),
        (cal2, f"Cal {cal2_year} (Cal+2)", "#BC5215"),
    ]:
        if series.empty:
            continue
        fig.add_trace(go.Scatter(
            x=series.index, y=series.values, mode="lines",
            name=label, line=dict(color=color, width=2.5),
            hovertemplate="%{x|%d %b %Y}: $%{y:.1f}/MWh<extra></extra>",
        ))
        for pt_idx in (0, -1):
            fig.add_trace(go.Scatter(
                x=[series.index[pt_idx]], y=[float(series.values[pt_idx])],
                mode="markers+text",
                marker=dict(size=8, color=color,
                            line=dict(color="white", width=1.5)),
                text=[f"${float(series.values[pt_idx]):.0f}"],
                textposition=("middle right" if pt_idx == -1
                              else "middle left"),
                textfont=dict(color=color, size=11),
                showlegend=False, hoverinfo="skip",
            ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=420,
        margin=dict(l=60, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        xaxis=dict(gridcolor=BORDER, gridwidth=0.5,
                   tickfont=dict(color=INK)),
        yaxis=dict(title=dict(text="$/MWh",
                              font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    div_id = f"plot-fwd-exp-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"{region} calendar-year forward averages")
            + f'<div id="{div_id}" style="height:420px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0">'
              f'Cal+1 = average of next calendar year\'s four quarterly '
              f'contracts; Cal+2 = same for the year after. Tracks how the '
              f'forward market reprices over time.</p>'
            + _attribution("Global-Roam"))


def _build_futures_vs_spot(futures_df: pd.DataFrame, contracts: dict,
                           spot_weekly: dict, region: str) -> str:
    """Cal+2 forward vs trailing 12-month average spot. Shows whether the
    market is pricing forwards above or below recent realised."""
    region_contracts = contracts.get(region, {})
    today = futures_df.index[-1]
    cal2_year = today.year + 2
    cal2 = _cal_year_average(futures_df, region_contracts, cal2_year)
    cutoff = pd.Timestamp("2024-01-01")
    cal2 = cal2[cal2.index >= cutoff].dropna()

    fig = go.Figure()
    if not cal2.empty:
        fig.add_trace(go.Scatter(
            x=cal2.index, y=cal2.values, mode="lines",
            name=f"Cal {cal2_year} forward",
            line=dict(color="#205EA6", width=2.5),
            hovertemplate="%{x|%d %b %Y}: $%{y:.1f}/MWh<extra></extra>",
        ))
    spot = spot_weekly.get(region)
    if spot is not None and not spot.empty:
        trail = spot.rolling(52, min_periods=26).mean()
        trail = trail[trail.index >= cutoff].dropna()
        if not trail.empty:
            fig.add_trace(go.Scatter(
                x=trail.index, y=trail.values, mode="lines",
                name="Trailing 12-month spot",
                line=dict(color="#BC5215", width=2.5),
                hovertemplate="%{x|%d %b %Y}: $%{y:.1f}/MWh<extra></extra>",
            ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=420,
        margin=dict(l=60, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        xaxis=dict(gridcolor=BORDER, gridwidth=0.5,
                   tickfont=dict(color=INK)),
        yaxis=dict(title=dict(text="$/MWh",
                              font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    div_id = f"plot-fvs-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"{region} &middot; Cal+2 forward vs trailing spot")
            + f'<div id="{div_id}" style="height:420px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0">'
              f'Cal+2 forward (blue) is what the market expects two calendar '
              f'years out. Trailing 12-month spot (orange) is the 52-week '
              f'rolling realised. Spread = forward risk premium.</p>'
            + _attribution("Global-Roam + AEMO"))


def _build_single_contract(futures_df: pd.DataFrame, contracts: dict,
                           year: int, quarter: int) -> str:
    """All 4 regions on one chart for a specific contract. Shows regional
    spread over time for that quarter."""
    fig = go.Figure()
    for r in FUTURES_REGIONS:
        region_contracts = contracts.get(r, {})
        col = region_contracts.get((year, quarter))
        if col is None or col not in futures_df.columns:
            continue
        series = futures_df[col].dropna()
        if series.empty:
            continue
        fig.add_trace(go.Scatter(
            x=series.index, y=series.values, mode="lines",
            name=r,
            line=dict(color=FUTURES_REGION_COLORS[r], width=2.5),
            hovertemplate=f"{r}: $%{{y:.1f}}/MWh<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=440,
        margin=dict(l=60, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
        xaxis=dict(gridcolor=BORDER, gridwidth=0.5,
                   tickfont=dict(color=INK)),
        yaxis=dict(title=dict(text="$/MWh",
                              font=dict(size=11, color=MUTED)),
                   gridcolor=BORDER, gridwidth=0.5, zeroline=False),
    )
    div_id = f"plot-single-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"{year} Q{quarter} base load &middot; all regions")
            + f'<div id="{div_id}" style="height:440px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0">'
              f'How NSW/QLD/SA/VIC traded for the {year} Q{quarter} contract '
              f'as it approached delivery. Spread between regions reflects '
              f'interconnector capacity + dispatch expectations.</p>'
            + _attribution("Global-Roam"))


# ── Selectors ───────────────────────────────────────────────────────────────

def _render_futures_region_pills(base_url: str, active: str,
                                 other_params: dict) -> str:
    pills = []
    for r in FUTURES_REGIONS:
        params = dict(other_params, region=r)
        url = _build_url(base_url, **params)
        cls = "pill active" if r == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{r}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Region</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _render_futures_contract_select(base_url: str, active: str,
                                    contracts_sorted: list,
                                    other_params: dict) -> str:
    """Native <select> wrapped in a form. Too many quarterly contracts
    (~20) for a pill bar; dropdown is the right control. requestSubmit()
    fires HTMX on change so the swap happens without a button click."""
    hidden = "".join(
        f'<input type="hidden" name="{k}" value="{v}">'
        for k, v in other_params.items() if v
    )
    options = []
    for y, qq in contracts_sorted:
        slug = f"{y}-{qq}"
        sel = " selected" if slug == active else ""
        options.append(
            f'<option value="{slug}"{sel}>{y} Q{qq}</option>')
    return (
        f'<form class="pill-bar" hx-get="{base_url}" '
        f'hx-target="#tab-body" hx-push-url="true">'
        f'  {hidden}'
        f'  <span class="pill-bar-label">Contract</span>'
        f'  <select name="contract" class="contract-select" '
        f'          onchange="this.form.requestSubmit()" '
        f'          style="border:1px solid {BORDER};border-radius:14px;'
        f'                 padding:4px 12px;font-size:12px;'
        f'                 background:{PAPER};color:{INK};'
        f'                 font-family:inherit;cursor:pointer">'
        f'{"".join(options)}'
        f'  </select>'
        f'</form>'
    )


# ── Content + route ─────────────────────────────────────────────────────────

def _futures_content(region: str, contract: tuple) -> str:
    df = _load_futures_df()
    if df.empty:
        return ('<div class="placeholder"><p><strong>'
                'Futures data unavailable</strong></p>'
                '<p>futures.csv not found at the configured path.</p>'
                '</div>')
    contracts = _parse_futures_contracts(df.columns)
    spot = _load_futures_spot_weekly()
    range_label = (f"data through {df.index[-1].strftime('%d %b %Y')}")

    y, qq = contract
    fwd_curve_html = _build_forward_curve(df, contracts, region, range_label)
    fwd_exp_html = _build_forward_expectations(df, contracts, region)
    fvs_html = _build_futures_vs_spot(df, contracts, spot, region)
    single_html = _build_single_contract(df, contracts, y, qq)

    return ('<div class="prices-stack">'
            f'<div class="card">{fwd_curve_html}</div>'
            f'<div class="card">{fwd_exp_html}</div>'
            f'<div class="card">{fvs_html}</div>'
            f'<div class="card">{single_html}</div>'
            '</div>')


@app.get("/futures", response_class=HTMLResponse)
def futures_page(request: Request,
                 region: str = "NSW",
                 contract: str | None = None) -> HTMLResponse:
    if region not in FUTURES_REGIONS:
        region = "NSW"

    df = _load_futures_df()
    contracts = _parse_futures_contracts(df.columns) if not df.empty else {}
    all_keys = set()
    for rc in contracts.values():
        all_keys.update(rc.keys())
    contracts_sorted = sorted(all_keys)

    # Pick the default contract: ~1 year out (index -4 if we have enough).
    default_idx = max(0, len(contracts_sorted) - 4)
    default_key = (contracts_sorted[default_idx] if contracts_sorted
                   else (datetime.now().year, 1))

    # Parse the URL ?contract=YYYY-Q value.
    parsed_key = default_key
    if contract:
        try:
            y_str, q_str = contract.split("-")
            parsed_key = (int(y_str), int(q_str))
        except ValueError:
            parsed_key = default_key
        if parsed_key not in contracts_sorted and contracts_sorted:
            parsed_key = default_key

    contract_slug = f"{parsed_key[0]}-{parsed_key[1]}"

    base_params = {"region": region, "contract": contract_slug}
    base_url = "/futures"
    selectors = _render_selector_strip(
        _render_futures_region_pills(
            base_url, region,
            {k: v for k, v in base_params.items() if k != "region"}),
        _render_futures_contract_select(
            base_url, contract_slug, contracts_sorted,
            {k: v for k, v in base_params.items() if k != "contract"}),
    )
    content = _futures_content(region, parsed_key)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /batteries — pivot + lollipop with battery-specific economics
# ----------------------------------------------------------------------------
#
# Reuses the Generators pivot infrastructure (Tabulator dataTree in the
# shell) but with a battery-extended column set and a lollipop card below
# the table. Fleet view is delivered by the existing /station-analysis
# drill-down — clicking a battery name or group navigates there.
#
# Battery columns split the discharge/charge sides that the standard
# pivot collapses:
#   Discharge GWh / Charge GWh
#   Discharge Rev $M / Charge Cost $M
#   $/MWh Disch / $/MWh Chrg (volume-weighted)
#   Spread $/MWh  (discharge price - charge price; price-arbitrage delta)
#   $/MWh-cap/yr  (annualised spread revenue per MWh of storage — the
#                  investment metric: capex per MWh ÷ this number = years
#                  to payback before opex / degradation)
#
# Lollipop ranks individual batteries by the selected metric. Default
# metric is the per-MWh-storage annualised spread because that's the
# number an analyst sizes a build decision against.

BATTERY_COLUMNS_ALL = [
    "disch_gwh", "ch_gwh",
    "disch_rev", "ch_cost",
    "disch_price", "ch_price",
    "spread", "spread_yr",
    "util", "cap", "storage",
]
BATTERY_METRICS = [
    ("spread_yr",    "$/MWh-cap/yr"),
    ("spread",       "Spread $/MWh"),
    ("disch_rev",    "Disch Rev $M"),
    ("ch_cost",      "Charge Cost $M"),
    ("disch_price",  "Disch $/MWh"),
    ("ch_price",     "Charge $/MWh"),
    ("disch_gwh",    "Discharge GWh"),
    ("ch_gwh",       "Charge GWh"),
    ("util",         "Util %"),
]
BATTERY_TOPN_OPTIONS = [("10", "10"), ("20", "20"), ("50", "50"),
                        ("all", "All")]
# Field name on each row (matches the dict keys produced by _battery_agg_node).
BATTERY_METRIC_FIELD = {
    "spread_yr":   "spread_per_mwh_yr",
    "spread":      "spread_pmwh",
    "disch_rev":   "discharge_rev_m",
    "ch_cost":     "charge_cost_m",
    "disch_price": "discharge_price",
    "ch_price":    "charge_price",
    "disch_gwh":   "discharge_gwh",
    "ch_gwh":      "charge_gwh",
    "util":        "util",
}
BATTERY_METRIC_FMT = {
    "spread_yr":   "$",
    "spread":      "$",
    "disch_rev":   "$M",
    "ch_cost":     "$M",
    "disch_price": "$",
    "ch_price":    "$",
    "disch_gwh":   "GWh",
    "ch_gwh":      "GWh",
    "util":        "%",
}


def _battery_query(s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                   regions: list[str]) -> pd.DataFrame:
    """DUID-level battery aggregates for the window. Splits scada into
    discharge (>0) and charge (<0) sides so we can compute the asymmetric
    economics (revenue from selling, cost of buying)."""
    if not regions:
        return pd.DataFrame()
    region_in = ", ".join(f"'{r}'" for r in regions)
    sql = f"""
      WITH price_src AS (
        SELECT settlementdate, regionid, rrp
        FROM prices_30min
        WHERE regionid IN ({region_in})
      )
      SELECT
        COALESCE(NULLIF(d.region, ''), 'Unknown')        AS region,
        COALESCE(NULLIF(d.owner, ''), 'Unknown')         AS owner,
        COALESCE(NULLIF(d."site name", ''), g.duid)      AS station_name,
        g.duid                                           AS duid,
        FIRST(d.capacity_mw)                             AS capacity_mw,
        FIRST(d.storage_mwh)                             AS storage_mwh,
        SUM(GREATEST(g.scadavalue, 0)) * 0.5             AS discharge_mwh,
        SUM(GREATEST(-g.scadavalue, 0)) * 0.5            AS charge_mwh,
        SUM(GREATEST(g.scadavalue, 0)
            * COALESCE(p.rrp, 0) * 0.5)                  AS discharge_rev,
        SUM(GREATEST(-g.scadavalue, 0)
            * COALESCE(p.rrp, 0) * 0.5)                  AS charge_cost,
        COUNT(*)                                         AS n_intervals
      FROM generation_30min g
      LEFT JOIN duid_mapping d ON g.duid = d.duid
      LEFT JOIN price_src p
        ON g.settlementdate = p.settlementdate
        AND d.region = p.regionid
      WHERE g.settlementdate >= ? AND g.settlementdate < ?
        AND d.fuel = 'Battery Storage'
        AND COALESCE(NULLIF(d.region, ''), 'Unknown') IN ({region_in})
      GROUP BY d.region, d.owner, d."site name", g.duid
    """
    return q(sql, [s_ts, e_ts])


def _battery_agg_node(rows: pd.DataFrame, hours: float, label: str,
                      kind: str, ctx: dict) -> dict:
    """Aggregate one set of battery leaf rows into a tree node.

    Pricing: pool discharge_rev / charge_cost / mwh first, then derive
    per-MWh prices (volume-weighted). Spread is disch_price − chrg_price.
    Spread per MWh of storage per year is the investment-relevant metric;
    we annualise from the window length (window_hours / (365 * 24))."""
    disch_mwh = float(rows["discharge_mwh"].sum())
    ch_mwh    = float(rows["charge_mwh"].sum())
    disch_rev = float(rows["discharge_rev"].sum())
    ch_cost   = float(rows["charge_cost"].sum())
    storage   = float(rows["storage_mwh"].fillna(0).sum()) if kind != "duid" \
                else float(rows["storage_mwh"].iloc[0] or 0)
    cap_mw    = float(rows["capacity_mw"].fillna(0).sum()) if kind != "duid" \
                else float(rows["capacity_mw"].iloc[0] or 0)
    effective_cap = storage / 24.0  # one cycle/day equivalent MW
    disch_price = (disch_rev / disch_mwh) if disch_mwh > 0 else None
    ch_price = (ch_cost / ch_mwh) if ch_mwh > 0 else None
    spread = ((disch_price - ch_price)
              if (disch_price is not None and ch_price is not None) else None)
    spread_rev = disch_rev - ch_cost
    # Annualise: window_hours / (365 * 24) tells you what fraction of a year
    # this window covers; divide by that to scale up.
    year_hours = 365.0 * 24.0
    annual_spread_rev = (spread_rev * year_hours / hours) if hours > 0 else 0
    spread_per_mwh_yr = (annual_spread_rev / storage) if storage > 0 else None
    util_denom = effective_cap * hours
    util_pct = ((disch_mwh / util_denom * 100)
                if util_denom > 0 else None)
    n_duids = int(rows["duid"].nunique()) if kind != "duid" else 1
    return {
        "label":   label,
        "kind":    kind,
        "ctx":     ctx,
        "discharge_gwh":    round(disch_mwh / 1000.0),
        "charge_gwh":       round(ch_mwh / 1000.0),
        "discharge_rev_m":  round(disch_rev / 1_000_000.0),
        "charge_cost_m":    round(ch_cost / 1_000_000.0),
        "discharge_price":  round(disch_price) if disch_price is not None else None,
        "charge_price":     round(ch_price) if ch_price is not None else None,
        "spread_pmwh":      round(spread) if spread is not None else None,
        "spread_per_mwh_yr":(round(spread_per_mwh_yr)
                             if spread_per_mwh_yr is not None else None),
        "util":             round(util_pct) if util_pct is not None else None,
        "cap_mw":           round(effective_cap),
        "storage_mwh":      round(storage),
        # Mirror generic field names so the generic shell formatter can
        # still display generic metrics if requested.
        "gen_gwh":          round(disch_mwh / 1000.0),
        "rev_m":            round(disch_rev / 1_000_000.0),
        "price":            round(disch_price) if disch_price is not None else None,
        "n_items":          n_duids,
    }


def _build_battery_tree(df: pd.DataFrame, group_dims: list[str],
                        hours: float) -> list[dict]:
    """Like _build_pivot_tree but using the battery aggregator. Group dims
    are picked from Region / Owner (no Fuel since locked to Battery)."""
    if df.empty:
        return []
    def recurse(sub, remaining, ctx, depth):
        if not remaining:
            nodes = []
            for station, srows in sub.groupby("station_name", sort=False):
                station_ctx = dict(ctx, station=station)
                duid_children = []
                for _, drow in srows.sort_values(
                        "discharge_rev", ascending=False).iterrows():
                    duid_ctx = dict(station_ctx, duid=drow["duid"])
                    one = pd.DataFrame([drow])
                    duid_children.append(_battery_agg_node(
                        one, hours, drow["duid"], "duid", duid_ctx))
                node = _battery_agg_node(srows, hours, station, "station",
                                         station_ctx)
                node["_children"] = duid_children
                nodes.append(node)
            nodes.sort(key=lambda n: n["discharge_rev_m"], reverse=True)
            return nodes
        dim = remaining[0]
        rest = remaining[1:]
        nodes = []
        for key, grp in sub.groupby(dim, sort=False):
            sub_ctx = dict(ctx, **{dim: key})
            children = recurse(grp, rest, sub_ctx, depth + 1)
            node = _battery_agg_node(grp, hours, str(key), "group", sub_ctx)
            node["_children"] = children
            nodes.append(node)
        nodes.sort(key=lambda n: n["discharge_rev_m"], reverse=True)
        return nodes
    return recurse(df, group_dims, {}, 0)


def _battery_lollipop_chart(df: pd.DataFrame, hours: float, metric: str,
                            top_n: int) -> str:
    """Horizontal lollipop of individual batteries by selected metric.

    Per-DUID aggregation (the leaf level — group dimensions don't affect
    this chart). Sorted desc, top-N. Battery names on y-axis (horizontal
    saves us name-rotation headaches with 20+ entries)."""
    if df.empty:
        return ('<div class="placeholder">'
                'No battery data for the selected window / regions.</div>')

    # Aggregate each DUID into a node so we can read the chosen metric
    # field directly.
    nodes = []
    for _, drow in df.iterrows():
        node = _battery_agg_node(
            pd.DataFrame([drow]), hours, drow["station_name"], "duid",
            {"duid": drow["duid"], "region": drow["region"],
             "owner": drow["owner"]})
        node["duid"] = drow["duid"]
        node["region"] = drow["region"]
        node["station_name"] = drow["station_name"]
        nodes.append(node)

    field = BATTERY_METRIC_FIELD.get(metric, "spread_per_mwh_yr")
    label_lookup = dict(BATTERY_METRICS)
    metric_label = label_lookup.get(metric, metric)
    fmt = BATTERY_METRIC_FMT.get(metric, "")

    valued = [n for n in nodes if n.get(field) is not None]
    valued.sort(key=lambda n: n[field], reverse=True)
    if top_n > 0:
        valued = valued[:top_n]
    if not valued:
        return ('<div class="placeholder">'
                'No data for the selected metric.</div>')

    # Display name = station name (more readable than DUID code), but
    # tooltip carries both.
    names = [n["station_name"] for n in valued]
    duids = [n["duid"] for n in valued]
    values = [n[field] for n in valued]
    regions_arr = [n["region"] for n in valued]
    region_colors = {
        "NSW1": "#205EA6", "QLD1": "#BC5215", "VIC1": "#66800B",
        "SA1": "#24837B", "TAS1": "#5E409D",
    }
    bar_colors = [region_colors.get(r, "#878580") for r in regions_arr]

    # Plot in reverse so the largest is at the top (Plotly's y-axis grows
    # upward by default).
    names = names[::-1]
    duids = duids[::-1]
    values = values[::-1]
    bar_colors = bar_colors[::-1]
    regions_arr = regions_arr[::-1]

    fig = go.Figure()
    # Stems (lollipop line)
    for i, v in enumerate(values):
        fig.add_shape(
            type="line", x0=0, x1=v, y0=i, y1=i,
            line=dict(color="#c7c5b8", width=2),
        )
    # Dots
    fig.add_trace(go.Scatter(
        x=values, y=list(range(len(values))),
        mode="markers",
        marker=dict(size=14, color=bar_colors,
                    line=dict(color="#100F0F", width=0.5)),
        customdata=list(zip(duids, regions_arr)),
        hovertemplate=("<b>%{y}</b><br>DUID: %{customdata[0]}<br>"
                       "Region: %{customdata[1]}<br>"
                       f"{metric_label}: %{{x:,.0f}}<extra></extra>"),
    ))
    # Value labels
    for i, (v, n) in enumerate(zip(values, valued[::-1])):
        if fmt == "$":
            txt = f"${round(v):,}"
        elif fmt == "$M":
            txt = f"${round(v):,}M"
        elif fmt == "GWh":
            txt = f"{round(v):,}"
        elif fmt == "%":
            txt = f"{round(v)}%"
        else:
            txt = f"{round(v):,}"
        fig.add_annotation(
            x=v, y=i, text=txt, showarrow=False,
            xanchor="left", xshift=10,
            font=dict(size=10, color=INK),
        )

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=max(280, 22 * len(values) + 80),
        margin=dict(l=200, r=80, t=20, b=44),
        xaxis=dict(
            title=dict(text=metric_label, font=dict(size=11, color=MUTED)),
            gridcolor=BORDER, gridwidth=0.5, zeroline=True,
            zerolinecolor=BORDER,
        ),
        yaxis=dict(
            tickmode="array", tickvals=list(range(len(values))),
            ticktext=names,
            tickfont=dict(size=11, color=INK), showgrid=False,
            range=[-0.5, len(values) - 0.5],
        ),
        showlegend=False,
    )

    div_id = f"plot-batt-lolli-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"Battery ranking &middot; {metric_label} "
                     f"&middot; top {len(values)}")
            + f'<div id="{div_id}" style="height:{fig.layout.height}px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;'
              f'line-height:1.5"><strong>$/MWh-cap/yr</strong> = annualised '
              f'(discharge revenue − charge cost) ÷ storage MWh. Compare to '
              f'capex per MWh to size payback: e.g. a $500/kWh battery '
              f'($500,000/MWh) wanting a 10% return needs $50,000/MWh-cap/yr. '
              f'Marker colour = region.</p>'
            + _attribution())


# Pill helpers (Region multi-filter is reused from the generic helper).

def _render_battery_metric_pills(base_url: str, active: str,
                                 other_params: dict) -> str:
    pills = []
    for slug, label in BATTERY_METRICS:
        params = dict(other_params, metric=slug)
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Metric</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _render_battery_topn_pills(base_url: str, active: str,
                               other_params: dict) -> str:
    pills = []
    for slug, label in BATTERY_TOPN_OPTIONS:
        params = dict(other_params, topn=slug)
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Top N</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _batteries_content(g_dims: list[str], regions: list[str],
                       range_slug: str, start: str | None, end: str | None,
                       metric: str, top_n_slug: str) -> str:
    # Reuse the Generators range resolver (same window semantics).
    s_ts, e_ts, range_label = _pivot_range_window(range_slug, start, end)
    hours = max((e_ts - s_ts).total_seconds() / 3600.0, 1e-9)

    df = _battery_query(s_ts, e_ts, regions)
    if df.empty:
        return ('<div class="prices-stack"><div class="card">'
                + _card_h3(f"Batteries &middot; {range_label}")
                + '<p style="padding:14px;color:#878580">'
                  f'No battery data for the selected window / regions.</p>'
                + _attribution()
                + '</div></div>')

    tree = _build_battery_tree(df, g_dims, hours)
    tree_json = json.dumps(tree, default=lambda v: None)

    cols_for_pivot = BATTERY_COLUMNS_ALL  # always show every battery column
    cols_json = json.dumps(cols_for_pivot)
    range_q = json.dumps(range_slug)
    start_q = json.dumps(start or "")
    end_q   = json.dumps(end or "")

    title_chain = " → ".join(
        dict(PIVOT_DIMS)[d] for d in g_dims) or "—"
    n_duids = df["duid"].nunique()
    n_stations = df["station_name"].nunique()
    total_storage = float(df["storage_mwh"].fillna(0).sum())
    subtitle = (f"{title_chain} → Station → DUID &middot; "
                f"{n_stations:,} stations / {n_duids:,} battery DUIDs "
                f"&middot; {total_storage:,.0f} MWh total storage "
                f"&middot; {range_label} "
                f"({s_ts:%d %b %Y} → {e_ts:%d %b %Y})")

    pivot_html = f"""
<div id="pivot-tabulator" style="margin-top:8px"></div>
<script>
  window._pivotData = {tree_json};
  window._pivotCols = {cols_json};
  window._pivotRange = {range_q};
  window._pivotStart = {start_q};
  window._pivotEnd   = {end_q};
  if (typeof window._buildPivotTable === "function") {{
    requestAnimationFrame(window._buildPivotTable);
  }}
</script>
"""

    # Lollipop card uses the leaf-level df (per DUID), independent of the
    # pivot's group hierarchy.
    top_n = (10 if top_n_slug == "10"
             else 50 if top_n_slug == "50"
             else 0 if top_n_slug == "all"
             else 20)
    lollipop_html = _battery_lollipop_chart(df, hours, metric, top_n)

    return ('<div class="prices-stack">'
            '<div class="card">'
            + _card_h3(f"Batteries &middot; {range_label}")
            + f'<p style="color:{MUTED};font-size:12px;margin:0 0 8px 14px">'
              f'{subtitle}</p>'
            + pivot_html
            + f'<p style="color:{MUTED};font-size:11px;margin:10px 14px 0;'
              f'line-height:1.5">Click any name to drill into the time '
              f'series and TOD on Station Analysis. <strong>Spread '
              f'$/MWh-cap/yr</strong> is the headline economic metric — '
              f'annualised arbitrage revenue per MWh of storage capacity.'
              f'</p>'
            + _attribution()
            + '</div>'
            + '<div class="card">' + lollipop_html + '</div>'
            '</div>')


@app.get("/batteries", response_class=HTMLResponse)
def batteries_page(request: Request,
                   g1: str = "region", g2: str = "owner", g3: str = "",
                   range: str = "1y",
                   start: str | None = None, end: str | None = None,
                   region: str = "NSW1,QLD1,SA1,TAS1,VIC1",
                   metric: str = "spread_yr",
                   topn: str = "20") -> HTMLResponse:
    # Group dims: only Region / Owner (Fuel is locked).
    valid_dims = {"region", "owner"}
    g_dims: list[str] = []
    for slot in (g1, g2, g3):
        if slot in valid_dims and slot not in g_dims:
            g_dims.append(slot)
    if not g_dims:
        g_dims = ["region"]

    valid_ranges = {slug for slug, _ in PIVOT_RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "1y"

    region_list = [r for r in (region or "").split(",") if r in REGION_ORDER]
    if not region_list:
        region_list = REGION_ORDER[:]
    region_param = ",".join(r for r in REGION_ORDER if r in region_list)

    valid_metrics = {slug for slug, _ in BATTERY_METRICS}
    if metric not in valid_metrics:
        metric = "spread_yr"

    valid_topn = {slug for slug, _ in BATTERY_TOPN_OPTIONS}
    if topn not in valid_topn:
        topn = "20"

    base_params = {
        "g1": g_dims[0] if len(g_dims) > 0 else "",
        "g2": g_dims[1] if len(g_dims) > 1 else "",
        "g3": g_dims[2] if len(g_dims) > 2 else "",
        "range": range,
        "region": region_param,
        "metric": metric,
        "topn":   topn,
    }
    if start: base_params["start"] = start
    if end:   base_params["end"] = end

    base_url = "/batteries"
    # Limit grouping pills to Region/Owner only (fuel locked to Battery).
    batt_dims = [d for d in PIVOT_DIMS if d[0] != "fuel"]

    def _other(*excl: str) -> dict:
        return {k: v for k, v in base_params.items() if k not in excl}

    # Lightweight grouping pill: just Region / Owner / (none).
    def _battery_group_pill(slot, active, disabled):
        pills = []
        if slot >= 2:
            params = dict(_other(f"g{slot}"), **{f"g{slot}": ""})
            url = _build_url(base_url, **params)
            cls = "pill active" if not active else "pill"
            pills.append(f'<button class="{cls}" hx-get="{url}" '
                         f'hx-target="#tab-body" hx-push-url="true">(none)</button>')
        for slug, label in batt_dims:
            is_active = slug == active
            is_disabled = slug in disabled and not is_active
            if is_disabled:
                pills.append(f'<button class="pill disabled" disabled>'
                             f'{label}</button>')
                continue
            params = dict(_other(f"g{slot}"), **{f"g{slot}": slug})
            url = _build_url(base_url, **params)
            cls = "pill active" if is_active else "pill"
            pills.append(f'<button class="{cls}" hx-get="{url}" '
                         f'hx-target="#tab-body" hx-push-url="true">'
                         f'{label}</button>')
        return (f'<div class="pill-bar">'
                f'<span class="pill-bar-label">Group {slot}</span>'
                f'<div class="pill-group">{"".join(pills)}</div>'
                f'</div>')

    selectors = _render_selector_strip(
        _battery_group_pill(1, g_dims[0] if g_dims else None, set()),
        _battery_group_pill(2, g_dims[1] if len(g_dims) > 1 else None,
                            {g_dims[0]} if g_dims else set()),
        _render_pivot_range_pills(base_url, range,
                                  _other("range", "start", "end"),
                                  start=start or "", end=end or ""),
        _render_region_pills(base_url, region_param,
                             _other("region"), multi=True),
        _render_battery_metric_pills(base_url, metric, _other("metric")),
        _render_battery_topn_pills(base_url, topn, _other("topn")),
    )
    content = _batteries_content(g_dims, region_list, range, start, end,
                                 metric, topn)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# /curtailment — UIGF-based curtailment with econ/grid classification
# ----------------------------------------------------------------------------
#
# Data:
#   curtailment_regional5 (settlementdate, regionid, solar_uigf, solar_cleared,
#                          solar_curtailment, wind_uigf, wind_cleared,
#                          wind_curtailment, total_curtailment) at 5-min cadence
#   curtailment_duid5      (settlementdate, duid, uigf, totalcleared,
#                          curtailment) at 5-min cadence
#
# Curtailment = UIGF (unconstrained intermittent generation forecast) −
# cleared dispatch. Classified by joining each interval to that region's
# RRP: rrp ≤ 0 → "economic" (oversupply/negative pricing made the plant
# stop voluntarily or be wound down by the dispatch optimiser), rrp > 0 →
# "grid" (forced curtailment despite a positive price, typically because
# of transmission constraints).
#
# Replaces a broken production tab where curtailment_tab.py drifted out of
# sync with the query manager API (calling renamed methods, missing args,
# expecting renamed columns).

CURTAIL_FUELS = [("Solar", "Solar"), ("Wind", "Wind")]
CURTAIL_TOPN_OPTIONS = [("10", "10"), ("20", "20"), ("50", "50"),
                        ("all", "All")]
CURTAIL_ECON_COLOR = "#BC5215"   # Flexoki orange
CURTAIL_GRID_COLOR = "#A02F6F"   # Flexoki magenta
CURTAIL_SOLAR_COLOR = "#D4A000"
CURTAIL_WIND_COLOR  = "#66800B"


def _curtailment_resolution(span: timedelta) -> tuple[str, str, float, str]:
    """(resolution_label, sql_truncation, hours_factor, time_expression).
    Auto-pick based on span:
      ≤7 days   → 5-min source, no rollup
      ≤30 days  → 30-min via date_trunc('hour') + half-hour bucket
      >30 days  → daily via date_trunc('day')

    Returns (label, time_expr, hours_factor, sql_alias). time_expr is the
    SQL expression that produces the bucket timestamp."""
    if span <= timedelta(days=7):
        return ("5-min", "settlementdate", 5.0 / 60.0, "settlementdate")
    if span <= timedelta(days=30):
        # 30-min buckets via half-hour floor on MINUTE
        expr = (
            "date_trunc('hour', settlementdate) + "
            "INTERVAL '30 minutes' * "
            "FLOOR(EXTRACT(MINUTE FROM settlementdate) / 30)"
        )
        return ("30-min", expr, 0.5, "bucket")
    return ("daily", "date_trunc('day', settlementdate)", 24.0, "bucket")


def _load_curtailment_timeseries(s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                                 regions: list[str], fuels: list[str]
                                 ) -> tuple[pd.DataFrame, str]:
    """Time series of curtailment MW broken down by fuel × classification.

    Classification (econ vs grid) is decided per-interval per-region from
    that region's RRP at the same timestamp. We join prices_5min for the
    raw 5-min source and aggregate the curtailment alongside the price-
    derived classification flag.
    """
    if not regions:
        return pd.DataFrame(), "5-min"
    span = e_ts - s_ts
    res_label, time_expr, hours_factor, alias = _curtailment_resolution(span)
    region_in = ", ".join(f"'{r}'" for r in regions)

    # Build per-interval rows with classification, then aggregate to the
    # chosen bucket. Each fuel keeps its own column so the chart can stack.
    solar_in = "'Solar'" in [f"'{x}'" for x in fuels]
    wind_in  = "'Wind'"  in [f"'{x}'" for x in fuels]
    solar_expr = "c.solar_curtailment" if "Solar" in fuels else "0"
    wind_expr  = "c.wind_curtailment"  if "Wind"  in fuels else "0"

    sql = f"""
      WITH paired AS (
        SELECT
          c.settlementdate, c.regionid,
          {solar_expr} AS solar_curt,
          {wind_expr}  AS wind_curt,
          COALESCE(p.rrp, 0) AS rrp
        FROM curtailment_regional5 c
        LEFT JOIN prices_5min p
          ON c.settlementdate = p.settlementdate
          AND c.regionid = p.regionid
        WHERE c.settlementdate >= ?
          AND c.settlementdate < ?
          AND c.regionid IN ({region_in})
      )
      SELECT
        {time_expr} AS {alias},
        SUM(CASE WHEN rrp <= 0 THEN solar_curt ELSE 0 END) AS solar_econ,
        SUM(CASE WHEN rrp >  0 THEN solar_curt ELSE 0 END) AS solar_grid,
        SUM(CASE WHEN rrp <= 0 THEN wind_curt  ELSE 0 END) AS wind_econ,
        SUM(CASE WHEN rrp >  0 THEN wind_curt  ELSE 0 END) AS wind_grid,
        COUNT(*) AS n_rows
      FROM paired
      GROUP BY {time_expr}
      ORDER BY {alias}
    """
    df = q(sql, [s_ts, e_ts])
    if df.empty:
        return df, res_label
    # Bucket counts intervals across regions; mean per-region MW is the sum
    # divided by # regions present in the bucket. For an aggregation that
    # represents *concurrent* curtailment, the AVG-style behaviour is what
    # the chart should show. But for daily we want MWh (∑ MW × hours), and
    # for sub-day we want mean MW. Keep the SUM here and let the chart
    # function decide via the resolution flag.
    return df, res_label


def _curtailment_aggregate_to_chart(df: pd.DataFrame, resolution: str,
                                    regions: list[str]) -> pd.DataFrame:
    """Normalise SUM(curtailment) per bucket into a chart-ready series.

    5-min and 30-min charts show mean MW per region (so the y-axis is in
    MW units of curtailment summed across regions but representative of
    typical co-incident curtailment). Daily charts show MWh per day —
    the sum of MW × hours = SUM(MW) × (5/60) for 5-min source.
    """
    if df.empty:
        return df
    out = df.copy()
    if resolution == "daily":
        # SUM(MW) over 5-min intervals × (5/60 h/interval) = MWh
        # Each settlementdate has one row per region — but our query SUMs
        # across regions before the daily bucket, so SUM(MW) per day is
        # the sum across (regions × intervals). To get total MWh for the
        # day = SUM × (5/60). That's per-region-summed MWh — exactly what
        # we want.
        for col in ("solar_econ", "solar_grid", "wind_econ", "wind_grid"):
            out[col] = out[col] * 5.0 / 60.0
        out["unit"] = "MWh"
    else:
        # Mean MW: SUM(MW) / n_intervals (where n = # regions × intervals
        # in the bucket). Since the regions param can pick a subset, we
        # divide by row count which is n_intervals × n_regions.
        n_regions = max(len(regions), 1)
        intervals_per_bucket = 1 if resolution == "5-min" else 6  # 6×5min=30min
        per_bucket = intervals_per_bucket * n_regions
        for col in ("solar_econ", "solar_grid", "wind_econ", "wind_grid"):
            out[col] = out[col] / per_bucket
        out["unit"] = "MW"
    return out


def _curtailment_stats(s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                       regions: list[str], fuels: list[str]) -> dict:
    """Headline stats: total curtailment MWh + rate vs UIGF + peak MW +
    # of distinct DUIDs that experienced curtailment in the window."""
    if not regions:
        return {}
    region_in = ", ".join(f"'{r}'" for r in regions)
    solar_curt = "c.solar_curtailment" if "Solar" in fuels else "0"
    solar_uigf = "c.solar_uigf"        if "Solar" in fuels else "0"
    wind_curt  = "c.wind_curtailment"  if "Wind" in fuels else "0"
    wind_uigf  = "c.wind_uigf"         if "Wind" in fuels else "0"

    main = q(f"""
      WITH paired AS (
        SELECT
          c.settlementdate, c.regionid,
          {solar_curt} + {wind_curt} AS curt_mw,
          {solar_uigf} + {wind_uigf} AS uigf_mw,
          COALESCE(p.rrp, 0) AS rrp
        FROM curtailment_regional5 c
        LEFT JOIN prices_5min p
          ON c.settlementdate = p.settlementdate
          AND c.regionid = p.regionid
        WHERE c.settlementdate >= ?
          AND c.settlementdate < ?
          AND c.regionid IN ({region_in})
      )
      SELECT
        SUM(curt_mw) * 5.0/60.0 AS total_curt_mwh,
        SUM(uigf_mw) * 5.0/60.0 AS total_uigf_mwh,
        SUM(CASE WHEN rrp <= 0 THEN curt_mw ELSE 0 END) * 5.0/60.0
          AS econ_mwh,
        SUM(CASE WHEN rrp >  0 THEN curt_mw ELSE 0 END) * 5.0/60.0
          AS grid_mwh,
        MAX(curt_mw) AS peak_mw
      FROM paired
    """, [s_ts, e_ts])
    if main.empty:
        return {}
    n_duids = q(f"""
      SELECT COUNT(DISTINCT c.duid) AS n
      FROM curtailment_duid5 c
      LEFT JOIN duid_mapping d ON c.duid = d.duid
      WHERE c.settlementdate >= ?
        AND c.settlementdate < ?
        AND COALESCE(NULLIF(d.region, ''), 'Unknown') IN ({region_in})
        AND d.fuel IN ({", ".join(f"'{f}'" for f in fuels)})
        AND c.curtailment > 0
    """, [s_ts, e_ts])
    row = main.iloc[0]
    total = float(row["total_curt_mwh"] or 0)
    uigf  = float(row["total_uigf_mwh"] or 0)
    return {
        "total_mwh":  total,
        "rate_pct":   (total / uigf * 100) if uigf > 0 else None,
        "econ_mwh":   float(row["econ_mwh"] or 0),
        "grid_mwh":   float(row["grid_mwh"] or 0),
        "peak_mw":    float(row["peak_mw"] or 0),
        "n_duids":    int(n_duids["n"].iloc[0] or 0) if not n_duids.empty else 0,
    }


def _load_curtailment_regional_summary(s_ts: pd.Timestamp,
                                       e_ts: pd.Timestamp,
                                       regions: list[str]
                                       ) -> pd.DataFrame:
    """Per-region summary table: solar/wind curt + econ/grid split + rates."""
    if not regions:
        return pd.DataFrame()
    region_in = ", ".join(f"'{r}'" for r in regions)
    sql = f"""
      WITH paired AS (
        SELECT
          c.settlementdate, c.regionid,
          c.solar_curtailment, c.wind_curtailment,
          c.solar_uigf, c.wind_uigf,
          COALESCE(p.rrp, 0) AS rrp
        FROM curtailment_regional5 c
        LEFT JOIN prices_5min p
          ON c.settlementdate = p.settlementdate
          AND c.regionid = p.regionid
        WHERE c.settlementdate >= ?
          AND c.settlementdate < ?
          AND c.regionid IN ({region_in})
      )
      SELECT
        regionid AS region,
        SUM(solar_curtailment + wind_curtailment) * 5.0/60.0 AS total_curt_mwh,
        SUM(solar_curtailment) * 5.0/60.0 AS solar_curt_mwh,
        SUM(wind_curtailment)  * 5.0/60.0 AS wind_curt_mwh,
        SUM(CASE WHEN rrp <= 0
                 THEN solar_curtailment + wind_curtailment
                 ELSE 0 END) * 5.0/60.0 AS econ_mwh,
        SUM(CASE WHEN rrp >  0
                 THEN solar_curtailment + wind_curtailment
                 ELSE 0 END) * 5.0/60.0 AS grid_mwh,
        SUM(solar_uigf + wind_uigf) * 5.0/60.0 AS total_uigf_mwh,
        SUM(solar_uigf) * 5.0/60.0 AS solar_uigf_mwh,
        SUM(wind_uigf)  * 5.0/60.0 AS wind_uigf_mwh
      FROM paired
      GROUP BY regionid
      ORDER BY total_curt_mwh DESC
    """
    df = q(sql, [s_ts, e_ts])
    if df.empty:
        return df
    df["curt_rate_pct"] = np.where(
        df["total_uigf_mwh"] > 0,
        df["total_curt_mwh"] / df["total_uigf_mwh"] * 100, np.nan)
    df["solar_rate_pct"] = np.where(
        df["solar_uigf_mwh"] > 0,
        df["solar_curt_mwh"] / df["solar_uigf_mwh"] * 100, np.nan)
    df["wind_rate_pct"] = np.where(
        df["wind_uigf_mwh"] > 0,
        df["wind_curt_mwh"] / df["wind_uigf_mwh"] * 100, np.nan)
    return df


def _load_curtailment_top_duids(s_ts: pd.Timestamp, e_ts: pd.Timestamp,
                                regions: list[str], fuels: list[str],
                                top_n: int) -> pd.DataFrame:
    """Top-N DUIDs by curtailment MWh in the window. Joins to duid_mapping
    for station / region / fuel labels and to prices_5min for the
    econ/grid split per DUID."""
    if not regions or not fuels:
        return pd.DataFrame()
    region_in = ", ".join(f"'{r}'" for r in regions)
    fuel_in = ", ".join(f"'{f}'" for f in fuels)
    limit_clause = "" if top_n <= 0 else f"LIMIT {top_n}"
    sql = f"""
      WITH paired AS (
        SELECT
          c.duid,
          c.curtailment AS curt_mw,
          c.uigf,
          COALESCE(p.rrp, 0) AS rrp,
          d.region,
          d.fuel,
          d."site name" AS station_name
        FROM curtailment_duid5 c
        LEFT JOIN duid_mapping d ON c.duid = d.duid
        LEFT JOIN prices_5min p
          ON c.settlementdate = p.settlementdate
          AND d.region = p.regionid
        WHERE c.settlementdate >= ?
          AND c.settlementdate < ?
          AND COALESCE(NULLIF(d.region, ''), 'Unknown') IN ({region_in})
          AND d.fuel IN ({fuel_in})
      )
      SELECT
        duid, station_name, region, fuel,
        SUM(curt_mw) * 5.0/60.0 AS total_curt_mwh,
        SUM(uigf)   * 5.0/60.0 AS total_uigf_mwh,
        SUM(CASE WHEN rrp <= 0 THEN curt_mw ELSE 0 END) * 5.0/60.0 AS econ_mwh,
        SUM(CASE WHEN rrp >  0 THEN curt_mw ELSE 0 END) * 5.0/60.0 AS grid_mwh
      FROM paired
      GROUP BY duid, station_name, region, fuel
      HAVING SUM(curt_mw) > 0
      ORDER BY total_curt_mwh DESC
      {limit_clause}
    """
    df = q(sql, [s_ts, e_ts])
    if df.empty:
        return df
    df["curt_rate_pct"] = np.where(
        df["total_uigf_mwh"] > 0,
        df["total_curt_mwh"] / df["total_uigf_mwh"] * 100, np.nan)
    return df


# ── Rendering helpers ───────────────────────────────────────────────────────

def _render_curtailment_stats_strip(stats: dict, range_label: str) -> str:
    if not stats:
        return ""
    def _fmt(v, suffix=""):
        if v is None:
            return '<span style="color:#878580">—</span>'
        return f"{round(v):,}{suffix}"
    total_mwh = stats["total_mwh"]
    econ = stats["econ_mwh"]
    grid = stats["grid_mwh"]
    econ_pct = (econ / total_mwh * 100) if total_mwh > 0 else None
    grid_pct = (grid / total_mwh * 100) if total_mwh > 0 else None
    tiles = [
        ("Total curt MWh",   _fmt(total_mwh)),
        ("Rate %",           _fmt(stats["rate_pct"], "%")),
        ("Econ MWh",         _fmt(econ)),
        ("Econ % of curt",   _fmt(econ_pct, "%")),
        ("Grid MWh",         _fmt(grid)),
        ("Grid % of curt",   _fmt(grid_pct, "%")),
        ("Peak MW",          _fmt(stats["peak_mw"])),
        ("# DUIDs curtailed", _fmt(stats["n_duids"])),
    ]
    tile_html = "".join(
        f'<div style="background:{PAPER};border:1px solid {BORDER};'
        f'border-radius:6px;padding:8px 14px;min-width:96px">'
        f'<div style="font-size:10px;color:{MUTED};text-transform:uppercase;'
        f'letter-spacing:0.4px;font-weight:600">{label}</div>'
        f'<div style="font-size:18px;font-weight:600;color:{INK};'
        f'margin-top:2px">{val}</div>'
        f'</div>'
        for label, val in tiles
    )
    return (f'<div style="display:flex;flex-wrap:wrap;gap:8px;'
            f'margin:0 0 12px 0">{tile_html}</div>'
            f'<div style="font-size:11px;color:{MUTED};margin:0 0 8px 0">'
            f'{range_label} &middot; econ = curtailment when regional '
            f'RRP &le; $0/MWh (oversupply/negative pricing); grid = '
            f'curtailment when RRP &gt; $0/MWh (transmission or system '
            f'constraint despite positive price).</div>')


def _curtailment_ts_chart(df: pd.DataFrame, resolution: str,
                          fuels: list[str], range_label: str) -> str:
    """Stacked area chart. Up to 4 traces: solar_econ, solar_grid,
    wind_econ, wind_grid. Filtered to the selected fuels."""
    fig = go.Figure()
    if df.empty:
        fig.add_annotation(text="No curtailment data in this window",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False,
                           font=dict(size=14, color=MUTED))
    else:
        x_col = "bucket" if "bucket" in df.columns else "settlementdate"
        unit = "MWh" if resolution == "daily" else "MW"
        traces = []
        if "Solar" in fuels:
            traces.append(("solar_econ", "Solar · economic",
                           CURTAIL_SOLAR_COLOR, 0.5))
            traces.append(("solar_grid", "Solar · grid",
                           CURTAIL_SOLAR_COLOR, 1.0))
        if "Wind" in fuels:
            traces.append(("wind_econ", "Wind · economic",
                           CURTAIL_WIND_COLOR, 0.5))
            traces.append(("wind_grid", "Wind · grid",
                           CURTAIL_WIND_COLOR, 1.0))
        for col, name, color, alpha in traces:
            if col not in df.columns:
                continue
            fig.add_trace(go.Scatter(
                x=df[x_col], y=df[col],
                stackgroup="curt", mode="lines",
                line=dict(color=color, width=0.5),
                name=name,
                fillcolor=_rgba(color, alpha),
                hovertemplate=(f"{name}<br>%{{x|%d %b %Y}}<br>"
                               f"%{{y:.1f}} {unit}<extra></extra>"),
            ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=440,
        margin=dict(l=60, r=24, t=20, b=44),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11),
                    bgcolor=PAPER),
    )
    unit = "MWh/day" if resolution == "daily" else "MW (mean per region)"
    fig.update_xaxes(gridcolor=BORDER, gridwidth=0.5,
                     tickfont=dict(color=INK))
    fig.update_yaxes(
        title=dict(text=unit, font=dict(size=11, color=MUTED)),
        gridcolor=BORDER, gridwidth=0.5, zeroline=False,
    )

    div_id = f"plot-curt-ts-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    return (_card_h3(f"Curtailment over time &middot; {range_label} "
                     f"&middot; {resolution}")
            + f'<div id="{div_id}" style="height:440px"></div>'
            + f'<script>(function(){{var f={fig_json};'
              f'Plotly.newPlot("{div_id}",f.data,f.layout,'
              f'{PLOTLY_CFG});}})();</script>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;'
              f'line-height:1.5">Stacked by classification: economic (lighter '
              f'fill, RRP &le; $0) on the bottom, grid-constrained (darker '
              f'fill, RRP &gt; $0) on top. Solar = gold, Wind = green.</p>'
            + _attribution())


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _curtailment_regional_table(df: pd.DataFrame, range_label: str) -> str:
    if df.empty:
        return (_card_h3(f"Regional comparison &middot; {range_label}")
                + '<p style="padding:14px;color:#878580">No data.</p>'
                + _attribution())

    def _fmt_int(v):
        if v is None or pd.isna(v):
            return '<span style="color:#878580">—</span>'
        return f"{round(v):,}"

    def _fmt_pct(v):
        if v is None or pd.isna(v):
            return '<span style="color:#878580">—</span>'
        return f"{v:.1f}%"

    header_cells = [
        '<th style="text-align:left;padding:6px 12px">Region</th>',
        '<th style="text-align:right;padding:6px 12px">Total MWh</th>',
        '<th style="text-align:right;padding:6px 12px">Rate %</th>',
        '<th style="text-align:right;padding:6px 12px">Solar MWh</th>',
        '<th style="text-align:right;padding:6px 12px">Solar %</th>',
        '<th style="text-align:right;padding:6px 12px">Wind MWh</th>',
        '<th style="text-align:right;padding:6px 12px">Wind %</th>',
        '<th style="text-align:right;padding:6px 12px">Econ MWh</th>',
        '<th style="text-align:right;padding:6px 12px">Grid MWh</th>',
    ]
    rows = []
    for _, r in df.iterrows():
        rows.append(
            f'<tr style="border-bottom:1px solid {BORDER}">'
            f'<td style="padding:6px 12px;color:{INK};font-weight:500">'
            f'{r["region"]}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["total_curt_mwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_pct(r["curt_rate_pct"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["solar_curt_mwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_pct(r["solar_rate_pct"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["wind_curt_mwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_pct(r["wind_rate_pct"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["econ_mwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["grid_mwh"])}</td>'
            f'</tr>'
        )
    return (_card_h3(f"Regional comparison &middot; {range_label}")
            + '<table style="width:100%;border-collapse:collapse;'
              'font-size:13px">'
            + f'<thead style="background:{BORDER};color:{INK};'
              f'font-size:11px;text-transform:uppercase;letter-spacing:0.4px">'
            + f'<tr>{"".join(header_cells)}</tr></thead>'
            + f'<tbody>{"".join(rows)}</tbody></table>'
            + _attribution())


def _curtailment_topn_table(df: pd.DataFrame, range_label: str,
                            top_n_slug: str, range_slug: str,
                            start: str | None, end: str | None) -> str:
    if df.empty:
        return (_card_h3(f"Top curtailed DUIDs &middot; {range_label}")
                + '<p style="padding:14px;color:#878580">'
                  'No DUIDs were curtailed in this window.</p>'
                + _attribution())

    def _fmt_int(v):
        if v is None or pd.isna(v):
            return '<span style="color:#878580">—</span>'
        return f"{round(v):,}"

    def _fmt_pct(v):
        if v is None or pd.isna(v):
            return '<span style="color:#878580">—</span>'
        return f"{v:.1f}%"

    # Carry range through to station-analysis on click.
    qs_base = f"range={range_slug}"
    if range_slug == "custom":
        if start: qs_base += f"&start={start}"
        if end:   qs_base += f"&end={end}"

    header = (
        f'<tr style="background:{BORDER};color:{INK};font-size:11px;'
        f'text-transform:uppercase;letter-spacing:0.4px">'
        '<th style="text-align:left;padding:6px 12px">DUID</th>'
        '<th style="text-align:left;padding:6px 12px">Station</th>'
        '<th style="text-align:left;padding:6px 12px">Region</th>'
        '<th style="text-align:left;padding:6px 12px">Fuel</th>'
        '<th style="text-align:right;padding:6px 12px">Curt MWh</th>'
        '<th style="text-align:right;padding:6px 12px">Rate %</th>'
        '<th style="text-align:right;padding:6px 12px">Econ MWh</th>'
        '<th style="text-align:right;padding:6px 12px">Grid MWh</th>'
        '</tr>'
    )
    rows = []
    for _, r in df.iterrows():
        duid_link = (
            f'<a href="/station-analysis?duid={r["duid"]}&{qs_base}" '
            f'style="color:{TEAL};text-decoration:none;'
            f'font-family:ui-monospace,Menlo,monospace;font-size:12px">'
            f'{r["duid"]}</a>')
        rows.append(
            f'<tr style="border-bottom:1px solid {BORDER}">'
            f'<td style="padding:6px 12px">{duid_link}</td>'
            f'<td style="padding:6px 12px;color:{INK}">'
            f'{r["station_name"] or ""}</td>'
            f'<td style="padding:6px 12px;color:{MUTED}">'
            f'{(r["region"] or "")[:-1]}</td>'
            f'<td style="padding:6px 12px;color:{MUTED}">{r["fuel"] or ""}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["total_curt_mwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_pct(r["curt_rate_pct"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["econ_mwh"])}</td>'
            f'<td style="text-align:right;padding:6px 12px">'
            f'{_fmt_int(r["grid_mwh"])}</td>'
            f'</tr>'
        )
    return (_card_h3(f"Top curtailed DUIDs &middot; {range_label} "
                     f"&middot; top {top_n_slug}")
            + '<table style="width:100%;border-collapse:collapse;'
              'font-size:13px">'
            + f'<thead>{header}</thead>'
            + f'<tbody>{"".join(rows)}</tbody></table>'
            + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;'
              f'line-height:1.5">Click any DUID to open it in Station '
              f'Analysis. Rate % = curtailment ÷ UIGF. Window covers all '
              f'5-min intervals in the selected range.</p>'
            + _attribution())


# Pill helpers
def _render_curtailment_fuel_pills(base_url: str, active_fuels: list[str],
                                   other_params: dict) -> str:
    selected = set(active_fuels)
    pills = []
    for code, label in CURTAIL_FUELS:
        is_active = code in selected
        if is_active and len(selected) > 1:
            new_set = selected - {code}
        elif is_active:
            new_set = selected
        else:
            new_set = selected | {code}
        new_param = ",".join(c for c, _ in CURTAIL_FUELS if c in new_set)
        params = dict(other_params, fuel=new_param)
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Fuel</span>'
            f'<div class="pill-toggles">{"".join(pills)}</div>'
            f'</div>')


def _render_curtailment_topn_pills(base_url: str, active: str,
                                   other_params: dict) -> str:
    pills = []
    for slug, label in CURTAIL_TOPN_OPTIONS:
        params = dict(other_params, topn=slug)
        url = _build_url(base_url, **params)
        cls = "pill active" if slug == active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{label}</button>'
        )
    return (f'<div class="pill-bar">'
            f'<span class="pill-bar-label">Top N</span>'
            f'<div class="pill-group">{"".join(pills)}</div>'
            f'</div>')


def _curtailment_content(regions: list[str], fuels: list[str],
                         range_slug: str, start: str | None,
                         end: str | None, top_n_slug: str) -> str:
    # Reuse the Generators range resolver (matching range slug semantics).
    s_ts, e_ts, range_label = _pivot_range_window(range_slug, start, end)

    # Clamp start to when curtailment data actually begins.
    curt_min = q("SELECT MIN(settlementdate) AS ts FROM curtailment_regional5")
    if not curt_min.empty and curt_min["ts"].iloc[0] is not None:
        data_start = pd.Timestamp(curt_min["ts"].iloc[0])
        if s_ts < data_start:
            s_ts = data_start
            range_label = (f"{range_label} (data from {data_start:%d %b %Y})")

    if not fuels:
        return ('<div class="prices-stack"><div class="card">'
                + _card_h3("Curtailment")
                + '<p style="padding:14px;color:#878580">No fuels selected.</p>'
                + _attribution()
                + '</div></div>')

    stats = _curtailment_stats(s_ts, e_ts, regions, fuels)
    stats_html = _render_curtailment_stats_strip(stats, range_label)

    raw_ts, res_label = _load_curtailment_timeseries(s_ts, e_ts,
                                                    regions, fuels)
    chart_df = _curtailment_aggregate_to_chart(raw_ts, res_label, regions)
    ts_html = _curtailment_ts_chart(chart_df, res_label, fuels, range_label)

    regional_df = _load_curtailment_regional_summary(s_ts, e_ts, regions)
    regional_html = _curtailment_regional_table(regional_df, range_label)

    top_n = (10 if top_n_slug == "10"
             else 50 if top_n_slug == "50"
             else 0 if top_n_slug == "all"
             else 20)
    top_df = _load_curtailment_top_duids(s_ts, e_ts, regions, fuels, top_n)
    top_html = _curtailment_topn_table(top_df, range_label, top_n_slug,
                                        range_slug, start, end)

    return ('<div class="prices-stack">'
            '<div class="card">'
            + _card_h3(f"Curtailment &middot; {range_label}")
            + f'<p style="color:{MUTED};font-size:12px;margin:0 0 8px 14px">'
              f'UIGF − cleared dispatch, classified by regional RRP.</p>'
            + stats_html
            + ts_html
            + '</div>'
            + f'<div class="card">{regional_html}</div>'
            + f'<div class="card">{top_html}</div>'
            + '</div>')


@app.get("/curtailment", response_class=HTMLResponse)
def curtailment_page(request: Request,
                     range: str = "30d",
                     start: str | None = None,
                     end: str | None = None,
                     region: str = "NSW1,QLD1,SA1,TAS1,VIC1",
                     fuel: str = "Solar,Wind",
                     topn: str = "20") -> HTMLResponse:
    valid_ranges = {slug for slug, _ in PIVOT_RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "30d"

    region_list = [r for r in (region or "").split(",") if r in REGION_ORDER]
    if not region_list:
        region_list = REGION_ORDER[:]
    region_param = ",".join(r for r in REGION_ORDER if r in region_list)

    valid_fuels = {code for code, _ in CURTAIL_FUELS}
    fuel_list = [f for f in (fuel or "").split(",") if f in valid_fuels]
    if not fuel_list:
        fuel_list = [code for code, _ in CURTAIL_FUELS]
    fuel_param = ",".join(c for c, _ in CURTAIL_FUELS if c in fuel_list)

    valid_topn = {slug for slug, _ in CURTAIL_TOPN_OPTIONS}
    if topn not in valid_topn:
        topn = "20"

    base_params = {
        "range": range, "region": region_param,
        "fuel": fuel_param, "topn": topn,
    }
    if start: base_params["start"] = start
    if end:   base_params["end"] = end

    base_url = "/curtailment"
    def _other(*excl: str) -> dict:
        return {k: v for k, v in base_params.items() if k not in excl}

    selectors = _render_selector_strip(
        _render_pivot_range_pills(base_url, range,
                                  _other("range", "start", "end"),
                                  start=start or "", end=end or ""),
        _render_region_pills(base_url, region_param,
                             _other("region"), multi=True),
        _render_curtailment_fuel_pills(base_url, fuel_list, _other("fuel")),
        _render_curtailment_topn_pills(base_url, topn, _other("topn")),
    )
    content = _curtailment_content(region_list, fuel_list, range, start, end,
                                    topn)
    body = _render_tab_body("", selectors + content)
    if _is_htmx(request):
        return HTMLResponse(body)
    return HTMLResponse(_render_shell(body))


# ----------------------------------------------------------------------------
# Flat placeholder routes for the remaining tabs (anything not Today or Prices)
# ----------------------------------------------------------------------------

def _make_flat_route(slug: str, label: str):
    async def handler(request: Request) -> HTMLResponse:
        body = _render_tab_body("", _placeholder(label))
        if _is_htmx(request):
            return HTMLResponse(body)
        return HTMLResponse(_render_shell(body))
    handler.__name__ = f"{slug}_page"
    app.get(f"/{slug}", response_class=HTMLResponse)(handler)


for _slug, _label, _subs in TABS:
    if _slug in ("today", "prices", "generation-mix", "evening-peak",
                 "gas", "generators", "futures", "batteries",
                 "curtailment") or _subs:
        continue
    _make_flat_route(_slug, _label)


# ============================================================================
# Clock tile (the only "tile" served from the shell header itself)
# ============================================================================

@app.get("/tile/clock", response_class=HTMLResponse)
def clock() -> HTMLResponse:
    now = datetime.now(NEM_TZ)
    return HTMLResponse(f"Updated {now:%H:%M:%S} AEST")


# ============================================================================
# Tile 1: Renewable gauge  (stacked fuel breakdown + hour/all-time records)
# ============================================================================

def _load_records() -> dict:
    """Load the renewable records JSON. Returns {} if missing."""
    p = RECORDS_DIR / "renewable_records_calculated.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


@app.get("/tile/renewable-gauge", response_class=HTMLResponse)
def renewable_gauge() -> HTMLResponse:
    return _cached_tile("renewable-gauge", _build_renewable_gauge)


def _build_renewable_gauge() -> HTMLResponse:
    # Query scada5 + duid_mapping directly for the latest interval — the
    # equivalent generation_by_fuel_5min VIEW costs ~1.7 s because it
    # aggregates across all history before filtering. Direct table query
    # with a tight WHERE clause: ~20 ms.
    df = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM scada5)
        SELECT s.settlementdate,
               d.fuel AS fuel_type,
               SUM(s.scadavalue) AS mw
          FROM scada5 s
          JOIN duid_mapping d ON s.duid = d.duid, latest
         WHERE s.settlementdate = latest.ts
         GROUP BY s.settlementdate, d.fuel
    """)
    if df.empty:
        return HTMLResponse('<div style="color:%s">no data</div>' % MUTED)

    latest_ts = df["settlementdate"].iloc[0]
    df = _normalise_fuels(df)
    by_fuel = df.groupby("fuel_type")["mw"].sum().to_dict()
    rooftop = _rooftop_latest_mw()
    by_fuel["Rooftop Solar"] = rooftop
    total = sum(v for v in by_fuel.values() if v > 0)  # batteries can be negative
    fuel_pct = {f: (by_fuel.get(f, 0) / total * 100 if total else 0)
                for f in RENEWABLE_FUELS}
    renew_pct = sum(fuel_pct.values())

    # Stack the segments along the gauge arc.
    steps = []
    cum = 0.0
    for fuel in RENEWABLE_FUELS:
        seg = fuel_pct[fuel]
        if seg > 0:
            steps.append({"range": [cum, cum + seg],
                          "color": GAUGE_FUEL_COLORS[fuel]})
            cum += seg
    if cum < 100:
        steps.append({"range": [cum, 100], "color": "#eef0e3"})

    # Records lookup. The hour key is a string "0".."23".
    records = _load_records()
    hour_key = str(latest_ts.hour) if hasattr(latest_ts, "hour") else None
    hour_rec = records.get("hourly", {}).get(hour_key, {})
    all_rec = records.get("all_time", {}).get("renewable_pct", {})
    hour_val = hour_rec.get("value")
    all_val = all_rec.get("value")

    # Gauge: hollow bar so the stacked steps show; threshold needle marks the value.
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=renew_pct,
        number={"suffix": "%", "valueformat": ".0f",
                "font": {"size": 32, "color": INK}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1,
                     "tickcolor": MUTED,
                     "tickfont": {"size": 9, "color": MUTED}},
            "bar": {"color": "rgba(0,0,0,0)", "thickness": 0},
            "bgcolor": PAPER, "borderwidth": 0,
            "steps": steps,
            "threshold": {
                "line": {"color": INK, "width": 3},
                "thickness": 0.85,
                "value": renew_pct,
            },
        },
    ))
    fig.update_layout(paper_bgcolor=PAPER, height=170,
                      margin=dict(l=10, r=10, t=10, b=10))

    # Inline fuel legend (4 chips) + records strip.
    chips = " ".join(
        f'<span style="display:inline-flex;align-items:center;gap:4px;'
        f'font-size:11px;color:{INK};margin-right:10px">'
        f'<span style="width:10px;height:10px;background:{GAUGE_FUEL_COLORS[f]};'
        f'border-radius:2px"></span>{f.replace(" Solar","")} {fuel_pct[f]:.0f}%</span>'
        for f in RENEWABLE_FUELS if fuel_pct[f] > 0
    )
    rec_strip = ""
    if hour_val is not None or all_val is not None:
        parts = []
        if hour_val is not None:
            parts.append(f'hour record <strong>{hour_val:.0f}%</strong>')
        if all_val is not None:
            parts.append(f'all-time <strong>{all_val:.0f}%</strong>')
        rec_strip = (f'<div style="font-size:11px;color:{MUTED};'
                     f'margin-top:4px;text-align:center">'
                     + " &middot; ".join(parts) + '</div>')

    return _wrap_plot_with_extras(
        "renew-gauge", "Renewable share &middot; now", fig,
        extras=f'<div style="text-align:center;margin-top:4px">{chips}</div>{rec_strip}',
    )


# ============================================================================
# Tile 2: Generation mix  (real, 24h stacked area)
# ============================================================================

@app.get("/tile/generation-mix", response_class=HTMLResponse)
def generation_mix() -> HTMLResponse:
    return _cached_tile("generation-mix", _build_generation_mix)


def _build_generation_mix() -> HTMLResponse:
    # Direct scada5 + duid_mapping query for the past 24h. The view-based
    # equivalent (generation_by_fuel_5min) is ~80× slower because it
    # pre-aggregates across all history before the WHERE clause filters.
    df = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM scada5)
        SELECT s.settlementdate,
               d.fuel AS fuel_type,
               SUM(s.scadavalue) AS mw
          FROM scada5 s
          JOIN duid_mapping d ON s.duid = d.duid, latest
         WHERE s.settlementdate >= latest.ts - INTERVAL 24 HOUR
         GROUP BY s.settlementdate, d.fuel
         ORDER BY s.settlementdate
    """)
    if df.empty:
        return HTMLResponse('<div style="color:%s">no data</div>' % MUTED)

    df = _normalise_fuels(df)
    # Re-aggregate after collapsing CCGT/OCGT/Gas other → Gas.
    df = df.groupby(["settlementdate", "fuel_type"], as_index=False)["mw"].sum()

    # Merge rooftop (30-min, 5 regions → NEM total) onto the 5-min grid via ffill.
    # Filter to NEM_PHYSICAL_REGIONS to avoid the rooftop sub-region double-
    # counting bug (QLDC/QLDN/QLDS/TASN/TASS in pre-2026 data).
    roof = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM scada5)
        SELECT settlementdate, SUM(power) AS mw
          FROM rooftop30, latest
         WHERE settlementdate >= latest.ts - INTERVAL 24 HOUR
           AND regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')
         GROUP BY settlementdate
         ORDER BY settlementdate
    """)
    if not roof.empty:
        roof["fuel_type"] = "Rooftop Solar"
        # forward-fill 30-min onto 5-min timestamps
        all_ts = df["settlementdate"].drop_duplicates().sort_values()
        roof_full = (roof.set_index("settlementdate")[["mw"]]
                         .reindex(all_ts, method="ffill")
                         .dropna()
                         .reset_index())
        roof_full["fuel_type"] = "Rooftop Solar"
        df = pd.concat([df, roof_full], ignore_index=True)

    pivot = df.pivot(index="settlementdate", columns="fuel_type", values="mw").fillna(0)
    # Order traces so renewables stack on top of thermals
    order = ["Coal", "Gas", "Hydro", "Battery Storage",
             "Wind", "Solar", "Rooftop Solar", "Other"]
    fig = go.Figure()
    for fuel in order:
        if fuel not in pivot.columns:
            continue
        fig.add_trace(go.Scatter(
            x=pivot.index, y=pivot[fuel] / 1000,  # GW
            name=fuel, stackgroup="one", mode="lines",
            line=dict(width=0.5, color=FUEL_COLORS.get(fuel, MUTED)),
            fillcolor=FUEL_COLORS.get(fuel, MUTED),
            hovertemplate=f"{fuel}: %{{y:.2f}} GW<extra></extra>",
        ))
    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=240, margin=dict(l=40, r=8, t=8, b=80),
        legend=dict(orientation="h", yanchor="top", y=-0.28,
                    xanchor="center", x=0.5,
                    font=dict(size=9), bgcolor=PAPER),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED)),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   title=dict(text="GW", font=dict(size=10, color=MUTED))),
    )
    return _wrap_plot("gen-mix", "Generation mix · past 24h", fig)


# ============================================================================
# Tile 3: NEM demand gauge  (gauge vs hour-of-day record + state composition)
# ============================================================================

@app.get("/tile/demand-gauge", response_class=HTMLResponse)
def demand_gauge() -> HTMLResponse:
    """Single-needle gauge of NEM-wide demand (operational + rooftop) against
    this hour-of-day's all-time record. State composition shown as a thin
    horizontal stacked bar underneath — see explanation in the surrounding
    response prose for why states aren't stacked on the arc itself.
    """
    regions_sql = "('NSW1','QLD1','VIC1','SA1','TAS1')"

    # Current per-region demand+rooftop, plus the latest interval timestamp.
    cur = q(f"""
        WITH latest AS (
            SELECT MAX(settlementdate) AS ts FROM demand30
             WHERE regionid IN {regions_sql}
        )
        SELECT d.regionid,
               d.demand + COALESCE(r.power, 0) AS total_mw,
               latest.ts AS ts
          FROM demand30 d
          LEFT JOIN rooftop30 r
            ON d.settlementdate = r.settlementdate
           AND d.regionid = r.regionid,
               latest
         WHERE d.settlementdate = latest.ts
           AND d.regionid IN {regions_sql}
         ORDER BY d.regionid
    """)
    if cur.empty:
        return HTMLResponse(_card_h3("NEM demand") +
            f'<div style="color:{MUTED};padding:10px">no demand data</div>')

    latest_ts = cur["ts"].iloc[0]
    current_hour = int(latest_ts.hour)
    by_region = cur.set_index("regionid")["total_mw"].to_dict()
    total_mw = float(sum(by_region.values()))

    # Hour-of-day record across full history (this is the gauge maximum).
    hour_rec = q(f"""
        SELECT MAX(period_total) AS mw FROM (
            SELECT d.settlementdate,
                   SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
              FROM demand30 d
              LEFT JOIN rooftop30 r
                ON d.settlementdate = r.settlementdate
               AND d.regionid = r.regionid
             WHERE d.regionid IN {regions_sql}
               AND EXTRACT(HOUR FROM d.settlementdate) = {current_hour}
             GROUP BY d.settlementdate
        )
    """)
    hour_record_mw = float(hour_rec["mw"].iloc[0] or 0)

    # All-time record (text only; not used as gauge max).
    alltime = q(f"""
        SELECT MAX(period_total) AS mw FROM (
            SELECT d.settlementdate,
                   SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
              FROM demand30 d
              LEFT JOIN rooftop30 r
                ON d.settlementdate = r.settlementdate
               AND d.regionid = r.regionid
             WHERE d.regionid IN {regions_sql}
             GROUP BY d.settlementdate
        )
    """)
    alltime_record_mw = float(alltime["mw"].iloc[0] or 0)

    # Forecast peak from latest predispatch run (5-region sum, future-only).
    fc = q(f"""
        WITH latest_run AS (SELECT MAX(run_time) AS rt FROM predispatch)
        SELECT MAX(period_total) AS mw FROM (
            SELECT settlementdate, SUM(demand_forecast) AS period_total
              FROM predispatch, latest_run
             WHERE run_time = latest_run.rt
               AND regionid IN {regions_sql}
               AND settlementdate >= latest_run.rt
             GROUP BY settlementdate
        )
    """)
    forecast_peak_mw = float(fc["mw"].iloc[0] or 0)

    # Gauge scale: 0 GW to whichever is larger — hour record or current demand
    # (the latter only matters if today is a new record). 5% headroom above.
    gauge_max_mw = max(hour_record_mw, total_mw) * 1.02
    pct_of_hour = (total_mw / hour_record_mw * 100) if hour_record_mw else 0

    # Choose needle/bar color by percent-of-hour: cool when low, warm when high.
    if pct_of_hour < 70:
        needle_color = "#66800b"
    elif pct_of_hour < 90:
        needle_color = "#bc5215"
    else:
        needle_color = "#af3029"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=total_mw / 1000,
        number={"suffix": " GW", "valueformat": ".1f",
                "font": {"size": 30, "color": INK}},
        gauge={
            "shape": "angular",
            "axis": {"range": [0, gauge_max_mw / 1000],
                     "tickwidth": 0, "tickcolor": MUTED,
                     "tickfont": {"size": 9, "color": MUTED},
                     "nticks": 3},
            "bar": {"color": needle_color, "thickness": 0.25},
            "bgcolor": "#eef0e3",
            "borderwidth": 0,
            "threshold": {
                "line": {"color": INK, "width": 3},
                "thickness": 0.85,
                "value": total_mw / 1000,
            },
        },
    ))
    fig.update_layout(paper_bgcolor=PAPER, height=170,
                      margin=dict(l=10, r=10, t=10, b=0))

    # Horizontal stacked composition bar (NSW → QLD → VIC → SA → TAS).
    composition_order = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    segments = []
    for region in composition_order:
        mw = by_region.get(region, 0)
        if mw <= 0:
            continue
        pct = mw / total_mw * 100 if total_mw else 0
        segments.append(
            f'<div title="{region[:-1]} {mw:,.0f} MW ({pct:.0f}%)" '
            f'style="background:{REGION_COLORS[region]};width:{pct}%;'
            f'display:flex;align-items:center;justify-content:center;'
            f'color:#fffcf0;font-size:10px;font-weight:600;'
            f'min-width:0;overflow:hidden;white-space:nowrap">'
            f'{region[:-1] if pct >= 10 else ""}</div>'
        )
    comp_bar = (
        f'<div style="height:14px;display:flex;border-radius:3px;'
        f'overflow:hidden;margin:4px 12px 0">{"".join(segments)}</div>'
    )

    # Records strip — match the production layout.
    records_strip = (
        f'<div style="display:flex;justify-content:space-around;flex-wrap:wrap;'
        f'gap:8px;font-size:11px;color:{MUTED};padding:8px 4px 0">'
        f'<span>Now <strong style="color:{INK}">{total_mw/1000:.1f} GW</strong></span>'
        f'<span>{current_hour:02d}:00 record '
        f'<strong style="color:{INK}">{hour_record_mw/1000:.1f} GW</strong></span>'
        f'<span>All-time '
        f'<strong style="color:{INK}">{alltime_record_mw/1000:.1f} GW</strong></span>'
    )
    if forecast_peak_mw > 0:
        records_strip += (
            f'<span>Forecast peak '
            f'<strong style="color:{INK}">{forecast_peak_mw/1000:.1f} GW</strong></span>'
        )
    records_strip += "</div>"

    return _wrap_plot_with_extras(
        "demand-gauge", "NEM demand", fig,
        extras=comp_bar + records_strip,
    )


# ============================================================================
# Tile 4: Price chart  (LOESS-smoothed, last ~10 hours, spike-annotated)
# ============================================================================

Y_CAP = 1500  # clip line to this; mark the spike value as text


def _smooth_region(y: np.ndarray) -> np.ndarray:
    """LOESS frac=0.1 on non-NaN points; EWMA fallback if statsmodels missing.

    Matches the production matplotlib chart's smoothing so the visual is
    comparable. Raw 5-min numbers stay in the price table.
    """
    out = np.full(len(y), np.nan)
    valid = ~np.isnan(y)
    if valid.sum() < 5:
        return y
    x = np.arange(len(y))
    if HAS_LOESS and valid.sum() > 10:
        try:
            out[valid] = lowess(y[valid], x[valid], frac=0.1, it=0,
                                return_sorted=False)
            return out
        except Exception:
            pass
    return pd.Series(y).ewm(alpha=0.22).mean().to_numpy()


@app.get("/tile/price-chart", response_class=HTMLResponse)
def price_chart() -> HTMLResponse:
    """Smoothed 5-minute spot prices, last 120 points (~10h). Y-axis scales
    to the smoothed data so intra-period texture stays visible; raw spikes
    that exceed the smoothed range are annotated rather than clipped.
    """
    df = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM prices5)
        SELECT settlementdate, regionid, rrp
          FROM prices5, latest
         WHERE settlementdate >= latest.ts - INTERVAL '10 hours'
         ORDER BY settlementdate
    """)
    if df.empty:
        return HTMLResponse('<div style="color:%s">no data</div>' % MUTED)

    pivot = (df.pivot_table(index="settlementdate", columns="regionid",
                            values="rrp", aggfunc="mean")
               .sort_index().tail(120))
    latest_ts = pivot.index[-1]

    fig = go.Figure()
    for region in REGION_ORDER:
        if region not in pivot.columns:
            continue
        raw = pivot[region].to_numpy(dtype=float)
        smooth = _smooth_region(raw)

        latest_raw = raw[-1] if not np.isnan(raw[-1]) else np.nan
        legend_label = (f"{region}: ${latest_raw:.0f}" if not np.isnan(latest_raw)
                        else region)

        fig.add_trace(go.Scatter(
            x=pivot.index, y=smooth, name=legend_label, mode="lines",
            line=dict(color=REGION_COLORS[region], width=1.8),
            hovertemplate="%{x|%H:%M} $%{y:.0f}/MWh<extra></extra>",
        ))

    # No explicit y-range. The standalone display_spot.py plots smoothed values
    # and lets matplotlib autoscale — we do the same with Plotly. The smoothed
    # line already dampens single-interval spikes, so autoscale fits the
    # in-window swing tightly without flattening texture.
    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=240, margin=dict(l=44, r=8, t=10, b=24),
        legend=dict(orientation="v", yanchor="middle", y=0.5,
                    xanchor="left", x=1.005, font=dict(size=10),
                    bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   dtick=3600000, tickformat="%H:%M"),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   autorange=True,
                   title=dict(text="$/MWh", font=dict(size=10, color=MUTED))),
    )
    title = (f"Smoothed 5 minute prices as at {latest_ts:%-d %b %H:%M} "
             f"({len(pivot)} points)")
    return _wrap_plot("price-chart", title, fig)


# ============================================================================
# Tile 5: Price table  (real)
# ============================================================================

@app.get("/tile/price-table", response_class=HTMLResponse)
def price_table() -> HTMLResponse:
    """Last 5 intervals + hour and 24h averages. Latest row at the bottom,
    bolded. Numbers are clean integers (no $); region columns coloured."""
    df = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM prices5)
        SELECT settlementdate, regionid, rrp
          FROM prices5, latest
         WHERE settlementdate >= latest.ts - INTERVAL '25 hours'
         ORDER BY settlementdate DESC
    """)
    if df.empty:
        return HTMLResponse('<div style="color:%s">no data</div>' % MUTED)

    pivot = (df.pivot_table(index="settlementdate", columns="regionid",
                            values="rrp", aggfunc="mean")
               .sort_index().tail(5))
    cols = [c for c in REGION_ORDER if c in pivot.columns]
    latest_ts = pivot.index[-1]
    hour_avg = (df[df["settlementdate"] >= latest_ts - timedelta(hours=1)]
                .groupby("regionid")["rrp"].mean().round(0).to_dict())
    day_avg = df.groupby("regionid")["rrp"].mean().round(0).to_dict()

    def cell(v, bold=False):
        if v is None or pd.isna(v):
            return '<td style="text-align:right;padding:6px 10px">&mdash;</td>'
        color = "#af3029" if v >= 300 else ("#bc5215" if v >= 100 else INK)
        weight = "600" if bold else "400"
        return (f'<td style="color:{color};text-align:right;padding:6px 10px;'
                f'font-weight:{weight}">{v:,.0f}</td>')

    body_rows = []
    for i, (ts, row) in enumerate(pivot.iterrows()):
        bold = (ts == latest_ts)
        bg = "" if i % 2 == 0 else f"background:#faf8eb;"
        weight = "600" if bold else "500"
        body_rows.append(
            f'<tr style="{bg}border-bottom:1px solid {BORDER}">'
            f'<td style="text-align:right;padding:6px 10px;font-weight:{weight};'
            f'color:{INK if bold else MUTED}">{ts:%H:%M}</td>'
            + "".join(cell(row.get(c), bold=bold) for c in cols) + "</tr>"
        )

    def avg_row(label, d, top_border=True):
        border = f"border-top:2px solid {BORDER};" if top_border else ""
        return (f'<tr style="{border}background:#f7f5e8">'
                f'<td style="text-align:right;padding:8px 10px;font-weight:600;'
                f'color:{INK}">{label}</td>'
                + "".join(cell(d.get(c), bold=True) for c in cols) + "</tr>")

    header = "".join(
        f'<th style="text-align:right;color:{REGION_COLORS[c]};'
        f'padding:8px 10px;font-weight:600">{c}</th>'
        for c in cols
    )

    return HTMLResponse(
      _card_h3(f"5 minute spot $/MWh &middot; {latest_ts:%-d %b %H:%M}") + f"""
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="background:#f7f5e8">
            <th></th>{header}
          </tr>
        </thead>
        <tbody>
          {"".join(body_rows)}
          {avg_row("Last hour average", hour_avg)}
          {avg_row("Last 24 hr average", day_avg, top_border=False)}
        </tbody>
      </table>
    """)


# ============================================================================
# Tile 6: Battery SOC gauge  (real — mainland bdu5 stored energy + 1h marker)
# ============================================================================

def _load_battery_record_gwh() -> float | None:
    p = RECORDS_DIR / "battery_records_plugin.json"
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        v = data.get("nem", {}).get("soc_mwh", {}).get("value")
        return v / 1000 if v else None
    except Exception:
        return None


@app.get("/tile/battery-soc", response_class=HTMLResponse)
def battery_soc() -> HTMLResponse:
    """Stored energy across NEM mainland (TAS is NaN in bdu5).

    Bar = current SOC, dashed marker = SOC 1h ago, axis scaled to 30-day
    rolling max as a capacity proxy. All-time record overlaid as a faint
    second marker if higher than the 30-day max.
    """
    stats = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM bdu5),
        now_row AS (
            SELECT SUM(bdu_energy_storage) AS mwh
              FROM bdu5, latest
             WHERE settlementdate = latest.ts
               AND regionid IN ('NSW1','QLD1','VIC1','SA1')
        ),
        hour_ago AS (
            SELECT SUM(bdu_energy_storage) AS mwh
              FROM bdu5
             WHERE regionid IN ('NSW1','QLD1','VIC1','SA1')
               AND settlementdate = (
                  SELECT MAX(settlementdate) FROM bdu5
                   WHERE settlementdate <= (SELECT ts FROM latest) - INTERVAL '55 minutes'
               )
        ),
        max30 AS (
            SELECT MAX(total) AS mwh FROM (
                SELECT SUM(bdu_energy_storage) AS total
                  FROM bdu5, latest
                 WHERE regionid IN ('NSW1','QLD1','VIC1','SA1')
                   AND settlementdate >= latest.ts - INTERVAL '30 days'
                 GROUP BY settlementdate
            )
        )
        SELECT (SELECT mwh FROM now_row) / 1000 AS now_gwh,
               (SELECT mwh FROM hour_ago) / 1000 AS hour_ago_gwh,
               (SELECT mwh FROM max30)   / 1000 AS max30_gwh
    """)
    row = stats.iloc[0]
    now = float(row["now_gwh"] or 0)
    hour_ago = float(row["hour_ago_gwh"] or 0)
    max30 = float(row["max30_gwh"] or 11.0)
    record = _load_battery_record_gwh()
    axis_top = max(max30, record or 0, now) * 1.05

    # Horizontal bar with gradient red→green; grey tail for unfilled capacity;
    # markers for 1h-ago and all-time record.
    fig = go.Figure()
    # gradient via small stacked segments
    n_seg = 30
    for i in range(n_seg):
        frac = i / n_seg
        seg_start = frac * now
        seg_w = now / n_seg
        # red→orange→yellow→green ramp
        if frac < 0.33:
            t = frac / 0.33
            color = f"rgb({int(175*(1-t)+188*t)},{int(48*(1-t)+101*t)},{int(41*(1-t)+21*t)})"
        elif frac < 0.66:
            t = (frac - 0.33) / 0.33
            color = f"rgb({int(188*(1-t)+173*t)},{int(101*(1-t)+131*t)},{int(21*(1-t)+1*t)})"
        else:
            t = (frac - 0.66) / 0.34
            color = f"rgb({int(173*(1-t)+102*t)},{int(131*(1-t)+128*t)},{int(1*(1-t)+11*t)})"
        fig.add_trace(go.Bar(
            x=[seg_w], y=["SOC"], base=seg_start, orientation="h",
            marker=dict(color=color, line=dict(width=0)),
            hoverinfo="skip", showlegend=False,
        ))
    # grey tail
    if now < axis_top:
        fig.add_trace(go.Bar(
            x=[axis_top - now], y=["SOC"], base=now, orientation="h",
            marker=dict(color="#e8e6da", line=dict(width=0)),
            hoverinfo="skip", showlegend=False,
        ))

    shapes = []
    annotations = []
    # 1h-ago marker
    if abs(now - hour_ago) > 0.05 and hour_ago > 0:
        shapes.append(dict(type="line", x0=hour_ago, x1=hour_ago,
                           y0=-0.45, y1=0.45,
                           line=dict(color=MUTED, width=2, dash="dash")))
        annotations.append(dict(x=hour_ago, y=0.6, text="1h ago",
                                showarrow=False, yanchor="bottom",
                                font=dict(size=9, color=MUTED)))
    # all-time record marker
    if record and record > 0:
        shapes.append(dict(type="line", x0=record, x1=record,
                           y0=-0.45, y1=0.45,
                           line=dict(color=INK, width=1.5, dash="dot")))
        annotations.append(dict(x=record, y=-0.6, text=f"record {record:.1f}",
                                showarrow=False, yanchor="top",
                                font=dict(size=9, color=INK)))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        barmode="stack", height=140,
        margin=dict(l=10, r=10, t=10, b=30),
        xaxis=dict(range=[0, axis_top], showgrid=False, zeroline=False,
                   tickfont=dict(size=10, color=MUTED),
                   ticksuffix=" GWh"),
        yaxis=dict(visible=False),
        shapes=shapes, annotations=annotations, showlegend=False,
    )

    delta = now - hour_ago
    delta_color = "#66800b" if delta >= 0 else "#af3029"
    delta_arrow = "▲" if delta >= 0 else "▼"
    summary = (f'<div style="display:flex;justify-content:space-between;'
               f'align-items:baseline;margin-bottom:6px">'
               f'<span style="font-size:24px;font-weight:600;color:{INK}">'
               f'{now:.2f} <span style="font-size:13px;color:{MUTED};'
               f'font-weight:400">GWh stored</span></span>'
               f'<span style="font-size:12px;color:{delta_color}">'
               f'{delta_arrow} {abs(delta):.2f} GWh in 1h</span></div>')
    return _wrap_plot_with_extras(
        "batt-soc", "Battery SOC &middot; NEM mainland", fig,
        extras_before=summary,
    )


# ============================================================================
# Tile 7: Forecast chart  (predispatch prices, 5 regions, 12h, no smoothing)
# ============================================================================

@app.get("/tile/forecast-chart", response_class=HTMLResponse)
def forecast_chart() -> HTMLResponse:
    """5-region predispatch price forecast, 12h horizon, raw 30-min values.

    Visual sibling of the historic price chart — same styling, legend on
    right with the *next* interval's price per region. No smoothing
    (forecasts are already half-hourly and chunky).
    """
    df = q("""
        WITH latest AS (SELECT MAX(run_time) AS rt FROM predispatch)
        SELECT settlementdate, regionid, price_forecast
          FROM predispatch, latest
         WHERE run_time = latest.rt
           AND settlementdate >= latest.rt
           AND settlementdate <= latest.rt + INTERVAL '12 hours'
         ORDER BY settlementdate
    """)
    if df.empty:
        return HTMLResponse('<div style="color:%s">no forecast</div>' % MUTED)

    pivot = (df.pivot(index="settlementdate", columns="regionid",
                      values="price_forecast").sort_index())
    run_time = pivot.index[0]
    horizon_hrs = (pivot.index[-1] - run_time).total_seconds() / 3600

    fig = go.Figure()
    for region in REGION_ORDER:
        if region not in pivot.columns:
            continue
        y = pivot[region].to_numpy(dtype=float)
        next_val = y[0] if not np.isnan(y[0]) else np.nan
        label = (f"{region}: ${next_val:.0f}" if not np.isnan(next_val)
                 else region)
        fig.add_trace(go.Scatter(
            x=pivot.index, y=y, name=label, mode="lines",
            line=dict(color=REGION_COLORS[region], width=1.8),
            hovertemplate="%{x|%H:%M} $%{y:.0f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        height=240, margin=dict(l=44, r=8, t=10, b=24),
        legend=dict(orientation="v", yanchor="middle", y=0.5,
                    xanchor="left", x=1.005, font=dict(size=10),
                    bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   dtick=3600000, tickformat="%H:%M"),
        yaxis=dict(showgrid=False, tickfont=dict(size=10, color=MUTED),
                   autorange=True,
                   title=dict(text="$/MWh", font=dict(size=10, color=MUTED))),
    )
    title = (f"Predispatch forecast as at {run_time:%-d %b %H:%M} "
             f"(next {horizon_hrs:.0f}h)")
    return _wrap_plot("forecast-chart", title, fig)


# ============================================================================
# Tile 8: Market notices  (live AEMO scrape via production fetch_market_notices)
# ============================================================================

_notices_cache: dict = {"ts": 0.0, "data": []}
_NOTICES_TTL = 600  # 10 min; the scrape touches ~50 files so we don't want to
                    # do it more than once per refresh window.

NOTICE_BADGE_COLORS = {
    "RESERVE":        "#af3029",  # LOR notices — red
    "POWER":          "#a02f6f",  # emergency — magenta
    "INTER-REGIONAL": "#205ea6",  # interconnector — blue
    "GENERAL":        "#bc5215",  # default — orange
}


def _get_notices(limit: int = 10) -> list[dict]:
    now = time.time()
    if _notices_cache["data"] and now - _notices_cache["ts"] < _NOTICES_TTL:
        return _notices_cache["data"][:limit]
    try:
        from aemo_dashboard.nem_dash.market_notices import fetch_market_notices
        notices = fetch_market_notices(limit=limit)
        _notices_cache["data"] = notices
        _notices_cache["ts"] = now
        return notices
    except Exception:
        # Fall back to whatever we cached last; empty list on first failure.
        return _notices_cache["data"][:limit]


def _badge_label(notice: dict) -> str:
    """Match production's badge text: 'LRC/LOR1/LOR2/LOR3' for RESERVE-class
    notices that mention LOR, else 'Inter-Regional Trans' or the bare type."""
    type_id = notice.get("notice_type_id", "")
    type_desc = notice.get("notice_type_description", "") or ""
    reason = (notice.get("reason", "") or "").upper()
    if type_id == "RESERVE" and any(k in reason for k in ("LOR1", "LOR2", "LOR3", "LRC")):
        return "LRC/LOR1/LOR2/LOR3"
    if type_id == "INTER-REGIONAL":
        return "Inter-Regional Trans"
    return type_desc[:24] if type_desc else type_id


@app.get("/tile/market-notices", response_class=HTMLResponse)
def market_notices() -> HTMLResponse:
    # External AEMO fetch is the slow part; cache for 2 min since notices
    # change at most a few times per day.
    return _cached_tile("market-notices", _build_market_notices, ttl=120.0)


def _build_market_notices() -> HTMLResponse:
    notices = _get_notices(limit=8)
    if not notices:
        return HTMLResponse(
            _card_h3("Key market notices") +
            f'<div style="color:{MUTED};padding:10px;font-style:italic">'
            f'No key market notices in last 48h</div>')

    items = []
    for n in notices:
        ts = n.get("creation_date")
        ts_str = ts.strftime("%-d %b %H:%M") if ts else "—"
        label = _badge_label(n)
        type_id = n.get("notice_type_id", "GENERAL")
        color = NOTICE_BADGE_COLORS.get(type_id, NOTICE_BADGE_COLORS["GENERAL"])
        reason_short = html_lib.escape((n.get("reason_short", "") or "").strip())
        reason_full = html_lib.escape((n.get("reason_full", "") or "").strip())
        items.append(f"""
          <details style="border-bottom:1px solid {BORDER};padding:6px 0">
            <summary style="cursor:pointer;list-style:none;
                            display:flex;align-items:center;gap:8px;flex-wrap:wrap">
              <span style="font-weight:600;font-size:11px;color:{INK};
                           white-space:nowrap">{ts_str}</span>
              <span style="background:{color};color:#fffcf0;padding:2px 6px;
                           border-radius:3px;font-size:9px;font-weight:700;
                           white-space:nowrap">{html_lib.escape(label)}</span>
              <span style="margin-left:auto;color:#24837b;font-size:10px">
                click to expand</span>
            </summary>
            <div style="font-size:11px;color:{INK};line-height:1.5;margin-top:4px">
              {reason_short}{('&hellip;' if len(reason_full) > len(reason_short) else '')}
            </div>
            <pre style="font-size:10px;color:{MUTED};white-space:pre-wrap;
                        margin-top:6px;font-family:inherit">{reason_full}</pre>
          </details>
        """)

    body = "".join(items)
    return HTMLResponse(_card_h3("Key market notices") + f"""
      <div style="max-height:420px;overflow-y:auto;margin:-4px 0">{body}</div>
    """)


# ============================================================================
# Tile 9: Generator outages  (PASA-driven, stacked bars by fuel × region)
# ============================================================================

_outages_cache: dict = {"ts": 0.0, "data": None}
_OUTAGES_TTL = 300

# Match production palette in pasa/pasa_tab.py REGION_COLORS for the segments.
OUTAGE_REGION_COLORS = {
    "NSW": "#879a39", "QLD": "#da702c", "SA": "#a02f6f",
    "TAS": "#24837b", "VIC": "#8b7ec8",
}
REGION_DISPLAY = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC",
                  "SA1": "SA", "TAS1": "TAS"}


def _get_outage_data() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    now = time.time()
    if _outages_cache["data"] is not None and now - _outages_cache["ts"] < _OUTAGES_TTL:
        return _outages_cache["data"]
    try:
        from aemo_dashboard.pasa.change_detector import ChangeDetector
        from aemo_dashboard.pasa.pasa_tab import load_gen_info, DEFAULT_DATA_PATH
        detector = ChangeDetector(data_path=DEFAULT_DATA_PATH)
        outages = detector.get_current_generator_outages(min_reduction_mw=50)
        gen_info = load_gen_info()
        _outages_cache["data"] = (outages, gen_info)
        _outages_cache["ts"] = now
        return _outages_cache["data"]
    except Exception:
        return None


@app.get("/tile/outages-summary", response_class=HTMLResponse)
def outages_summary() -> HTMLResponse:
    # PASA fetch + render is the slow part; cache for 2 min since outage
    # schedules don't move minute-to-minute.
    return _cached_tile("outages-summary", _build_outages_summary, ttl=120.0)


def _build_outages_summary() -> HTMLResponse:
    """Horizontal stacked bar per fuel; segments per DUID coloured by region.

    Mirrors the production PASA tab's `create_generator_fuel_summary` —
    uses live PASA availability so returned generators self-exclude.
    """
    data = _get_outage_data()
    if data is None:
        return HTMLResponse(_card_h3("Generator outages") +
            f'<div style="color:{MUTED};padding:10px">PASA data unavailable</div>')
    outages, gen_info = data

    if outages.empty or gen_info.empty:
        return HTMLResponse(_card_h3("Generator outages") +
            f'<div style="color:{MUTED};padding:10px">No current outages</div>')

    # Join outages with fuel + region; fall back to gen_info capacity when
    # PASA reports 0/0 (genuine "off, nameplate") rather than "no change".
    info = gen_info.set_index("DUID")
    rows = []
    for _, row in outages.iterrows():
        duid = row["DUID"]
        if duid not in info.index:
            fuel, cap, region = "Unknown", 0.0, "Unknown"
        else:
            r = info.loc[duid]
            fuel = r.get("Fuel") or "Unknown"
            cap = float(r.get("Capacity(MW)") or 0)
            region = r.get("Region") or "Unknown"
        region_short = REGION_DISPLAY.get(region, region)
        reduction = float(row["reduction_mw"])
        if reduction <= 0 and float(row["current_mw"]) == 0 and cap > 0:
            reduction = cap
        if reduction > 0:
            rows.append({"duid": duid, "fuel": fuel,
                         "region": region_short, "mw": reduction})

    if not rows:
        return HTMLResponse(_card_h3("Generator outages") +
            f'<div style="color:{MUTED};padding:10px">No outages above 50 MW</div>')

    df = pd.DataFrame(rows)
    fuel_totals = df.groupby("fuel")["mw"].sum().sort_values(ascending=True)
    fuel_order = list(fuel_totals.index)  # ascending → Plotly draws top-to-bottom correctly
    total = float(fuel_totals.sum())

    fig = go.Figure()
    # One bar trace per DUID so each segment carries its own colour + label.
    # Stacked by y=fuel; legend is custom (region chips) so per-trace legend off.
    for _, r in df.sort_values(["fuel", "mw"], ascending=[True, False]).iterrows():
        color = OUTAGE_REGION_COLORS.get(r["region"], MUTED)
        fig.add_trace(go.Bar(
            x=[r["mw"]], y=[r["fuel"]], orientation="h",
            marker=dict(color=color, line=dict(width=0.5, color=PAPER)),
            text=r["duid"], textposition="inside", insidetextanchor="middle",
            textfont=dict(size=10, color=PAPER),
            hovertemplate=f"{r['duid']} ({r['region']}) %{{x:.0f}} MW<extra></extra>",
            showlegend=False,
        ))

    # MW totals as annotations on the right of each bar.
    max_mw = float(fuel_totals.max())
    annotations = [dict(x=mw, y=fuel, xref="x", yref="y",
                         text=f"<b>{mw:,.0f} MW</b>", showarrow=False,
                         xanchor="left", xshift=6,
                         font=dict(size=11, color=INK))
                    for fuel, mw in fuel_totals.items()]

    # Manual region legend (single chip per region present in this snapshot).
    regions_present = sorted(df["region"].unique())
    legend_chips = " ".join(
        f'<span style="display:inline-flex;align-items:center;gap:4px;'
        f'margin-right:10px;font-size:11px;color:{INK}">'
        f'<span style="width:11px;height:11px;background:'
        f'{OUTAGE_REGION_COLORS.get(rg, MUTED)};border-radius:2px"></span>{rg}'
        f'</span>'
        for rg in regions_present
    )

    fig.update_layout(
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        barmode="stack", height=max(160, 38 * len(fuel_order) + 50),
        margin=dict(l=70, r=80, t=8, b=24),
        xaxis=dict(showgrid=False, zeroline=False,
                   tickfont=dict(size=10, color=MUTED), ticksuffix=" MW",
                   range=[0, max_mw * 1.18]),
        yaxis=dict(showgrid=False, tickfont=dict(size=11, color=INK),
                   categoryorder="array", categoryarray=fuel_order),
        annotations=annotations,
    )
    return _wrap_plot_with_extras(
        "outages", f"Generator outages &middot; {total:,.0f} MW total", fig,
        extras_before=f'<div style="margin:0 0 6px 70px">{legend_chips}</div>',
    )


# ============================================================================
# Shared helpers
# ============================================================================

def _card_h3(title: str) -> str:
    return (f'<h3 style="margin:0 0 8px 0;font-size:12px;color:{TEAL};'
            f'text-transform:uppercase;letter-spacing:0.5px;font-weight:600">'
            f'{title}</h3>')


def _attribution(source: str = "AEMO") -> str:
    """Card footer: 'Data: <source> · plot: ITK'. Italic, muted, right-
    aligned, small — sits at the bottom of a card without competing with
    the chart. Use 'Global-Roam' for futures, 'AEMO STTM' for the gas tab,
    'AEMO' (default) elsewhere. See docs/NEM_dash_contents.md for the
    per-card data source inventory."""
    return (f'<div style="color:{MUTED};font-size:10px;font-style:italic;'
            f'margin:6px 14px 0;text-align:right">'
            f'Data: {source} &middot; plot: ITK</div>')


def _wrap_plot(slug: str, title: str, fig: go.Figure) -> HTMLResponse:
    return _wrap_plot_with_extras(slug, title, fig)


# ── Per-worker TTL cache for slow tile endpoints ─────────────────────────────
# The user-visible slow tiles (renewable-gauge, generation-mix) read from
# DuckDB *views* that scan large underlying tables; each request takes
# ~2–10 s. The tiles refresh client-side every 5 min, so a server-side
# cache with a short TTL collapses repeated visits within the same worker
# to a single computation. Caches don't share across uvicorn workers
# (each worker is its own process), but with 4 workers + 30-second TTL,
# the worst case is ~4 cold computations every 30 s — well below the
# every-5-min HTMX poll cadence.

_TILE_CACHE: dict[str, tuple[float, HTMLResponse]] = {}


def _cached_tile(key: str, builder, ttl: float = 30.0) -> HTMLResponse:
    """Return a cached HTMLResponse if fresh; otherwise rebuild and cache."""
    now = time.time()
    cached = _TILE_CACHE.get(key)
    if cached and (now - cached[0]) < ttl:
        return cached[1]
    fresh = builder()
    _TILE_CACHE[key] = (now, fresh)
    return fresh


def _wrap_plot_with_extras(slug: str, title: str, fig: go.Figure,
                           extras: str = "", extras_before: str = "") -> HTMLResponse:
    # Unique div id per render so concurrent tiles never collide.
    div_id = f"plot-{slug}-{int(datetime.now().timestamp() * 1000)}"
    fig_json = _plot_json(fig)
    height = fig.layout.height or 200
    return HTMLResponse(_card_h3(title) + extras_before + f"""
      <div id="{div_id}" style="height:{height}px"></div>
      <script>
        (function() {{
          var f = {fig_json};
          Plotly.newPlot("{div_id}", f.data, f.layout, {PLOTLY_CFG});
        }})();
      </script>
    """ + extras)
