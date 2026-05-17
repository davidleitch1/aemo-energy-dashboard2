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
    """Most-recent rooftop NEM-wide MW from rooftop30 (5-region sum)."""
    df = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM rooftop30)
        SELECT SUM(power) AS mw FROM rooftop30, latest
         WHERE settlementdate = latest.ts
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
    ("generation-mix",   "Generation mix",   [("stack",         "Stack"),
                                              ("tod",           "Time of day"),
                                              ("transmission",  "Transmission"),
                                              ("yr-on-yr",      "Yr on yr")]),
    ("evening-peak",     "Evening peak",     []),
    ("prices",           "Prices",           [("analysis", "Price Analysis"),
                                              ("bands",    "Price Bands")]),
    ("batteries",        "Batteries",        []),  # moved
    ("futures",          "Futures",          []),  # moved
    ("pivot-table",      "Pivot table",      []),
    ("station-analysis", "Station Analysis", []),
    ("trends",           "Trends",           []),
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
    tiny activate-on-URL JS handle the active state without server help."""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>NEM Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://unpkg.com/htmx.org@2.0.4"></script>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>{SHELL_CSS}</style>
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


def _render_range_pills(base_url: str, active: str, other_params: dict,
                        start: str = "", end: str = "") -> str:
    """Preset range pills + a Custom pill. When range=custom, the date form
    renders inline next to the pills so the user can refine the window."""
    pills = []
    for slug, label in RANGE_OPTIONS:
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
              f'Mean is the time-weighted price average.</p>')


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
                         f'paired at the 30-min level.</p>')

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
        f'</div>'
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
        + '</div>'
        + '<div class="card">'
        + _card_h3(tod_title)
        + f'<div id="{tod_div}" style="height:280px"></div>'
        + f'<script>(function(){{var f={tod_json};'
          f'Plotly.newPlot("{tod_div}",f.data,f.layout,{PLOTLY_CFG});}})();</script>'
        + f'<p style="color:{MUTED};font-size:11px;margin:8px 14px 0;line-height:1.5">'
        + 'Mean flow per hour of day across the selected window.</p>'
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
    # The iOS endpoint only supports a fixed set of periods. Our pill slugs
    # are broader; fold to the nearest supported period.
    period_map = {
        "7d": "7d", "30d": "30d", "ytd": "ytd", "1y": "1y",
        "1h": "7d", "24h": "7d", "all": "1y", "custom": "ytd",
    }
    period = period_map.get(range_slug, "ytd")
    period_note = (f' &middot; range "{range_slug.upper()}" → "{period.upper()}"'
                   if range_slug != period and range_slug not in ("custom",)
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
        start_curr, end_curr = _resolve_window(period, end_curr)
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

    period_labels = {"7d": "Last 7 days", "30d": "Last 30 days",
                     "ytd": "Year to date", "1y": "Last 12 months"}
    period_label = period_labels.get(period, period)
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
        + '</div></div>'
    )


@app.get("/generation-mix", response_class=HTMLResponse)
def generation_mix_root(region: str = "", range: str = "",
                        start: str | None = None, end: str | None = None
                        ) -> RedirectResponse:
    params = {}
    if region: params["region"] = region
    if range:  params["range"]  = range
    if start:  params["start"]  = start
    if end:    params["end"]    = end
    return RedirectResponse(url=_build_url("/generation-mix/stack", **params))


@app.get("/generation-mix/{sub}", response_class=HTMLResponse)
def generation_mix_sub(sub: str, request: Request,
                       region: str = "NEM",
                       range: str = "24h",
                       start: str | None = None,
                       end: str | None = None) -> HTMLResponse:
    _, subtabs = TAB_LOOKUP["generation-mix"]
    sub_slugs = {s for s, _ in subtabs}
    if sub not in sub_slugs:
        return HTMLResponse(status_code=404, content="Not found")

    if region not in GENMIX_REGION_LIST:
        region = "NEM"
    valid_ranges = {slug for slug, _ in RANGE_OPTIONS} | {"custom"}
    if range not in valid_ranges:
        range = "24h"

    base_params = {"region": region, "range": range}
    if start: base_params["start"] = start
    if end:   base_params["end"] = end

    base_url = f"/generation-mix/{sub}"
    selectors = _render_selector_strip(
        _render_region_pills(base_url, region,
                             {k: v for k, v in base_params.items() if k != "region"},
                             regions=GENMIX_REGION_LIST),
        _render_range_pills(base_url, range,
                            {k: v for k, v in base_params.items()
                             if k not in ("range", "start", "end")},
                            start=start or "", end=end or ""),
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
    if _slug in ("today", "prices", "generation-mix", "evening-peak") or _subs:
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
    df = q("""
        WITH latest AS (
            SELECT MAX(settlementdate) AS ts FROM generation_by_fuel_5min
        )
        SELECT settlementdate, fuel_type, SUM(total_generation_mw) AS mw
          FROM generation_by_fuel_5min, latest
         WHERE settlementdate = latest.ts
         GROUP BY settlementdate, fuel_type
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
    df = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM generation_by_fuel_5min)
        SELECT settlementdate, fuel_type, SUM(total_generation_mw) AS mw
          FROM generation_by_fuel_5min, latest
         WHERE settlementdate >= latest.ts - INTERVAL 24 HOUR
         GROUP BY settlementdate, fuel_type
         ORDER BY settlementdate
    """)
    if df.empty:
        return HTMLResponse('<div style="color:%s">no data</div>' % MUTED)

    df = _normalise_fuels(df)
    # Re-aggregate after collapsing CCGT/OCGT/Gas other → Gas.
    df = df.groupby(["settlementdate", "fuel_type"], as_index=False)["mw"].sum()

    # Merge rooftop (30-min, 5 regions → NEM total) onto the 5-min grid via ffill.
    roof = q("""
        WITH latest AS (SELECT MAX(settlementdate) AS ts FROM generation_by_fuel_5min)
        SELECT settlementdate, SUM(power) AS mw
          FROM rooftop30, latest
         WHERE settlementdate >= latest.ts - INTERVAL 24 HOUR
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


def _wrap_plot(slug: str, title: str, fig: go.Figure) -> HTMLResponse:
    return _wrap_plot_with_extras(slug, title, fig)


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
