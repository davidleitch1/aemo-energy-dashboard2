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

REGION_ORDER = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
REGION_COLORS = {
    "NSW1": "#879a39", "QLD1": "#da702c", "SA1": "#a02f6f",
    "TAS1": "#24837b", "VIC1": "#8b7ec8",
}
# Display palette uses normalised fuel names. The raw `generation_by_fuel_5min`
# table calls hydro "Water" and splits gas into CCGT/OCGT/Gas other; rooftop
# isn't in that table at all (it lives in `rooftop30`). `_normalise_fuels`
# folds raw → display.
FUEL_COLORS = {
    "Coal":            "#6F6E69",
    "Gas":             "#af3029",
    "Hydro":           "#205ea6",
    "Wind":            "#66800b",
    "Solar":           "#ad8301",
    "Rooftop Solar":   "#bc5215",
    "Battery Storage": "#5e409d",
    "Biomass":         "#7a6b3e",
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
    ("generation-mix",   "Generation mix",   []),
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
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
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
                  ("30d", "30D"), ("ytd", "YTD"), ("all", "All")]


def _build_url(base: str, **params) -> str:
    clean = {k: v for k, v in params.items() if v is not None and v != ""}
    return f"{base}?{urlencode(clean)}" if clean else base


def _render_region_pills(base_url: str, active: str,
                         other_params: dict, multi: bool = False) -> str:
    """Single-region (segmented capsule) by default. multi=True switches to
    independent toggle pills with comma-separated `region` URL param —
    clicking adds/removes from the selection. Last selected region cannot
    be deselected (would leave nothing to query)."""
    if not multi:
        pills = []
        for region in REGION_ORDER:
            params = dict(other_params, region=region)
            url = _build_url(base_url, **params)
            cls = "pill active" if region == active else "pill"
            pills.append(
                f'<button class="{cls}" '
                f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
                f'{region[:-1]}</button>'
            )
        return (f'<div class="pill-bar">'
                f'<span class="pill-bar-label">Region</span>'
                f'<div class="pill-group">{"".join(pills)}</div>'
                f'</div>')

    selected = set((active or "").split(","))
    pills = []
    for region in REGION_ORDER:
        is_active = region in selected
        if is_active and len(selected) > 1:
            new_set = selected - {region}      # toggle off
        elif is_active:
            new_set = selected                  # last one — no-op
        else:
            new_set = selected | {region}      # toggle on
        new_param = ",".join(r for r in REGION_ORDER if r in new_set)
        params = dict(other_params, region=new_param)
        url = _build_url(base_url, **params)
        cls = "pill active" if is_active else "pill"
        pills.append(
            f'<button class="{cls}" '
            f'hx-get="{url}" hx-target="#tab-body" hx-push-url="true">'
            f'{region[:-1]}</button>'
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
    fig_json = fig.to_json()
    return (
        _card_h3(title)
        + f'<div id="{div_id}" style="height:280px"></div>'
        + f'<script>(function(){{var f={fig_json};'
          f'Plotly.newPlot("{div_id}",f.data,f.layout,'
          f'{{displayModeBar:false,responsive:true}});}})();</script>'
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
    fig_json = fig.to_json()
    chart_html = (
        _card_h3(chart_title)
        + f'<div id="{div_id}" style="height:340px"></div>'
        + f'<script>(function(){{var f={fig_json};'
          f'Plotly.newPlot("{div_id}",f.data,f.layout,'
          f'{{displayModeBar:false,responsive:true}});}})();</script>'
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
    fig_json = fig.to_json()
    fig_height = fig.layout.height or 320
    butterfly_html = (
        _card_h3(f"Price band contribution &middot; {range_label}")
        + f'<div id="{div_id}" style="height:{fig_height}px"></div>'
        + f'<script>(function(){{var f={fig_json};'
          f'Plotly.newPlot("{div_id}",f.data,f.layout,'
          f'{{displayModeBar:false,responsive:true}});}})();</script>'
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
        stack_json = stack_fig.to_json()
        stack_html = (
            _card_h3(f"Price contribution by band &middot; {range_label}")
            + f'<div id="{stack_div}" style="height:320px"></div>'
            + f'<script>(function(){{var f={stack_json};'
              f'Plotly.newPlot("{stack_div}",f.data,f.layout,'
              f'{{displayModeBar:false,responsive:true}});}})();</script>'
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
    if _slug in ("today", "prices") or _subs:
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
    fig_json = fig.to_json()
    height = fig.layout.height or 200
    return HTMLResponse(_card_h3(title) + extras_before + f"""
      <div id="{div_id}" style="height:{height}px"></div>
      <script>
        (function() {{
          var f = {fig_json};
          Plotly.newPlot("{div_id}", f.data, f.layout,
                         {{displayModeBar: false, responsive: true}});
        }})();
      </script>
    """ + extras)
