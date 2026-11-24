"""Network congestion tab: where the grid holds generation and batteries back.

Framework-free, like battery_tod: every public *_html() function returns a
string the main dashboard drops into its tab body.

Sources (all opened read-only, once per query, never held)
- network_summary.duckdb: pre-aggregated daily tables, rebuilt whole by atomic
  rename. Almost every page reads only this file.
- constraints_readonly.duckdb: live 5-minute rows, used by the Now view only.
- aemo_readonly.duckdb: duid_mapping, LEFT JOINed for site, fuel, capacity.

Conventions
- `day` in the summary is the date of the interval start; the interval stamped
  00:00 belongs to the previous day. The last complete day is therefore the day
  of (data_through - 5 min) when that is 23:55 or later, otherwise the day
  before.
- Binding hours of an area = distinct 5-minute intervals in which at least one
  of its constraints bound, divided by 12. Constraint-hours count each binding
  constraint separately, so they can exceed binding hours.
- "Intervals limited" measures exposure (a binding constraint that includes the
  unit), not lost output.
- All times are NEM time (UTC+10, no daylight saving).
"""
from __future__ import annotations

import html as _html
import itertools
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go

DATA_DIR = "/Users/davidleitch/aemo_production/data"
SUMMARY_DB = os.environ.get("NETWORK_SUMMARY_DB", f"{DATA_DIR}/network_summary.duckdb")
CONSTRAINTS_DB = f"{DATA_DIR}/constraints_readonly.duckdb"
AEMO_DB = f"{DATA_DIR}/aemo_readonly.duckdb"
_DBS = {"summary": "SUMMARY_DB", "constraints": "CONSTRAINTS_DB", "aemo": "AEMO_DB"}

NEM_TZ = timezone(timedelta(hours=10))

REGIONS = [("NEM", "NEM"), ("NSW1", "NSW"), ("QLD1", "QLD"),
           ("VIC1", "VIC"), ("SA1", "SA"), ("TAS1", "TAS")]
RANGES = [("30d", "30D"), ("90d", "90D"), ("ytd", "YTD"), ("1y", "1Y"),
          ("all", "All")]
FUELS = [("all", "All"), ("wind", "Wind"), ("solar", "Solar"),
         ("battery", "Battery"), ("other", "Other")]
DEFAULT_RANGE = "90d"
MIN_UNIT_MW = 5.0
AREA_CONSTRAINT_CAP = 25
UNIT_CONSTRAINT_CAP = 15
OUTAGE_NOW_CAP = 25
VIOLATION_PENALTY_MV = 100_000
PASA_TRANSMISSION_URL = "/pasa/transmission"

# Flexoki Light
INK, MUTED, BORDER = "#403E3C", "#878580", "#E6E4D9"
CAUSES = ["System normal", "Outage", "Unit-specific limit"]
CAUSE_COLORS = {"System normal": "#205EA6", "Outage": "#BC5215",
                "Unit-specific limit": "#5E409D"}
CAUSE_DISPLAY = {"System normal": "Network as built", "Outage": "Equipment outage",
                 "Unit-specific limit": "Limit on a single plant"}
CAUSE_TILE = {"System normal": "Network as built share",
              "Outage": "Equipment outage share",
              "Unit-specific limit": "Single-plant limit share"}
CAUSE_KEY = [
    ("Network as built", "the limit binds with every line and transformer in service. "
     "AEMO marks these “Out = Nil”."),
    ("Equipment outage", "the limit applies only while a named line or transformer is "
     "out of service."),
    ("Limit on a single plant", "AEMO caps one wind or solar farm. Most are system "
     "strength limits, which depend on how many coal, gas or hydro units and "
     "synchronous condensers are running nearby."),
]
FUEL_COLORS = {"Wind": "#66800B", "Solar": "#AD8301", "Battery": "#5E409D"}
SERIES_COLORS = ["#AF3029", "#BC5215", "#AD8301", "#66800B",
                 "#24837B", "#205EA6", "#5E409D", "#A02F6F"]

CSS = """
.netw { padding: 8px 24px 24px; }
.netw .sub, .netw .note { color:#878580; font-size:12px; margin:6px 0; }
.netw .note { margin-top:18px; line-height:1.5; }
.netw h2 { font-size:17px; font-weight:600; margin:10px 0 2px; }
.netw h3 { font-size:12px; color:#24837B; text-transform:uppercase;
           letter-spacing:0.5px; font-weight:600; margin:22px 0 6px; }
.netw a { color:#205EA6; text-decoration:none; }
.netw a:hover { text-decoration:underline; }
.netw .strip { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
               gap:8px; margin:10px 0 4px; }
.netw .tile { border:1px solid #e6e4d9; border-radius:6px; padding:8px 10px; }
.netw .tile .k { color:#878580; font-size:11px; }
.netw .tile .v { font-size:17px; font-weight:600; margin-top:2px; }
.netw .placeholder { padding:40px; text-align:center; color:#878580; }
.netw .tbl-wrap { overflow-x:auto; }
.netw table { width:100%; border-collapse:collapse; font-size:13px; }
.netw th { text-align:left; font-size:11px; font-weight:600; color:#878580;
           text-transform:uppercase; letter-spacing:0.4px; padding:6px 10px;
           border-bottom:1px solid #e6e4d9; white-space:nowrap; }
.netw td { padding:6px 10px; border-top:1px solid #e6e4d9; vertical-align:top; }
.netw tbody tr:first-child td { border-top:none; }
.netw th.r, .netw td.r { text-align:right; font-variant-numeric:tabular-nums; }
.netw td.nw { white-space:nowrap; }
.netw td.wrap360 { max-width:360px; min-width:200px; white-space:normal; overflow-wrap:anywhere; }
.netw td.wrap260 { max-width:260px; min-width:160px; white-space:normal; overflow-wrap:anywhere; }
.netw .muted { color:#878580; }
.netw .mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; color:#878580; font-size:12px; }
.netw .key { margin:10px 0 0; padding:0; list-style:none; font-size:12px; color:#403E3C; line-height:1.5; }
.netw .key li { margin:2px 0; }
.netw .key i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:6px; }
.netw .cbar { display:flex; height:8px; width:90px; border-radius:2px;
              overflow:hidden; margin-top:4px; }
.netw .cbar span { display:block; height:100%; }
.netw .legend { display:flex; flex-wrap:wrap; gap:14px; font-size:12px;
                color:#403E3C; margin:6px 0 0; }
.netw .legend i { display:inline-block; width:10px; height:10px;
                  border-radius:2px; margin-right:5px; }
.netw .row3 { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; }
.netw .row3 h3 { margin-top:10px; }
.netw-strip .pill-bar { flex-wrap:wrap; }
.netw-strip .pill-group { flex-wrap:wrap; border-radius:12px; }
.netw .vswitch { display:inline-flex; border:1px solid #e6e4d9; border-radius:14px;
                 overflow:hidden; margin:8px 0 2px; }
.netw .vsw { padding:4px 14px; font-size:13px; color:#403E3C; border-right:1px solid #e6e4d9; }
.netw .vsw:last-child { border-right:none; }
.netw .vsw.active { background:#24837B; color:#fffcf0; }
.netw .vsw:hover { text-decoration:none; }
.netw .cpick { display:flex; flex-wrap:wrap; gap:6px; margin:6px 0; }
.netw .cpick a { border:1px solid #e6e4d9; border-radius:4px; padding:3px 8px; font-size:12px; color:#403E3C; }
.netw .cpick a.on { background:#f2f0e5; border-color:#b7b5ac; font-weight:600; }
.netw .lead { background:#f2f0e5; border-radius:4px; padding:10px 12px; font-size:13px; margin:8px 0; }
.netw .lead .idm { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:13px; }
.netw .eq { display:grid; grid-template-columns:1fr auto 1fr; gap:14px; align-items:center;
            border-top:1px solid #b7b5ac; border-bottom:1px solid #b7b5ac; padding:14px 0;
            font-size:14px; color:#100f0f; }
.netw .eq > div:first-child { border-left:4px solid #205EA6; padding-left:10px; }
.netw .eq > div:last-child { border-left:4px solid #AF3029; padding-left:10px; }
.netw .eq .lab { display:block; font-size:11px; letter-spacing:0.08em; text-transform:uppercase;
                 color:#6f6e69; font-weight:700; margin-bottom:2px; }
.netw .eq .rel { font-size:24px; font-weight:600; border:none; padding:0; }
.netw .note-eq { max-width:70ch; font-size:15px; color:#100f0f; margin:0; }
.netw .stale { color:#878580; font-size:12px; margin:0 0 6px; }
.netw .hbars { display:flex; align-items:flex-end; gap:3px; height:48px; max-width:520px; }
.netw .hbars span { flex:1; background:#AF3029; border-radius:1px; min-width:4px; }
.netw .haxis { display:flex; justify-content:space-between; max-width:520px; font-size:11px;
               color:#878580; }
.netw td.mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px;
                white-space:nowrap; color:#403E3C; }
.netw .selbar { margin:8px 0; padding:8px 10px; border:1px solid #e6e4d9;
                border-radius:6px; font-size:13px; }
.netw .mapwrap { display:grid; grid-template-columns:minmax(0,1fr) 300px; gap:14px;
                 align-items:start; margin-top:8px; }
.netw .mapbox { height:640px; border:1px solid #e6e4d9; border-radius:6px; overflow:hidden; }
.netw .side h3 { margin-top:0; }
.netw .side ul { list-style:none; margin:0; padding:0; font-size:13px; }
.netw .side li { padding:6px 0; border-top:1px solid #e6e4d9; }
.netw .side li:first-child { border-top:none; }
.netw .side-meta { display:block; color:#878580; font-size:12px; margin-top:2px; }
.netw .tag { display:inline-block; border:1px solid #e6e4d9; border-radius:3px;
             padding:0 5px; font-size:11px; color:#403E3C; margin-right:6px; }
@media (max-width: 800px) {
  .netw .mapwrap { grid-template-columns:1fr; }
  .netw .mapbox { height:480px; }
}
@media (max-width: 800px) {
  .netw { padding:8px 12px 20px; }
  .netw .row3 { grid-template-columns:1fr; }
}
"""

_plot_counter = itertools.count()


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

def q(sql: str, params: list | None = None, db: str = "summary") -> pd.DataFrame:
    """One read-only connection per query (the summary file is replaced by an
    atomic rename, so a held connection would keep reading the old file)."""
    path = globals()[_DBS[db]]
    conn = duckdb.connect(path, read_only=True)
    try:
        return conn.execute(sql, params or []).df()
    finally:
        conn.close()


def _columns(table: str) -> set[str]:
    return set(q(f"DESCRIBE {table}")["column_name"])


def _tables() -> set[str]:
    try:
        return set(q("SELECT table_name FROM information_schema.tables")["table_name"])
    except Exception:
        return set()


SORTS = [("hours", "Binding hours"), ("energy", "Energy held back")]


def normalise_sort(sort: str, default: str = "hours") -> str:
    return sort if sort in dict(SORTS) else default


def normalise_view(view: str) -> str:
    return view if view in ("overview", "equation") else "overview"


def fw(v, d: int = 3) -> str:
    """Thousands separators, up to d decimals, trailing decimal zeros dropped.
    Zeros of whole numbers are never dropped (350 stays 350)."""
    if _isnan(v):
        return "–"
    t = f"{abs(v):,.{d}f}"
    if "." in t:
        t = t.rstrip("0").rstrip(".")
    return ("−" if v < 0 and float(t.replace(",", "") or 0) != 0 else "") + t


def _nem_now() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None))


class NoData(Exception):
    """Raised when a page cannot be built; rendered as a placeholder."""


# ---------------------------------------------------------------------------
# Parameters and period arithmetic
# ---------------------------------------------------------------------------

def normalise_region(region: str) -> str:
    return region if region in dict(REGIONS) else "NEM"


def normalise_range(range_slug: str) -> str:
    return range_slug if range_slug in dict(RANGES) else DEFAULT_RANGE


def normalise_fuel(fuel: str) -> str:
    return fuel if fuel in dict(FUELS) else "all"


def area_region(region: str) -> str | None:
    """Region slug (NSW1) -> the label used in area.region (NSW); NEM -> None."""
    return None if region == "NEM" else dict(REGIONS)[region]


@dataclass(frozen=True)
class Period:
    start: date
    end: date
    label: str
    slug: str
    n_intervals: int = 0     # 5-minute intervals processed in the period

    @property
    def hours(self) -> float:
        return self.n_intervals / 12


def last_complete_day(data_through: pd.Timestamp) -> date:
    ts = pd.Timestamp(data_through) - pd.Timedelta(minutes=5)
    if ts.hour == 23 and ts.minute >= 55:
        return ts.date()
    return ts.date() - timedelta(days=1)


_RANGE_DAYS = {"30d": (30, "last 30 days"), "90d": (90, "last 90 days"),
               "1y": (365, "last 12 months")}


def period_window(slug: str, data_from: pd.Timestamp,
                  data_through: pd.Timestamp) -> Period:
    slug = normalise_range(slug)
    end = last_complete_day(data_through)
    first = (pd.Timestamp(data_from) - pd.Timedelta(minutes=5)).date()
    if slug == "ytd":
        start, label = date(end.year, 1, 1), "year to date"
    elif slug == "all":
        start, label = first, "all data"
    else:
        n, label = _RANGE_DAYS[slug]
        start = end - timedelta(days=n - 1)
    return Period(max(start, first), end, label, slug)


def load_meta() -> dict:
    try:
        m = q("SELECT built_at, data_from, data_through FROM meta")
    except Exception as exc:
        raise NoData("Network summary data is not available.") from exc
    if m.empty:
        raise NoData("Network summary data is not available.")
    return m.iloc[0].to_dict()


def get_period(range_slug: str) -> Period:
    """Period for a range slug, with the interval count filled in. Raises
    NoData when the period holds no processed intervals."""
    meta = load_meta()
    p = period_window(range_slug, meta["data_from"], meta["data_through"])
    if p.end < p.start:
        raise NoData("No complete day of network data yet.")
    n = q("SELECT COALESCE(SUM(n_intervals), 0) AS n FROM intervals_daily "
          "WHERE day BETWEEN ? AND ?", [p.start, p.end]).iloc[0]["n"]
    if not n:
        raise NoData("No network data in this period.")
    return Period(p.start, p.end, p.label, p.slug, int(n))


def days_in_months(p: Period) -> int:
    """Days with processed intervals in the calendar months the period touches."""
    first = pd.Timestamp(p.start).replace(day=1).date()
    last = (pd.Timestamp(p.end) + pd.offsets.MonthEnd(0)).date()
    return int(q("SELECT COUNT(*) AS n FROM intervals_daily WHERE day BETWEEN ? AND ? "
                 "AND n_intervals > 0", [first, last]).iloc[0]["n"])


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def esc(v) -> str:
    return _html.escape("" if _isnan(v) else str(v), quote=True)


def _isnan(v) -> bool:
    return v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NA \
        or v is pd.NaT


def fnum(v, nd: int = 0) -> str:
    return "–" if _isnan(v) else f"{v:,.{nd}f}"


def fhours(v) -> str:
    return "–" if _isnan(v) else f"{v:,.0f}"


def fgwh(v) -> str:
    """GWh: one decimal below 100, none from 100 up."""
    if _isnan(v):
        return "–"
    return f"{v:,.1f}" if abs(v) < 100 else f"{v:,.0f}"


def fpct(v) -> str:
    """Percent: one decimal below 10, none from 10 up."""
    if _isnan(v):
        return "–"
    return f"{v:.1f}%" if abs(v) < 10 else f"{v:.0f}%"


def cdisp(cause) -> str:
    return CAUSE_DISPLAY.get(cause, "–" if _isnan(cause) else str(cause))


def fmoney_m(v) -> str:
    return "–" if _isnan(v) else f"${v:,.1f}m"


def fsigned(v, nd: int = 0) -> str:
    if _isnan(v):
        return "–"
    return ("−" if v < 0 else "") + f"{abs(v):,.{nd}f}"


def fdt(v) -> str:
    return "–" if _isnan(v) else pd.Timestamp(v).strftime("%-d %b %Y %H:%M")


def fuel_label(fuel) -> str:
    return "Battery" if fuel == "Battery Storage" else ("–" if _isnan(fuel) else str(fuel))


def fuel_group(fuel) -> str:
    f = fuel_label(fuel)
    return f.lower() if f in ("Wind", "Solar", "Battery") else "other"


def cause_shares(sn: float, ou: float, us: float) -> tuple[float, float, float]:
    """Percent shares of (System normal, Outage, Unit-specific); they sum to 100
    (all zero when there is nothing to split)."""
    tot = sn + ou + us
    if not tot:
        return (0.0, 0.0, 0.0)
    return (sn / tot * 100, ou / tot * 100, us / tot * 100)


def link(url: str, text: str) -> str:
    return (f'<a href="{esc(url)}" hx-get="{esc(url)}" hx-target="#tab-body" '
            f'hx-push-url="true">{esc(text)}</a>')


def area_url(area_id: str, rng: str, view: str | None = None,
             constraint: str | None = None, all_terms: bool = False) -> str:
    u = f"/network/area?id={quote(str(area_id), safe='')}&range={rng}"
    if view and view != "overview":
        u += f"&view={view}"
    if constraint:
        u += f"&constraint={quote(str(constraint), safe='')}"
    if all_terms:
        u += "&all=1"
    return u


def unit_url(duid: str, rng: str) -> str:
    return f"/network/unit?duid={quote(str(duid), safe='')}&range={rng}"


def tiles_html(tiles: list[tuple[str, str]]) -> str:
    inner = "".join(f'<div class="tile"><div class="k">{esc(k)}</div>'
                    f'<div class="v">{v}</div></div>' for k, v in tiles)
    return f'<div class="strip">{inner}</div>'


class Span(str):
    """A cell that spans several columns."""
    def __new__(cls, text: str, span: int = 2):
        o = super().__new__(cls, text)
        o.span = span
        return o


def table_html(headers: list[tuple], rows: list[list[str]],
               empty: str = "No rows in this period.") -> str:
    """headers: (label, right_aligned[, css class for the column's cells]).
    Cells are pre-rendered HTML; a Span cell covers several columns."""
    if not rows:
        return f'<div class="sub">{esc(empty)}</div>'
    head = "".join(f'<th class="{"r" if h[1] else ""}">{esc(h[0])}</th>' for h in headers)
    body = []
    for row in rows:
        tds, col = [], 0
        for c in row:
            span = getattr(c, "span", 1)
            right = headers[col][1]
            extra = headers[col][2] if len(headers[col]) > 2 else ""
            cls = " ".join(x for x in (("r" if right else ""), extra) if x)
            colspan = f' colspan="{span}"' if span > 1 else ""
            tds.append(f'<td class="{cls}"{colspan}>{c}</td>')
            col += span
        body.append("<tr>" + "".join(tds) + "</tr>")
    return (f'<div class="tbl-wrap"><table><thead><tr>{head}</tr></thead>'
            f'<tbody>{"".join(body)}</tbody></table></div>')


def cause_bar(sn: float, ou: float, us: float) -> str:
    shares = cause_shares(sn, ou, us)
    if not sum(shares):
        return "–"
    title = " · ".join(f"{CAUSE_DISPLAY[c]} {s:.0f}%" for c, s in zip(CAUSES, shares))
    segs = "".join(f'<span style="width:{s:.2f}%;background:{CAUSE_COLORS[c]}"></span>'
                   for c, s in zip(CAUSES, shares))
    return f'<div class="cbar" title="{esc(title)}">{segs}</div>'


def cause_legend() -> str:
    return '<div class="legend">' + "".join(
        f'<span><i style="background:{CAUSE_COLORS[c]}"></i>{CAUSE_DISPLAY[c]}</span>'
        for c in CAUSES) + "</div>"


def cause_key() -> str:
    return '<ul class="key">' + "".join(
        f'<li><i style="background:{CAUSE_COLORS[c]}"></i><b>{esc(n)}:</b> {esc(t)}</li>'
        for c, (n, t) in zip(CAUSES, CAUSE_KEY)) + "</ul>"


def placeholder(msg: str) -> str:
    return f'<div class="netw"><div class="placeholder">{esc(msg)}</div></div>'


def wrap(body: str) -> str:
    return f'<div class="netw">{body}</div>'


def note_html() -> str:
    try:
        m = load_meta()
        vintage = (f"Summary built {fdt(m['built_at'])}; data through "
                   f"{fdt(m['data_through'])} NEM time. ")
    except NoData:
        vintage = ""
    return (
        '<div class="note">Source: AEMO. ' + vintage +
        '“Intervals limited” measures exposure (a binding constraint that '
        'includes the unit), not lost output. “Curtailed while constrained” is '
        'the unit’s curtailment in intervals when such a constraint was binding, '
        'valued at the regional price with negative prices counted as zero. '
        'Constraint definitions arrive from AEMO about a month after the '
        'event, so recent constraints show “description pending”.</div>')


# ---------------------------------------------------------------------------
# Plotly
# ---------------------------------------------------------------------------

def _layout(fig: go.Figure, height: int, **kw) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, -apple-system, sans-serif", size=12, color=INK),
        height=height, margin=dict(l=48, r=12, t=8, b=36),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
        **kw)
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor=BORDER,
                     tickfont=dict(color=INK))
    fig.update_yaxes(showgrid=False, zeroline=False, linecolor=BORDER,
                     tickfont=dict(color=INK))
    return fig


def plot_html(fig: go.Figure, height: int) -> str:
    """Div + script. The id is unique per render: htmx's settle step copies the
    old element's attributes onto a new element with the same id, and Plotly
    initialising against those stale attributes garbles the chart."""
    div_id = f"netw-plot-{int(datetime.now().timestamp() * 1000)}-{next(_plot_counter)}"
    cfg = json.dumps({"displaylogo": False, "responsive": True})
    return (f'<div id="{div_id}" style="height:{height}px"></div>'
            f'<script>(function(){{var f={fig.to_json()};'
            f'Plotly.newPlot("{div_id}",f.data,f.layout,{cfg});}})();</script>')


def _month_axis(p: Period) -> list[pd.Timestamp]:
    return list(pd.date_range(pd.Timestamp(p.start).replace(day=1),
                              pd.Timestamp(p.end), freq="MS"))


def stacked_monthly(p: Period, series: dict[str, pd.Series], colors: dict[str, str],
                    ytitle: str, height: int = 340) -> str:
    """series: name -> Series indexed by month-start Timestamp (hours)."""
    months = _month_axis(p)
    labels = [m.strftime("%b %Y") for m in months]   # categories: bars sit on the tick
    fig = go.Figure()
    for name, s in series.items():
        s = s.reindex(months).fillna(0.0)
        fig.add_trace(go.Bar(
            x=labels, y=s.values, name=CAUSE_DISPLAY.get(name, name),
            marker_color=colors[name],
            hovertemplate=f"{esc(CAUSE_DISPLAY.get(name, name))}<br>%{{x}}: %{{y:,.0f}}<extra></extra>"))
    _layout(fig, height, barmode="stack", bargap=0.2)
    fig.update_yaxes(title=dict(text=ytitle, font=dict(size=11, color=MUTED)),
                     tickformat=",.0f")
    fig.update_xaxes(type="category")
    return plot_html(fig, height)


def hbar(labels: list[str], values: list[float], color: str | list[str],
         height: int = 260) -> str:
    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h", marker_color=color,
        hovertemplate="%{y}: %{x:,.0f} h<extra></extra>"))
    _layout(fig, height, showlegend=False)
    fig.update_layout(margin=dict(l=8, r=12, t=8, b=36))
    fig.update_yaxes(autorange="reversed", automargin=True)
    fig.update_xaxes(title=dict(text="constraint-hours", font=dict(size=11, color=MUTED)),
                     tickformat=",.0f")
    return plot_html(fig, height)


def _monthly(df: pd.DataFrame, day_col: str, val_col: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    d = pd.to_datetime(df[day_col]).dt.to_period("M").dt.to_timestamp()
    return df[val_col].groupby(d).sum()


# ---------------------------------------------------------------------------
# Hotspots
# ---------------------------------------------------------------------------

def hotspot_areas(p: Period, region: str, sort: str = "hours") -> pd.DataFrame:
    """One row per area that bound in the period, ranked by binding hours or by
    energy held back (ties fall back to the other measure)."""
    cols = _columns("area")
    mw = "a.units_mw" if "units_mw" in cols else "a.weighted_mw"
    reg_sql, params = "", [p.start, p.end]
    ar = area_region(region)
    if ar:
        reg_sql, params = "WHERE a.region = ?", params + [ar]
    ic = "a.interconnectors" if "interconnectors" in cols else "NULL"
    order = ("COALESCE(ad.mwh, 0) DESC, ad.n DESC" if sort == "energy" else
             "ad.n DESC, COALESCE(ad.mwh, 0) DESC")
    df = q(f"""
        WITH ad AS (
            SELECT area_id, SUM(n_intervals) AS n, SUM(n_sysnormal) AS sn,
                   SUM(n_outage) AS ou, SUM(n_unit_specific) AS us,
                   SUM(constrained_mwh) AS mwh, SUM(constrained_value) AS val
              FROM area_daily WHERE day BETWEEN ? AND ?
             GROUP BY area_id HAVING SUM(n_intervals) > 0)
        SELECT ad.*, COALESCE(a.name, ad.area_id) AS name, a.region, a.limittype,
               a.n_units, {mw} AS units_mw, {ic} AS interconnectors
          FROM ad JOIN area a USING (area_id) {reg_sql}
         ORDER BY {order}, ad.area_id""", params)
    df["hours"] = df["n"] / 12
    df["pct_period"] = df["n"] / p.n_intervals * 100
    return df


def load_ranks() -> dict | None:
    """{window: DataFrame indexed by area_id} from area_rank, or None when the
    table is absent."""
    if "area_rank" not in _tables():
        return None
    try:
        df = q('SELECT "window" AS win, area_id, gwh, hours, rank_energy, rank_hours '
               "FROM area_rank")
    except Exception:
        return None
    return {w: g.set_index("area_id") for w, g in df.groupby("win")}


def rank_movement(rank, prev) -> str:
    """Movement of a rank against the earlier window: ▲n up, ▼n down, – no
    change, new when the area was not ranked before."""
    if _isnan(rank):
        return "–"
    if _isnan(prev):
        return "new"
    d = int(prev) - int(rank)
    return f"▲{d}" if d > 0 else f"▼{-d}" if d < 0 else "–"


def _rank_of(ranks: dict, window: str, area_id: str, col: str):
    df = ranks.get(window)
    if df is None or area_id not in df.index:
        return None
    v = df.loc[area_id, col]
    return None if _isnan(v) else int(v)


def rising_areas(ranks: dict | None, top: int = 5) -> set[str]:
    """Areas in the 90-day top five by energy that are not in the 12-month top five."""
    if not ranks or "90d" not in ranks:
        return set()
    out = set()
    for aid, r in ranks["90d"]["rank_energy"].items():
        if _isnan(r) or int(r) > top:
            continue
        r12 = _rank_of(ranks, "12m", aid, "rank_energy")
        if r12 is None or r12 > top:
            out.add(aid)
    return out


def rank_cell(ranks: dict, area_id: str, sort: str) -> str:
    col = "rank_energy" if sort == "energy" else "rank_hours"
    r = _rank_of(ranks, "12m", area_id, col)
    if r is None:
        return "–"
    mv = rank_movement(r, _rank_of(ranks, "12m_prev", area_id, col))
    return f'{r} <span class="muted">{mv}</span>'


RISING_TAG = '<span class="tag">rising</span>'


def _sum_cause(df: pd.DataFrame) -> tuple[float, float, float]:
    return float(df["sn"].sum()), float(df["ou"].sum()), float(df["us"].sum())


def _area_monthly(area_ids: list[str], p: Period, col: str = "n") -> pd.DataFrame:
    ph = ",".join("?" * len(area_ids))
    df = q(f"""SELECT date_trunc('month', day) AS month, area_id,
                      SUM(n_intervals) AS n, SUM(n_sysnormal) AS sn,
                      SUM(n_outage) AS ou, SUM(n_unit_specific) AS us
                 FROM area_daily WHERE day BETWEEN ? AND ? AND area_id IN ({ph})
                GROUP BY 1, 2""", [p.start, p.end] + area_ids)
    df["month"] = pd.to_datetime(df["month"])
    return df


def _period_sub(p: Period, lead: str = "Period") -> str:
    return (f'<div class="sub">{esc(lead)}, {p.label} '
            f'({p.start.strftime("%-d %b %Y")} to {p.end.strftime("%-d %b %Y")}, '
            f'{fnum(p.hours)} hours).</div>')


def _region_text(region: str) -> str:
    return "NEM" if region == "NEM" else dict(REGIONS)[region]


def hotspots_html(region: str, range_slug: str, sort: str = "hours") -> str:
    try:
        sort = normalise_sort(sort)
        p = get_period(range_slug)
        df = hotspot_areas(p, region, sort)
        ranks = load_ranks()
        rising = rising_areas(ranks)
        if df.empty:
            return placeholder(f"No constraint bound in {_region_text(region)} "
                               f"for the {p.label}.")
        sn, ou, us = _sum_cause(df)
        sh = cause_shares(sn, ou, us)
        tiles = tiles_html([
            ("Areas that bound", fnum(len(df))),
            (CAUSE_TILE["System normal"], fpct(sh[0])),
            (CAUSE_TILE["Outage"], fpct(sh[1])),
            (CAUSE_TILE["Unit-specific limit"], fpct(sh[2])),
            ("Curtailed while constrained", f"{fgwh(df['mwh'].sum() / 1000)} GWh"),
            ("Value of that curtailment", fmoney_m(df["val"].sum() / 1e6)),
        ])
        has_ic = "interconnectors" in df.columns
        rows = []
        for _, r in df.head(25).iterrows():
            if not r["n_units"]:
                ic = r["interconnectors"] if has_ic and not _isnan(r["interconnectors"]) \
                    and str(r["interconnectors"]).strip() else "interconnector"
                units_cells = [Span(f'<span class="muted">{esc(ic)}</span>', 2)]
            else:
                units_cells = [fnum(r["n_units"]), fnum(r["units_mw"])]
            name_cell = link(area_url(r["area_id"], p.slug), r["name"]) \
                + (" " + RISING_TAG if r["area_id"] in rising else "")
            rows.append(([rank_cell(ranks, r["area_id"], sort)] if ranks else []) + [
                name_cell,
                esc(r["region"] or "–"),
                fhours(r["hours"]), fpct(r["pct_period"]),
                cause_bar(r["sn"], r["ou"], r["us"]),
                *units_cells,
                esc(r["limittype"] or "–"),
                fgwh(r["mwh"] / 1000), fnum(r["val"] / 1e6, 1)])
        heads = ([("12-month rank", True)] if ranks else []) + [
            ("Area", False, "wrap360"), ("Region", False), ("Hours", True),
            ("% of period", True), ("Cause split", False),
            ("Units in constraints", True), ("MW in constraints", True),
            ("Limit type", False), ("GWh curtailed", True), ("$m", True)]
        table = table_html(heads, rows)
        if ranks:
            measure_txt = "energy held back" if sort == "energy" else "binding hours"
            table += ('<div class="sub">12-month rank is the area’s position over the last '
                      f'12 months by {measure_txt}, whatever period is selected. The mark beside it '
                      'shows movement against the 12 months before: ▲ moved up, ▼ moved down, '
                      '– no change, new = not ranked then. “rising” marks an area in the top five '
                      'by energy held back over the last 90 days that is outside the 12-month top '
                      'five.</div>')
        top = list(df.head(8)["area_id"])
        m = _area_monthly(top, p)
        names = dict(zip(df["area_id"], df["name"]))
        series, colors = {}, {}
        for i, aid in enumerate(top):
            label = str(names[aid])
            label = label if len(label) <= 38 else label[:36] + "…"
            if label in series:
                label = f"{label} ({aid})"
            sub = m[m["area_id"] == aid]
            series[label] = pd.Series(sub["n"].to_numpy() / 12, index=sub["month"])
            colors[label] = SERIES_COLORS[i % len(SERIES_COLORS)]
        chart = stacked_monthly(p, series, colors, "binding hours per month", 380)
        return wrap(
            _period_sub(p, f"{_region_text(region)} network congestion")
            + tiles + cause_key()
            + f'<h3>Top 25 congestion areas by {"energy held back" if sort == "energy" else "binding hours"}</h3>' + cause_legend()
            + table
            + '<h3>Monthly binding hours, top 8 areas of the period</h3>' + chart
            + note_html())
    except NoData as e:
        return placeholder(str(e))


# ---------------------------------------------------------------------------
# Constraint descriptions
# ---------------------------------------------------------------------------

def describe(desc, rule) -> str:
    if _isnan(rule) or rule == "none" or _isnan(desc) or not str(desc).strip():
        return '<span style="color:#878580">description pending</span>'
    return esc(desc)


# ---------------------------------------------------------------------------
# Area page
# ---------------------------------------------------------------------------

def area_outages(area_id: str, p: Period, limit: int = 30) -> pd.DataFrame:
    return q("""
        SELECT outageid,
               string_agg(DISTINCT CASE WHEN substationid IN ('N/A', '') THEN NULL
                                        ELSE substationid END, ', ') AS substation,
               string_agg(DISTINCT equipmenttype || ' ' || equipmentid, ', ') AS equipment,
               MIN(starttime) AS starttime, MAX(endtime) AS endtime,
               ANY_VALUE(outagestatuscode) AS status, ANY_VALUE(reason) AS reason,
               MAX(n_binding_intervals) AS n
          FROM outage_area
         WHERE area_id = ? AND outagestatuscode <> 'WDRAWN'
           AND n_binding_intervals > 0
           AND starttime < ? AND endtime >= ?
         GROUP BY outageid ORDER BY n DESC, outageid LIMIT ?""",
             [area_id, p.end + timedelta(days=1), p.start, limit])


def outage_rows(df: pd.DataFrame, with_area: bool = False) -> list[list[str]]:
    rows = []
    for _, r in df.iterrows():
        row = []
        if with_area:
            row.append(r["area_html"])
        row += [esc(r["substation"] or "–"), esc(r["equipment"]),
                fdt(r["starttime"]), fdt(r["endtime"]), esc(r["status"]),
                esc(r["reason"] or "–"), fhours(r["n"] / 12)]
        rows.append(row)
    return rows


def area_html(area_id: str, range_slug: str, view: str = "overview",
              constraint: str | None = None, show_all: bool = False) -> str:
    try:
        p = get_period(range_slug)
        cols = _columns("area")
        mw = "units_mw" if "units_mw" in cols else "weighted_mw"
        desc_sql = "description" if "description" in cols else "NULL"
        a = q(f"SELECT name, region, limittype, n_units, {mw} AS units_mw, "
              f"{desc_sql} AS description FROM area WHERE area_id = ?", [area_id])
        if a.empty:
            return placeholder(f"No congestion area with id “{area_id}”.")
        a = a.iloc[0]
        cons = q("""SELECT d.constraintid, SUM(d.n_intervals) AS n, m.cause, m.limittype,
                           m.description, m.definition_rule
                      FROM constraint_daily d
                      JOIN constraint_meta m USING (constraintid)
                     WHERE m.area_id = ? AND d.day BETWEEN ? AND ?
                     GROUP BY ALL ORDER BY n DESC, d.constraintid""",
                 [area_id, p.start, p.end])
        ad = q("""SELECT SUM(n_intervals) AS n, SUM(n_sysnormal) AS sn, SUM(n_outage) AS ou,
                         SUM(n_unit_specific) AS us, SUM(constrained_mwh) AS mwh,
                         SUM(constrained_value) AS val
                    FROM area_daily WHERE area_id = ? AND day BETWEEN ? AND ?""",
               [area_id, p.start, p.end]).iloc[0]
        n = 0.0 if _isnan(ad["n"]) else float(ad["n"])
        sh = cause_shares(*[0.0 if _isnan(ad[k]) else float(ad[k]) for k in ("sn", "ou", "us")])
        desc = a["description"]
        if _isnan(desc) or not str(desc).strip():       # fall back to the top constraint
            known = cons[(cons["definition_rule"] != "none")
                         & cons["description"].fillna("").str.strip().ne("")]
            if known.empty:
                known = q("""SELECT description FROM constraint_meta
                              WHERE area_id = ? AND definition_rule <> 'none'
                                AND description IS NOT NULL AND description <> ''
                              LIMIT 1""", [area_id])
            desc = known.iloc[0]["description"] if len(known) else None
        tiles = tiles_html([
            ("Binding hours", fhours(n / 12)),
            ("% of period", fpct(n / p.n_intervals * 100)),
            (CAUSE_TILE["System normal"], fpct(sh[0])),
            (CAUSE_TILE["Outage"], fpct(sh[1])),
            (CAUSE_TILE["Unit-specific limit"], fpct(sh[2])),
            ("Units in constraints", fnum(a["n_units"])),
            ("MW in constraints", fnum(a["units_mw"])),
            ("Curtailed while constrained",
             "–" if _isnan(ad["mwh"]) else f"{fgwh(ad['mwh'] / 1000)} GWh"),
            ("Value of that curtailment",
             fmoney_m(None if _isnan(ad["val"]) else ad["val"] / 1e6)),
        ])
        m = _area_monthly([area_id], p)
        series = {c: (m.set_index("month")[k] / 12) for c, k in
                  zip(CAUSES, ("sn", "ou", "us"))}
        chart = stacked_monthly(p, series, CAUSE_COLORS, "constraint-hours per month", 320)
        crows = [[esc(r["constraintid"]), fhours(r["n"] / 12), esc(cdisp(r["cause"])),
                  esc(r["limittype"] or "–"), describe(r["description"], r["definition_rule"])]
                 for _, r in cons.head(AREA_CONSTRAINT_CAP).iterrows()]
        ctable = table_html([("Constraint", False), ("Hours", True), ("Cause", False),
                             ("Limit type", False), ("AEMO description", False)], crows,
                            "No constraint of this area bound in the period.")
        if len(cons) > AREA_CONSTRAINT_CAP:
            ctable += (f'<div class="sub">Showing the {AREA_CONSTRAINT_CAP} constraints with '
                       f'the most hours; {len(cons) - AREA_CONSTRAINT_CAP} others also bound.</div>')
        units = q("SELECT * FROM area_units WHERE area_id = ?", [area_id])
        wcol = "max_rel_weight" if "max_rel_weight" in units.columns else "max_factor"
        units = units.sort_values("capacity_mw", ascending=False, na_position="last")
        urows = [[link(unit_url(r["duid"], p.slug), r["site"] or r["duid"]),
                  esc(fuel_label(r["fuel"])), fnum(r["capacity_mw"]),
                  "–" if _isnan(r[wcol]) else f"{r[wcol]:.2f}", esc(r["direction"] or "–")]
                 for _, r in units.iterrows()]
        utable = table_html([("Unit", False), ("Fuel", False), ("MW", True),
                             ("Weight", True), ("Direction", False)], urows,
                            "No units recorded for this area.")
        out = area_outages(area_id, p)
        otable = table_html(
            [("Substation", False), ("Equipment", False), ("Start", False), ("End", False),
             ("Status", False), ("Reason", False), ("Binding hours (whole outage)", True)],
            outage_rows(out), "No associated outage overlapped this period.")
        head = (f'<h2>{esc(a["name"])}</h2>'
                f'<div class="sub">{esc(a["region"] or "–")} · {esc(a["limittype"] or "–")}'
                f' · <span class="mono">{esc(area_id)}</span></div>'
                + (f'<div class="sub">Most frequent constraint: {esc(desc)}</div>' if desc else
                   '<div class="sub">Most frequent constraint: description pending</div>')
                + _period_sub(p))
        view = normalise_view(view)
        switch = ('<div class="vswitch">' + "".join(
            f'<a class="vsw{" active" if v == view else ""}" href="{esc(area_url(area_id, p.slug, v))}" '
            f'hx-get="{esc(area_url(area_id, p.slug, v))}" hx-target="#tab-body" '
            f'hx-push-url="true">{lab}</a>' for v, lab in
            (("overview", "Overview"), ("equation", "Equation"))) + "</div>")
        if view == "equation":
            return wrap(head + switch
                        + equation_html(area_id, p, constraint, show_all) + note_html())
        return wrap(head + switch + tiles
                    + cause_key()
                    + '<h3>Constraint-hours by cause, per month</h3>' + cause_legend() + chart
                    + '<h3>Constraints that bound in the period</h3>' + ctable
                    + '<h3>Units behind it</h3>' + utable
                    + '<h3>Associated outages</h3>' + otable + note_html())
    except NoData as e:
        return placeholder(str(e))


# ---------------------------------------------------------------------------
# Equation view (area page)
# ---------------------------------------------------------------------------

EQUATION_NOT_BUILT = "The equation data has not been built yet."
LHS_SHOWN = 12


def equation_available() -> bool:
    t = _tables()
    return "constraint_equation" in t and "constraint_equation_meta" in t


def constraint_picker(area_id: str, p: Period) -> pd.DataFrame:
    """The area's constraints that bound in the period, most energy held back
    first, then most hours."""
    e = "SUM(d.constrained_mwh)" if "constrained_mwh" in _columns("constraint_daily") else "NULL"
    df = q(f"""SELECT d.constraintid, SUM(d.n_intervals) AS n, {e} AS mwh
                 FROM constraint_daily d JOIN constraint_meta m USING (constraintid)
                WHERE m.area_id = ? AND d.day BETWEEN ? AND ?
                GROUP BY d.constraintid
                ORDER BY mwh DESC NULLS LAST, n DESC, d.constraintid""",
           [area_id, p.start, p.end])
    df["hours"] = df["n"] / 12
    df["gwh"] = df["mwh"] / 1000
    return df


def _lead_constraint(area_id: str) -> str | None:
    if "lead_constraintid" not in _columns("area"):
        return None
    r = q("SELECT lead_constraintid FROM area WHERE area_id = ?", [area_id])
    if r.empty or _isnan(r.iloc[0]["lead_constraintid"]):
        return None
    return str(r.iloc[0]["lead_constraintid"])


def _belongs(area_id: str, constraintid: str) -> bool:
    return not q("SELECT 1 FROM constraint_meta WHERE constraintid = ? AND area_id = ?",
                 [constraintid, area_id]).empty


def equation_block(meta: dict) -> str:
    """The two-column left <= right block, worded from the meta row."""
    rel = {"<=": "≤", ">=": "≥"}.get(str(meta.get("constrainttype")), "=")
    n_u = int(meta.get("n_lhs_units") or 0)
    n_i = int(meta.get("n_lhs_interconnectors") or 0)
    n_fb = int(meta.get("n_feedback") or 0)
    scale = meta.get("scale")
    scale = 1.0 if _isnan(scale) else float(scale)
    left = f"weighted output of {n_u} generators and batteries"
    if n_i:
        left += f" + weighted flow on {n_i} interconnector{'s' if n_i != 1 else ''}"
    right = (f"{fw(scale, 3)} × " if abs(scale - 1) > 1e-9 else "") \
        + "( the limit − measured flows − margin )"
    if n_fb:
        right += f" + the same weighted sum at current output ({n_fb} terms)"
    return (f'<div class="eq"><div><span class="lab">Left side</span>{esc(left)}</div>'
            f'<div class="rel">{rel}</div>'
            f'<div><span class="lab">Right side</span>{esc(right)}</div></div>')


def lhs_rows(constraintid: str, p: Period) -> pd.DataFrame:
    """Left-side terms: units by largest absolute weight, then interconnectors.
    Adds absf and gwh (energy held back in the period across all limits)."""
    df = q("""SELECT * FROM constraint_equation
               WHERE constraintid = ? AND side = 'lhs' AND kind IN ('unit', 'interconnector')
               ORDER BY ord""", [constraintid])
    if df.empty:
        return df.assign(absf=[], gwh=[])
    ud = q("""SELECT duid, SUM(constrained_mwh) / 1000 AS gwh FROM unit_daily
               WHERE day BETWEEN ? AND ? GROUP BY duid""", [p.start, p.end])
    df["gwh"] = df["duid"].map(dict(zip(ud["duid"], ud["gwh"])))
    df["absf"] = df["factor"].abs()
    df["_ic"] = (df["kind"] == "interconnector").astype(int)
    df = df.sort_values(["_ic", "absf", "ord"], ascending=[True, False, True],
                        kind="stable").drop(columns="_ic")
    return df.reset_index(drop=True)


def _reading(r) -> str:
    if "reading" in r.index and not _isnan(r["reading"]) and str(r["reading"]).strip():
        return esc(r["reading"])
    f = r["factor"]
    if _isnan(f):
        return "–"
    return (f"Each extra MW adds {fw(f, 2)} to the left side" if f > 0
            else f"Each extra MW takes {fw(-f, 2)} off the left side")


def _hour_bars(constraintid: str) -> str:
    h = q("SELECT hour, n_intervals FROM constraint_hour WHERE constraintid = ?",
          [constraintid])
    s = h.set_index("hour")["n_intervals"].reindex(range(24)).fillna(0)
    mx = max(float(s.max()), 1.0)
    bars = "".join(f'<span title="{i:02d}:00  {fnum(x / 12)} h" '
                   f'style="height:{max(2, 46 * x / mx):.0f}px"></span>' for i, x in s.items())
    return (f'<div class="hbars">{bars}</div><div class="haxis"><span>00</span><span>06</span>'
            '<span>12</span><span>18</span><span>23</span></div>')


def equation_html(area_id: str, p: Period, constraint: str | None, show_all: bool) -> str:
    if not equation_available():
        return f'<div class="placeholder">{EQUATION_NOT_BUILT}</div>'
    pk = constraint_picker(area_id, p)
    chosen = None
    if constraint and _belongs(area_id, constraint):
        chosen = constraint
    if chosen is None:
        lead = _lead_constraint(area_id)
        chosen = lead if lead and _belongs(area_id, lead) else (
            pk.iloc[0]["constraintid"] if len(pk) else None)
    if chosen is None:
        return '<div class="sub">No constraint of this area bound in the period.</div>'
    picks = "".join(
        f'<a class="{"on" if r["constraintid"] == chosen else ""}" '
        f'href="{esc(area_url(area_id, p.slug, "equation", r["constraintid"]))}" '
        f'hx-get="{esc(area_url(area_id, p.slug, "equation", r["constraintid"]))}" '
        f'hx-target="#tab-body" hx-push-url="true">{esc(r["constraintid"])} · '
        f'{"–" if _isnan(r["gwh"]) else fgwh(r["gwh"])} GWh · {fhours(r["hours"])} h</a>'
        for _, r in pk.head(20).iterrows())
    picker = '<h3>Constraint</h3><div class="cpick">' + picks + "</div>"
    mcols = _columns("constraint_equation_meta")
    order = " ORDER BY effectivedate DESC" if "effectivedate" in mcols else ""
    meta = q(f"SELECT * FROM constraint_equation_meta WHERE constraintid = ?{order} LIMIT 1",
             [chosen])
    if meta.empty:
        return picker + '<div class="sub">No equation is stored for this constraint.</div>'
    meta = meta.iloc[0].to_dict()
    cm = q("SELECT limittype, cause, description, definition_rule FROM constraint_meta "
           "WHERE constraintid = ?", [chosen])
    cm = cm.iloc[0] if len(cm) else None
    hrs = pk.loc[pk["constraintid"] == chosen, "hours"]
    lead = (f'<div class="lead"><span class="lab">Constraint</span> '
            f'<span class="idm">{esc(chosen)}</span>'
            + (f' · bound for {fhours(float(hrs.iloc[0]))} hours' if len(hrs) else "")
            + (f' · {esc(cm["limittype"] or "–")} · {esc(cdisp(cm["cause"]))}' if cm is not None else "")
            + ('<br><span class="muted">AEMO: “' + esc(re.sub(r"\s+", " ", str(cm["description"]))) + '”</span>'
               if cm is not None and not _isnan(cm["description"])
               and cm["definition_rule"] != "none" else "") + "</div>")
    # plain English
    note_html_ = ""
    if "area_note" in _tables():
        nt = q("SELECT * FROM area_note WHERE area_id = ? AND constraintid = ?",
               [area_id, chosen])
        if len(nt) and not _isnan(nt.iloc[0]["note"]) and str(nt.iloc[0]["note"]).strip():
            n0 = nt.iloc[0]
            stale = "is_stale" in nt.columns and bool(n0["is_stale"]) \
                and not _isnan(n0["is_stale"])
            banner = ""
            if stale:
                d = "" if _isnan(n0.get("written_effectivedate")) \
                    else pd.Timestamp(n0["written_effectivedate"]).strftime("%Y-%m-%d")
                banner = (f'<p class="stale">Written for the version effective {d}. AEMO has '
                          "since changed this equation's structure; the paragraph is awaiting "
                          "review.</p>")
            note_html_ = (f'<h3>In plain English</h3>{banner}'
                          f'<p class="note-eq">{esc(n0["note"])}</p>')
    # equation and its sentence
    med, p10, p90 = meta.get("rhs_med"), meta.get("rhs_p10"), meta.get("rhs_p90")
    sent = "When the constraint binds, the two sides are equal."
    if not _isnan(med):
        sent += f" Over the year the right side sat at {fw(med, 0)} (middle value)"
        if not _isnan(p10) and not _isnan(p90):
            sent += f", between {fw(p10, 0)} and {fw(p90, 0)} for 80% of binding intervals"
        sent += "."
    if meta.get("has_operations") is not None and not _isnan(meta.get("has_operations")) \
            and bool(meta["has_operations"]):
        sent += (" The right side chooses between alternatives with a short sequence of "
                 "operations; the rows marked as calculation steps show it.")
    eq = (f'<h3>The equation</h3>{equation_block(meta)}<p class="sub">{esc(sent)}</p>')
    # left side
    rows = lhs_rows(chosen, p)
    units = rows[rows["kind"] == "unit"]
    ics = rows[rows["kind"] == "interconnector"]
    n_pos, n_neg = int((units["factor"] > 0).sum()), int((units["factor"] < 0).sum())
    shown = units if show_all else units.head(LHS_SHOWN)
    more = len(units) - len(shown)
    lhs_rows_html = []
    for _, r in shown.iterrows():
        gwh = r["gwh"]
        lhs_rows_html.append([
            esc(r["site"] if not _isnan(r["site"]) else r["duid"] if not _isnan(r["duid"]) else r["spd_id"]),
            esc(fuel_label(r["fuel"])),
            "–" if _isnan(r["capacity_mw"]) else fw(r["capacity_mw"], 0),
            fw(r["factor"], 3), _reading(r),
            "–" if _isnan(gwh) or gwh < 0.05 else fgwh(gwh)])
    for _, r in ics.iterrows():
        lhs_rows_html.append([
            esc(r["aemo_description"] if not _isnan(r["aemo_description"]) else r["spd_id"]),
            "interconnector", "", fw(r["factor"], 3), _reading(r), ""])
    counts = (f"{n_pos} units add to the left side and {n_neg} reduce it. "
              + (f"All {len(units)} are shown." if show_all or len(units) <= LHS_SHOWN else
                 f"The {LHS_SHOWN} largest weights are shown, of {len(units)}.")
              + " A weight near 1 means the unit’s output flows almost entirely through the "
                "limited equipment.")
    left = (f'<h3>Left side: what AEMO can change</h3><p class="sub">{esc(counts)}</p>'
            + table_html([("Unit", False), ("Fuel", False), ("MW", True), ("Weight", True),
                          ("Reading", False), ("GWh held back, all limits", True)], lhs_rows_html,
                         "No left-side terms are stored."))
    if more > 0:
        left += (f'<div class="sub">{more} more unit{"s" if more != 1 else ""} not shown. '
                 + link(area_url(area_id, p.slug, "equation", chosen, True), "Show all")
                 + "</div>")
    # right side
    rcols = _columns("constraint_equation")
    fb_clause = " AND NOT COALESCE(is_feedback, false)" if "is_feedback" in rcols else ""
    rhs = q(f"SELECT * FROM constraint_equation WHERE constraintid = ? AND side = 'rhs'"
            f"{fb_clause} ORDER BY ord", [chosen])
    rrows = [[f'<span class="mono">{esc(r["spd_id"])}</span>' if False else esc(r["spd_id"]),
              fw(r["factor"], 3), _reading(r)] for _, r in rhs.iterrows()]
    rtable = table_html([("AEMO’s term", False, "mono"), ("Weight", True),
                         ("What it is", False)], rrows, "No right-side terms are stored.")
    n_fb = int(meta.get("n_feedback") or 0)
    right = ("<h3>Right side: the limit</h3>" + rtable
             + (f'<p class="sub">The right side also repeats {n_fb} of the left-side terms with '
                "the same weights, measured at their current output. This is what AEMO calls a "
                "feedback constraint: the limit each five minutes is what the units are producing "
                "now plus the room left on the equipment.</p>" if n_fb else ""))
    hours = "<h3>When it binds, by hour of day</h3>" + _hour_bars(chosen)
    ed = meta.get("effectivedate")
    ed_txt = "" if _isnan(ed) else pd.Timestamp(ed).strftime("%Y-%m-%d")
    vn = meta.get("versionno")
    rule = ("the exact version" if meta.get("version_rule") == "exact" else
            "an earlier version; AEMO's archive does not yet hold the current one")
    foot = (f'<p class="sub">Constraint version effective {ed_txt}'
            + ("" if _isnan(vn) else f" (version {int(vn)})")
            + f". This is {rule}. Right-side terms from AEMO's GENERICCONSTRAINTRHS table, "
              "dispatch scope; descriptions from AEMO's EMSMASTER table.</p>")
    return picker + lead + note_html_ + eq + left + right + hours + foot


# ---------------------------------------------------------------------------
# Causes
# ---------------------------------------------------------------------------

def cause_totals(p: Period, region: str) -> dict:
    """Constraint-interval counts: by_cause, by_limittype, by_region_cause."""
    reg_sql, params = "", [p.start, p.end]
    ar = area_region(region)
    if ar:
        reg_sql, params = "AND region = ?", params + [ar]
    df = q(f"""SELECT date_trunc('month', day) AS month, region, cause, limittype,
                      SUM(n_constraint_intervals) AS n
                 FROM cause_daily WHERE day BETWEEN ? AND ? {reg_sql}
                GROUP BY ALL""", params)
    df["month"] = pd.to_datetime(df["month"])
    by_cause = {c: int(df.loc[df["cause"] == c, "n"].sum()) for c in CAUSES}
    by_lt = df.groupby("limittype")["n"].sum().sort_values(ascending=False)
    by_rc = df.groupby(["region", "cause"])["n"].sum()
    return {"by_cause": by_cause, "by_limittype": by_lt, "by_region_cause": by_rc,
            "monthly": df}


def top_outages(p: Period, region: str, limit: int = 30) -> pd.DataFrame:
    reg_sql, params = "", [p.end + timedelta(days=1), p.start]
    ar = area_region(region)
    if ar:
        reg_sql, params = "AND a.region = ?", params + [ar]
    df = q(f"""
        WITH o AS (
            SELECT o.outageid, o.area_id,
                   string_agg(DISTINCT CASE WHEN substationid IN ('N/A', '') THEN NULL
                                            ELSE substationid END, ', ') AS substation,
                   string_agg(DISTINCT equipmenttype || ' ' || equipmentid, ', ') AS equipment,
                   MIN(starttime) AS starttime, MAX(endtime) AS endtime,
                   ANY_VALUE(outagestatuscode) AS status, ANY_VALUE(reason) AS reason,
                   MAX(n_binding_intervals) AS n
              FROM outage_area o JOIN area a USING (area_id)
             WHERE outagestatuscode <> 'WDRAWN' AND n_binding_intervals > 0
               AND starttime < ? AND endtime >= ? {reg_sql}
             GROUP BY o.outageid, o.area_id),
        best AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY outageid
                                              ORDER BY n DESC, area_id) AS rk FROM o)
        SELECT best.*, COALESCE(a.name, best.area_id) AS area_name
          FROM best LEFT JOIN area a USING (area_id)
         WHERE rk = 1 ORDER BY n DESC, outageid LIMIT {int(limit)}""", params)
    return df


def causes_html(region: str, range_slug: str) -> str:
    try:
        p = get_period(range_slug)
        t = cause_totals(p, region)
        if not sum(t["by_cause"].values()):
            return placeholder(f"No constraint bound in {_region_text(region)} "
                               f"for the {p.label}.")
        lt = t["by_limittype"]
        c1 = hbar(list(lt.index), list((lt / 12).round(1)), "#205EA6", 300)
        c2 = hbar([CAUSE_DISPLAY[c] for c in CAUSES], [t["by_cause"][c] / 12 for c in CAUSES],
                  [CAUSE_COLORS[c] for c in CAUSES], 300)
        rc = t["by_region_cause"]
        regs = [r for r in ("NSW", "QLD", "VIC", "SA", "TAS", "Other")
                if r in rc.index.get_level_values(0)]
        fig = go.Figure()
        for c in CAUSES:
            fig.add_trace(go.Bar(
                y=regs, x=[rc.get((r, c), 0) / 12 for r in regs], name=CAUSE_DISPLAY[c],
                orientation="h", marker_color=CAUSE_COLORS[c],
                hovertemplate=f"{CAUSE_DISPLAY[c]}<br>%{{y}}: %{{x:,.0f}} h<extra></extra>"))
        _layout(fig, 300, barmode="stack", showlegend=False)
        fig.update_layout(margin=dict(l=8, r=12, t=8, b=36))
        fig.update_yaxes(autorange="reversed", automargin=True)
        fig.update_xaxes(title=dict(text="constraint-hours",
                                    font=dict(size=11, color=MUTED)), tickformat=",.0f")
        c3 = plot_html(fig, 300)
        mo = t["monthly"]
        series = {c: _monthly(mo[mo["cause"] == c], "month", "n") / 12 for c in CAUSES}
        monthly = stacked_monthly(p, series, CAUSE_COLORS, "constraint-hours per month", 320)
        out = top_outages(p, region)
        if not out.empty:
            out["area_html"] = [link(area_url(r["area_id"], p.slug), r["area_name"])
                                for _, r in out.iterrows()]
        otable = table_html(
            [("Area", False), ("Substation", False), ("Equipment", False), ("Start", False),
             ("End", False), ("Status", False), ("Reason", False),
             ("Binding hours (whole outage)", True)],
            outage_rows(out, with_area=True) if not out.empty else [],
            "No outage with binding constraints overlapped this period.")
        return wrap(
            _period_sub(p, f"{_region_text(region)} constraint causes")
            + cause_key() + cause_legend()
            + '<div class="row3">'
              '<div><h3>By limit type</h3>' + c1 + '</div>'
              '<div><h3>By cause</h3>' + c2 + '</div>'
              '<div><h3>By region and cause</h3>' + c3 + '</div></div>'
            + '<h3>Constraint-hours by cause, per month</h3>' + monthly
            + '<h3>The 30 outages with the most binding hours</h3>' + otable
            + '<div class="sub">Each outage is listed once, against the area it affects '
              'most. Hours are for the whole outage.</div>'
            + note_html())
    except NoData as e:
        return placeholder(str(e))


# ---------------------------------------------------------------------------
# Units
# ---------------------------------------------------------------------------

def load_mapping() -> pd.DataFrame:
    try:
        m = q('SELECT duid, region, "site name" AS site, owner, capacity_mw, '
              "storage_mwh, fuel FROM duid_mapping", db="aemo")
    except Exception:
        m = pd.DataFrame(columns=["duid", "region", "site", "owner", "capacity_mw",
                                  "storage_mwh", "fuel"])
    return m


def weighted_median(values: pd.Series, weights: pd.Series) -> float:
    ok = values.notna() & weights.notna() & (weights > 0)
    if not ok.any():
        return float("nan")
    v, w = values[ok].to_numpy(), weights[ok].to_numpy()
    order = np.argsort(v)
    v, w = v[order], w[order]
    cum = np.cumsum(w)
    return float(v[np.searchsorted(cum, cum[-1] / 2)])


def unit_totals(p: Period) -> pd.DataFrame:
    ud = q("""SELECT day, duid, n_discharge_limited AS dis, n_charge_limited AS chg,
                     constrained_mwh AS mwh, other_curtailed_mwh AS other,
                     constrained_value AS val, n_local_price AS nlp,
                     median_local_price_adj AS adj
                FROM unit_daily WHERE day BETWEEN ? AND ?""", [p.start, p.end])
    if ud.empty:
        return pd.DataFrame(columns=["duid", "dis", "chg", "mwh", "other", "val",
                                     "pct_dis", "pct_chg", "gwh_con", "gwh_other",
                                     "value_m", "median_adj"])
    g = ud.groupby("duid")
    out = g[["dis", "chg", "mwh", "other", "val"]].sum(min_count=1).reset_index()
    adj = {k: weighted_median(d["adj"], d["nlp"]) for k, d in g}
    out["median_adj"] = out["duid"].map(adj)
    out["pct_dis"] = out["dis"] / p.n_intervals * 100
    out["pct_chg"] = out["chg"] / p.n_intervals * 100
    out["gwh_con"] = out["mwh"] / 1000
    out["gwh_other"] = out["other"] / 1000
    out["value_m"] = out["val"] / 1e6
    return out


def unit_league(p: Period, region: str, fuel: str, limit: int = 40) -> pd.DataFrame:
    tot = unit_totals(p)
    m = load_mapping()
    df = tot.merge(m, on="duid", how="left")
    df["fuel_group"] = df["fuel"].map(fuel_group)
    if region != "NEM":
        df = df[df["region"] == region]
    if fuel != "all":
        df = df[df["fuel_group"] == fuel]
    df = df[~(df["capacity_mw"] < MIN_UNIT_MW)]            # unknown capacity stays
    df = df[df["dis"].fillna(0) + df["chg"].fillna(0) > 0]
    df = df.sort_values(["pct_dis", "duid"], ascending=[False, True]).head(limit)
    return df.reset_index(drop=True)


def top_area_names(duids: list[str], p: Period) -> dict[str, str]:
    if not duids:
        return {}
    ph = ",".join("?" * len(duids))
    df = q(f"""
        WITH u AS (SELECT duid, constraintid, SUM(n_intervals) AS n
                     FROM unit_constraint_daily
                    WHERE day BETWEEN ? AND ? AND duid IN ({ph}) GROUP BY ALL),
        a AS (SELECT u.duid, m.area_id, SUM(u.n) AS n
                FROM u JOIN constraint_meta m USING (constraintid) GROUP BY ALL),
        r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY duid ORDER BY n DESC, area_id) AS rk
                FROM a)
        SELECT r.duid, r.area_id, COALESCE(area.name, r.area_id) AS name
          FROM r LEFT JOIN area USING (area_id) WHERE rk = 1""",
          [p.start, p.end] + duids)
    return {row["duid"]: (row["area_id"], row["name"]) for _, row in df.iterrows()}


def units_html(region: str, fuel: str, range_slug: str) -> str:
    try:
        p = get_period(range_slug)
        df = unit_league(p, region, fuel)
        if df.empty:
            return placeholder("No unit of this kind was limited by a network constraint "
                               f"in {_region_text(region)} for the {p.label}.")
        battery = fuel == "battery"
        areas = top_area_names(list(df["duid"]), p)
        heads = [("Unit", False), ("Region", False), ("Fuel", False), ("MW", True),
                 ("% intervals discharge-limited" if battery else "% intervals limited", True)]
        if battery:
            heads.append(("% intervals charge-limited", True))
        heads += [("GWh curtailed while constrained", True), ("GWh other curtailment", True),
                  ("$m", True), ("Median local price adj. $/MWh", True),
                  ("Most frequent constraint area", False)]
        rows = []
        for _, r in df.iterrows():
            adj = r["median_adj"]
            a = areas.get(r["duid"])
            row = [link(unit_url(r["duid"], p.slug), r["site"] if not _isnan(r["site"]) else r["duid"]),
                   esc((r["region"] or "–")[:-1] if not _isnan(r["region"]) else "–"),
                   esc(fuel_label(r["fuel"])), fnum(r["capacity_mw"]),
                   fpct(r["pct_dis"])]
            if battery:
                row.append(fpct(r["pct_chg"]))
            row += [fgwh(r["gwh_con"]), fgwh(r["gwh_other"]), fnum(r["value_m"], 1),
                    fsigned(adj) if not _isnan(adj) and adj < 0 else "–",
                    link(area_url(a[0], p.slug), a[1]) if a else "–"]
            rows.append(row)
        table = table_html(heads, rows)
        return wrap(
            _period_sub(p, f"{_region_text(region)} {dict(FUELS)[fuel].lower()} units")
            + '<h3>Top 40 units by share of intervals limited</h3>' + table
            + f'<div class="sub">Units under {MIN_UNIT_MW:.0f} MW are left out. Curtailment '
              'columns cover the period from 28 Feb 2025, when curtailment data begins. '
              'The local price adjustment is AEMO’s published figure; only negative '
              'medians are shown.</div>'
            + note_html())
    except NoData as e:
        return placeholder(str(e))


# ---------------------------------------------------------------------------
# One unit
# ---------------------------------------------------------------------------

def unit_hour_profile(duid: str, p: Period) -> pd.DataFrame:
    """Share of intervals limited by hour of day, over the calendar months the
    period touches (the table is monthly)."""
    first = pd.Timestamp(p.start).replace(day=1).date()
    last = (pd.Timestamp(p.end) + pd.offsets.MonthEnd(0)).date()
    d = days_in_months(p)
    df = q("""SELECT hour, direction, SUM(n_intervals) AS n FROM unit_hour_month
               WHERE duid = ? AND month BETWEEN ? AND ? GROUP BY ALL""",
           [duid, first, last])
    out = pd.DataFrame({"hour": range(24)})
    for dr in ("discharge", "charge"):
        s = df[df["direction"] == dr].set_index("hour")["n"]
        out[dr] = out["hour"].map(s).fillna(0.0) / (d * 12) * 100 if d else 0.0
    return out


def unit_html(duid: str, range_slug: str) -> str:
    try:
        p = get_period(range_slug)
        m = load_mapping()
        m = m[m["duid"] == duid]
        in_summary = int(q("SELECT COUNT(*) AS n FROM unit_daily WHERE duid = ?",
                           [duid]).iloc[0]["n"]) + int(
            q("SELECT COUNT(*) AS n FROM area_units WHERE duid = ?", [duid]).iloc[0]["n"])
        if m.empty and not in_summary:
            return placeholder(f"No unit with id “{duid}”.")
        info = m.iloc[0] if len(m) else None
        site = info["site"] if info is not None and not _isnan(info["site"]) else duid
        fuel = fuel_label(info["fuel"]) if info is not None else "–"
        is_batt = fuel == "Battery"
        cap = info["capacity_mw"] if info is not None else float("nan")
        reg = info["region"] if info is not None else None
        ud = q("""SELECT day, n_discharge_limited AS dis, n_charge_limited AS chg,
                         constrained_mwh AS mwh, other_curtailed_mwh AS other,
                         constrained_value AS val, n_local_price AS nlp,
                         median_local_price_adj AS adj
                    FROM unit_daily WHERE duid = ? AND day BETWEEN ? AND ?""",
               [duid, p.start, p.end])
        dis, chg = float(ud["dis"].sum()), float(ud["chg"].sum())
        adj = weighted_median(ud["adj"], ud["nlp"]) if len(ud) else float("nan")
        tiles = [("Capacity", f"{fnum(cap)} MW"),
                 ("Intervals discharge-limited" if is_batt else "Intervals limited",
                  fpct(dis / p.n_intervals * 100))]
        if is_batt:
            tiles.append(("Intervals charge-limited", fpct(chg / p.n_intervals * 100)))
        tiles += [("Curtailed while constrained", f"{fgwh(ud['mwh'].sum() / 1000)} GWh"),
                  ("Other curtailment", f"{fgwh(ud['other'].sum() / 1000)} GWh"),
                  ("Value of constrained curtailment", fmoney_m(ud["val"].sum() / 1e6)),
                  ("Median local price adj.",
                   f"{fsigned(adj)} $/MWh" if not _isnan(adj) and adj < 0 else "–")]
        prof = unit_hour_profile(duid, p)
        fig = go.Figure()
        fig.add_trace(go.Bar(x=prof["hour"], y=prof["discharge"],
                             name="Discharge-limited" if is_batt else "Limited",
                             marker_color=FUEL_COLORS.get(fuel, "#6F6E69"),
                             hovertemplate="%{x}:00<br>%{y:.1f}% of intervals<extra></extra>"))
        if is_batt:
            fig.add_trace(go.Bar(x=prof["hour"], y=prof["charge"], name="Charge-limited",
                                 marker_color="#24837B",
                                 hovertemplate="%{x}:00<br>%{y:.1f}% of intervals<extra></extra>"))
        _layout(fig, 300, barmode="group", bargap=0.15)
        fig.update_xaxes(tickvals=list(range(0, 24, 3)),
                         ticktext=[f"{h:02d}:00" for h in range(0, 24, 3)],
                         title=dict(text="Hour of day (NEM time)",
                                    font=dict(size=11, color=MUTED)))
        fig.update_yaxes(title=dict(text="% of intervals", font=dict(size=11, color=MUTED)),
                         ticksuffix="%")
        tod = plot_html(fig, 300)
        # monthly trend: % of the month's intervals
        mi = q("""SELECT date_trunc('month', day) AS month, SUM(n_intervals) AS n
                    FROM intervals_daily WHERE day BETWEEN ? AND ? GROUP BY 1""",
               [p.start, p.end]).set_index("month")["n"]
        ud["month"] = pd.to_datetime(ud["day"]).dt.to_period("M").dt.to_timestamp()
        mm = ud.groupby("month")[["dis", "chg"]].sum()
        months = _month_axis(p)
        fig2 = go.Figure()
        for col, name, color in [("dis", "Discharge-limited" if is_batt else "Limited",
                                  FUEL_COLORS.get(fuel, "#6F6E69")),
                                 ("chg", "Charge-limited", "#24837B")]:
            if col == "chg" and not is_batt:
                continue
            y = (mm[col].reindex(months).fillna(0) / mi.reindex(months) * 100)
            fig2.add_trace(go.Scatter(
                x=months, y=y, name=name, mode="lines+markers",
                line=dict(color=color, width=2), marker=dict(size=5),
                hovertemplate="%{x|%b %Y}: %{y:.1f}%<extra></extra>"))
        _layout(fig2, 280)
        fig2.update_xaxes(tickformat="%b %Y")
        fig2.update_yaxes(title=dict(text="% of intervals", font=dict(size=11, color=MUTED)),
                          ticksuffix="%", rangemode="tozero")
        trend = plot_html(fig2, 280)
        cons = q("""SELECT d.constraintid, d.direction, SUM(d.n_intervals) AS n, m.area_id,
                           COALESCE(a.name, m.area_id) AS area_name, m.description,
                           m.definition_rule
                      FROM unit_constraint_daily d
                      LEFT JOIN constraint_meta m USING (constraintid)
                      LEFT JOIN area a ON a.area_id = m.area_id
                     WHERE d.duid = ? AND d.day BETWEEN ? AND ?
                     GROUP BY ALL ORDER BY n DESC, d.constraintid""",
                 [duid, p.start, p.end])
        n_cons = len(cons)
        crows = [[esc(r["constraintid"]),
                  link(area_url(r["area_id"], p.slug), r["area_name"])
                  if not _isnan(r["area_id"]) else "–",
                  fhours(r["n"] / 12), esc(r["direction"]),
                  describe(r["description"], r["definition_rule"])]
                 for _, r in cons.head(UNIT_CONSTRAINT_CAP).iterrows()]
        ctable = table_html([("Constraint", False), ("Area", False, "wrap260"),
                             ("Hours", True), ("Direction", False),
                             ("AEMO description", False)], crows,
                            "No network constraint included this unit in the period.")
        if n_cons > UNIT_CONSTRAINT_CAP:
            ctable += (f'<div class="sub">Showing the {UNIT_CONSTRAINT_CAP} constraints with '
                       f'the most hours; {n_cons - UNIT_CONSTRAINT_CAP} others also included '
                       f'this unit.</div>')
        sub = (f'{esc(fuel)} · {esc((reg or "–")[:-1] if reg else "–")} · {esc(duid)}')
        return wrap(f'<h2>{esc(site)}</h2><div class="sub">{sub}</div>'
                    + _period_sub(p)
                    + tiles_html(tiles)
                    + '<h3>Share of intervals limited, by hour of day</h3>' + tod
                    + '<div class="sub">Calendar months that overlap the period.</div>'
                    + '<h3>Share of intervals limited, by month</h3>' + trend
                    + '<h3>Constraints behind it</h3>' + ctable + note_html())
    except NoData as e:
        return placeholder(str(e))


# ---------------------------------------------------------------------------
# Now
# ---------------------------------------------------------------------------

# Transmission outage status codes found in network_outagedetail on 24 Nov 2026:
#   WDRAWN  withdrawn          -> excluded
#   WD REQ  withdrawal request -> excluded
#   COMPLETE finished          -> excluded
#   RESUBMIT superseded by a resubmission -> excluded
#   UTP     unable to proceed  -> excluded
#   SUBMIT, INFO, PTP, PTR, PDLTP, MTLTP, STLTP -> counted as in progress
OUTAGE_EXCLUDED = ("WDRAWN", "WD REQ", "COMPLETE", "RESUBMIT", "UTP")


def outages_in_progress(now: pd.Timestamp, limit: int = OUTAGE_NOW_CAP) -> pd.DataFrame:
    """Outages in progress, those that have invoked a constraint set first, then
    most recently started. df.attrs['total'] = count before the cap."""
    ph = ",".join("?" * len(OUTAGE_EXCLUDED))
    df = q(f"""
        WITH o AS (
            SELECT outageid,
                   string_agg(DISTINCT CASE WHEN substationid IN ('N/A', '') THEN NULL
                                            ELSE substationid END, ', ') AS substation,
                   string_agg(DISTINCT equipmenttype || ' ' || equipmentid, ', ') AS equipment,
                   MIN(starttime) AS starttime, MAX(endtime) AS endtime,
                   ANY_VALUE(outagestatuscode) AS status, ANY_VALUE(reason) AS reason
              FROM network_outagedetail
             WHERE starttime <= ? AND endtime >= ?
               AND outagestatuscode NOT IN ({ph})
               AND (actual_endtime IS NULL OR actual_endtime > ?)
               AND COALESCE(NULLIF(TRIM(equipmentid), ''), 'N/A') <> 'N/A'
             GROUP BY outageid),
        cs AS (
            SELECT outageid, string_agg(DISTINCT genconsetid, ', ') AS constraint_set
              FROM network_outageconstraintset
             WHERE startinterval <= ? AND endinterval >= ? GROUP BY outageid)
        SELECT o.*, cs.constraint_set FROM o LEFT JOIN cs USING (outageid)
         ORDER BY (cs.constraint_set IS NULL), o.starttime DESC, o.outageid DESC""",
             [now, now, *OUTAGE_EXCLUDED, now, now, now], db="constraints")
    total = len(df)
    df = df.head(int(limit)).reset_index(drop=True)
    df.attrs["total"] = total
    return df


def mv_cell(v) -> str:
    if _isnan(v):
        return "–"
    if abs(v) >= VIOLATION_PENALTY_MV:
        return '<span class="muted">at violation penalty</span>'
    return fsigned(v, 1)


def binding_now() -> tuple[pd.Timestamp, pd.DataFrame]:
    ts = q("SELECT MAX(settlementdate) AS ts FROM constraint_binding5",
           db="constraints").iloc[0]["ts"]
    if _isnan(ts):
        raise NoData("No live constraint data yet.")
    b = q("""SELECT constraintid, marginalvalue FROM constraint_binding5
              WHERE settlementdate = ? AND intervention = 0 AND marginalvalue <> 0
                AND constraintid NOT LIKE 'F\\_%' ESCAPE '\\'
                AND constraintid NOT LIKE 'F-%'
                AND constraintid NOT LIKE '#%' AND constraintid NOT LIKE '$%'""",
          [ts], db="constraints")
    meta = q("""SELECT c.constraintid, c.cause, c.limittype, c.description, c.definition_rule,
                       c.area_id, a.name AS area_name
                  FROM constraint_meta c LEFT JOIN area a USING (area_id)""")
    b = b.merge(meta, on="constraintid", how="left")
    b["abs_mv"] = b["marginalvalue"].abs()
    return pd.Timestamp(ts), b.sort_values(["abs_mv", "constraintid"],
                                           ascending=[False, True])


def now_html() -> str:
    try:
        now = _nem_now()
        ts, b = binding_now()
        age = max(0, int(round((now - ts).total_seconds() / 60)))
        rng = DEFAULT_RANGE
        brows = []
        for _, r in b.iterrows():
            brows.append([
                esc(r["constraintid"]),
                link(area_url(r["area_id"], rng), r["area_name"])
                if not _isnan(r["area_id"]) else "–",
                esc(cdisp(r["cause"])),
                esc(r["limittype"] if not _isnan(r["limittype"]) else "–"),
                describe(r["description"], r["definition_rule"]),
                mv_cell(r["marginalvalue"])])
        btable = table_html([("Constraint", False), ("Area", False), ("Cause", False),
                             ("Limit type", False), ("AEMO description", False),
                             ("Marginal value $/MWh", True)], brows,
                            "No network constraint is binding in this interval.")
        lp = q("""SELECT settlementdate, duid, local_price_adjustment AS adj FROM local_price5
                   WHERE settlementdate = (SELECT MAX(settlementdate) FROM local_price5)
                     AND local_price_adjustment IS NOT NULL
                   ORDER BY ABS(local_price_adjustment) DESC, duid LIMIT 30""",
               db="constraints")
        urows, lp_ts = [], None
        if len(lp):
            lp_ts = pd.Timestamp(lp.iloc[0]["settlementdate"])
            lp = lp.merge(load_mapping(), on="duid", how="left")
            for _, r in lp.iterrows():
                urows.append([
                    link(unit_url(r["duid"], rng),
                         r["site"] if not _isnan(r["site"]) else r["duid"]),
                    esc((r["region"] or "–")[:-1] if not _isnan(r["region"]) else "–"),
                    esc(fuel_label(r["fuel"])), fnum(r["capacity_mw"]), fsigned(r["adj"], 1)])
        utable = table_html([("Unit", False), ("Region", False), ("Fuel", False),
                             ("MW", True), ("Local price adj. $/MWh", True)], urows,
                            "No local price adjustment in the latest interval.")
        out = outages_in_progress(now)
        n_out = out.attrs.get("total", len(out))
        orows = [[esc(r["substation"] or "–"), esc(r["equipment"]), fdt(r["starttime"]),
                  fdt(r["endtime"]), esc(r["status"]), esc(r["reason"] or "–"),
                  esc(r["constraint_set"]) if not _isnan(r["constraint_set"]) else "–"]
                 for _, r in out.iterrows()]
        otable = table_html([("Substation", False), ("Equipment", False), ("Start", False),
                             ("End", False), ("Status", False), ("Reason", False),
                             ("Constraint set", False)], orows,
                            "No transmission outage is in progress.")
        lp_note = (f" (local price adjustments: interval ending {fdt(lp_ts)})"
                   if lp_ts is not None and lp_ts != ts else "")
        return wrap(
            f'<div class="sub">Latest dispatch interval: ending {fdt(ts)} NEM time, '
            f'age {age} min{lp_note}.</div>'
            + tiles_html([("Binding network constraints", fnum(len(b))),
                          ("Interval age", f"{age} min"),
                          ("Units with an adjustment", fnum(len(lp))),
                          ("Outages in progress", fnum(n_out))])
            + '<h3>Binding network constraints</h3>' + btable
            + '<h3>Units with the largest local price adjustment</h3>' + utable
            + '<h3>Transmission outages in progress</h3>'
            + f'<div class="sub">{fnum(n_out)} in progress; showing '
              f'{fnum(len(out))}, those that have invoked a constraint set first, then '
              'the most recently started.</div>' + otable
            + '<div class="sub">Outages counted: start time before now, end time after now, '
              'status not WDRAWN, WD REQ, COMPLETE, RESUBMIT or UTP, and no actual end '
              'time before now. Outages with no equipment id are left out. Start and '
              'end are as submitted. Marginal values reflect the bids of constrained '
              'generators, often at the −$1,000 market floor, and are not a measure of '
              'economic cost. See also '
              f'<a href="{PASA_TRANSMISSION_URL}" hx-get="{PASA_TRANSMISSION_URL}" '
              'hx-target="#tab-body" hx-push-url="true">PASA transmission outages</a>.</div>'
            + note_html())
    except NoData as e:
        return placeholder(str(e))
    except Exception:
        return placeholder("Live constraint data is not available.")


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

GEO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_geo.json")
_geo_cache: dict = {}

MAP_RANGES = RANGES + [("now", "Now")]
MEASURES = [("pct", "% of intervals held back"),
            ("gwh", "GWh curtailed while constrained")]
MAP_BASE_VIEWS = {"NEM": (146.0, -30.5, 4.0), "NSW1": (147.5, -32.8, 5.0),
                  "QLD1": (146.0, -22.5, 4.2), "VIC1": (144.8, -37.0, 6.0),
                  "SA1": (135.0, -32.5, 4.5), "TAS1": (146.6, -42.0, 6.3)}
MAP_RAMP = ["#ECCB60", "#DA702C", "#AF3029", "#6C201C"]
UNIT_SCALE = [[0, "#E6E4D9"], [0.25, "#ECCB60"], [0.5, "#DA702C"],
              [0.75, "#AF3029"], [1, "#6C201C"]]
BACKBONE_COLOR = "#9C8F7D"
BACKBONE_WIDTH = {"220": 1.0, "275": 1.0, "330": 1.2, "500": 1.8}
HALO_COLOR = "#5E409D"
REFERENCE_LINE_COLOR = "#205EA6"
NOW_COLOR = "#AF3029"
MAP_TOP_LINES = 10
MAP_TOP_HALOS = 15
MAP_SIDE_LIST = 15
LOCAL_PRICE_CAP = 1000
MAP_SENTENCE = ("A dot shows where generation is held back; a coloured line shows the "
                "limit AEMO names. They can be far apart.")


def normalise_map_range(range_slug: str) -> str:
    return range_slug if range_slug in dict(MAP_RANGES) else DEFAULT_RANGE


def normalise_measure(measure: str) -> str:
    return measure if measure in dict(MEASURES) else "pct"


def load_geo() -> dict | None:
    """The reference geometry, cached per process and re-read when the file's
    mtime changes. None when the file is missing or unreadable."""
    try:
        mtime = os.stat(GEO_PATH).st_mtime
        if _geo_cache.get("path") == GEO_PATH and _geo_cache.get("mtime") == mtime:
            return _geo_cache["data"]
        with open(GEO_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict) or "units" not in data:
            return None
    except (OSError, ValueError):
        return None
    _geo_cache.update(path=GEO_PATH, mtime=mtime, data=data)
    return data


def _hex_rgb(c: str) -> tuple[int, int, int]:
    return int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)


def _luminance(c: str) -> float:
    r, g, b = _hex_rgb(c)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def ramp_colors(values: list[float]) -> list[str]:
    """Colour per value on the four-stop ramp, lightest = fewest hours,
    darkest = most (min-max over the values given)."""
    if not len(values):
        return []
    lo, hi = min(values), max(values)
    out = []
    for v in values:
        t = 1.0 if hi == lo else (v - lo) / (hi - lo)
        x = t * (len(MAP_RAMP) - 1)
        i = min(int(x), len(MAP_RAMP) - 2)
        f = x - i
        a, b = _hex_rgb(MAP_RAMP[i]), _hex_rgb(MAP_RAMP[i + 1])
        out.append("#%02X%02X%02X" % tuple(round(a[k] + (b[k] - a[k]) * f) for k in range(3)))
    return out


def bbox_view(points: list[tuple[float, float]], width_px: int = 900,
              height_px: int = 600, pad: float = 1.35) -> tuple[float, float, float]:
    """(lon, lat, zoom) that fits the points (MapLibre: 512 px world at zoom 0)."""
    if not points:
        lon, lat, z = MAP_BASE_VIEWS["NEM"]
        return lon, lat, z
    lons, lats = [p[0] for p in points], [p[1] for p in points]
    lon_c, lat_c = (min(lons) + max(lons)) / 2, (min(lats) + max(lats)) / 2
    lon_span = (max(lons) - min(lons)) * pad
    lat_span = (max(lats) - min(lats)) * pad / max(math.cos(math.radians(lat_c)), 0.2)
    cands = [math.log2(px * 360 / (512 * span))
             for px, span in ((width_px, lon_span), (height_px, lat_span)) if span > 1e-6]
    z = min(cands) if cands else 9.0
    return lon_c, lat_c, float(min(max(z, 3.0), 11.0))


def _feature_points(features: list[dict]) -> list[tuple[float, float]]:
    pts = []
    for f in features:
        if f.get("kind") == "substation" and f.get("point"):
            pts.append(tuple(f["point"][:2]))
        for part in f.get("parts") or []:
            pts += [tuple(c[:2]) for c in part]
    return pts


def _drawable(geo: dict, area_id: str) -> list[dict]:
    return [f for f in geo.get("area_lines", {}).get(area_id, [])
            if f.get("role") in ("protected", "interconnector")]


def top_line_areas(df: pd.DataFrame, geo: dict, n: int = MAP_TOP_LINES) -> pd.DataFrame:
    """The areas of `df` (already ranked by hours) that have a protected or
    interconnector feature, at most n."""
    keep = [a for a in df["area_id"] if _drawable(geo, a)]
    return df[df["area_id"].isin(keep)].head(n)


def area_tag(geo: dict, area_id: str) -> str:
    st = geo.get("area_status", {}).get(area_id)
    if st is None:
        return "not mapped"
    if st == "single plant":
        return "single plant"
    if st.startswith("no line"):
        return "no line"
    feats = geo.get("area_lines", {}).get(area_id, [])
    return "interconnector" if any(f.get("role") == "interconnector" for f in feats) \
        else "line"


def status_words(geo: dict, area_id: str) -> str:
    st = geo.get("area_status", {}).get(area_id)
    if st is None:
        return "Not mapped"
    if st == "single plant":
        return "A limit on a single plant"
    if st.startswith("no line"):
        reason = st.split(":", 1)[1].strip() if ":" in st else ""
        return f"No line is drawn: {reason}" if reason else "No line is drawn"
    names = []
    for f in geo.get("area_lines", {}).get(area_id, []):
        if f.get("role") in ("protected", "interconnector"):
            label = f.get("name") or "unnamed"
            label = f"{label} {f['kv']} kV" if f.get("kv") else label
            if label not in names:
                names.append(label)
    return ("Lines drawn: " + ", ".join(names)) if names else "Mapped"


def unit_frame(geo: dict, values: dict[str, float]) -> tuple[pd.DataFrame, int]:
    """Units to draw (geo coordinates LEFT JOINed to duid_mapping, 5 MW and over;
    unknown capacity stays) with their value, and the count of mapped units of
    5 MW and over that have no coordinates."""
    m = load_mapping().drop_duplicates("duid")
    coords = geo.get("units", {})
    df = pd.DataFrame([(d, c[0], c[1]) for d, c in coords.items()],
                      columns=["duid", "lon", "lat"])
    df = df.merge(m, on="duid", how="left")
    df = df[~(df["capacity_mw"] < MIN_UNIT_MW)]
    df["value"] = df["duid"].map(values).fillna(0.0)
    missing = m[(m["capacity_mw"] >= MIN_UNIT_MW) & ~m["duid"].isin(list(coords))]
    return df.reset_index(drop=True), int(len(missing))


def map_url(region: str, range_slug: str, measure: str, area: str | None = None,
            sort: str | None = None) -> str:
    u = f"/network/map?region={region}&range={range_slug}&measure={measure}"
    if sort:
        u += f"&sort={sort}"
    return u + (f"&area={quote(area, safe='')}" if area else "")


def _line_lonlat(features: list[dict]) -> tuple[list, list]:
    lon, lat = [], []
    for f in features:
        if f.get("kind") == "line":
            for part in f.get("parts") or []:
                lon += [c[0] for c in part] + [None]
                lat += [c[1] for c in part] + [None]
    return lon, lat


def _add_area_features(fig, features: list[dict], color: str, width: int, name: str,
                       hover: str, aid: str | None, legend: bool = False) -> None:
    lon, lat = _line_lonlat([f for f in features if f.get("kind") == "line"])
    cd = [f"a:{aid}"] * len(lon) if aid else None
    if lon:
        fig.add_trace(go.Scattermap(
            lon=lon, lat=lat, mode="lines+markers" if aid else "lines", name=name,
            line=dict(color=color, width=width),
            marker=dict(size=7, color=color) if aid else None,
            hoverinfo="text" if aid else "skip", text=hover if aid else None,
            customdata=cd, showlegend=legend, legendgroup=f"area-{aid or name}"))
    subs = [f for f in features if f.get("kind") == "substation" and f.get("point")]
    if subs:
        fig.add_trace(go.Scattermap(
            lon=[f["point"][0] for f in subs], lat=[f["point"][1] for f in subs],
            mode="markers", name=name, marker=dict(size=16, color=color),
            hoverinfo="text" if aid else "skip", text=hover if aid else None,
            customdata=[f"a:{aid}"] * len(subs) if aid else None,
            showlegend=not lon and legend, legendgroup=f"area-{aid or name}"))


def _area_meta(area_id: str) -> pd.Series | None:
    a = q("SELECT area_id, name, region FROM area WHERE area_id = ?", [area_id])
    return None if a.empty else a.iloc[0]


def _now_state() -> dict:
    """Binding areas and local price adjustments of the latest interval."""
    ts, b = binding_now()
    areas = b.dropna(subset=["area_id"]).groupby("area_id").size().to_dict()
    try:
        lp = q("""SELECT duid, local_price_adjustment AS adj FROM local_price5
                   WHERE settlementdate = (SELECT MAX(settlementdate) FROM local_price5)
                     AND local_price_adjustment IS NOT NULL""", db="constraints")
    except Exception:
        lp = pd.DataFrame(columns=["duid", "adj"])
    adj = {r["duid"]: float(r["adj"]) for _, r in lp.iterrows()}
    return {"ts": ts, "areas": areas, "adj": adj}


def build_map(region: str, range_slug: str, measure: str, area_id: str | None,
              sort: str = "energy") -> dict:
    """Everything the map page shows, as a dict (the figure plus the side list,
    selection and counts). Raises NoData for a missing geo file or period."""
    geo = load_geo()
    if geo is None:
        raise NoData("The map reference file is not available.")
    sort = normalise_sort(sort, "energy")
    now_mode = range_slug == "now"
    p = get_period(DEFAULT_RANGE if now_mode else range_slug)
    ar = area_region(region)
    sel_meta = _area_meta(area_id) if area_id else None
    sel = None
    fig = go.Figure()

    # -- area ranking for the period, or the binding set for Now
    now = _now_state() if now_mode else None
    if now_mode:
        regs = q("SELECT area_id, name, region FROM area")
        regs = regs[regs["area_id"].isin(list(now["areas"]))]
        if ar:
            regs = regs[regs["region"] == ar]
        df = regs.assign(n=regs["area_id"].map(now["areas"])).sort_values(
            ["n", "area_id"], ascending=[False, True]).reset_index(drop=True)
        df["hours"] = np.nan
    else:
        df = hotspot_areas(p, region, sort)
    names = dict(zip(df["area_id"], df["name"]))

    # -- 1. reference network (off until the reader turns it on)
    first = True
    for kv in ("220", "275", "330", "500"):
        polylines = geo.get("backbone", {}).get(kv) or []
        lon, lat = [], []
        for pl in polylines:
            lon += [c[0] for c in pl] + [None]
            lat += [c[1] for c in pl] + [None]
        if not lon:
            continue
        fig.add_trace(go.Scattermap(
            lon=lon, lat=lat, mode="lines", name=f"{kv} kV", visible="legendonly",
            line=dict(color=BACKBONE_COLOR, width=BACKBONE_WIDTH[kv]), hoverinfo="skip",
            legendgroup="backbone",
            legendgrouptitle=dict(text="Network, 220 kV and above") if first else None))
        first = False

    # -- 2. area lines
    if sel_meta is not None:
        aid = sel_meta["area_id"]
        feats = geo.get("area_lines", {}).get(aid, [])
        hours = None
        if not now_mode:
            hours = float(q("SELECT COALESCE(SUM(n_intervals), 0) AS n FROM area_daily "
                            "WHERE area_id = ? AND day BETWEEN ? AND ?",
                            [aid, p.start, p.end]).iloc[0]["n"]) / 12
        sel = {"area_id": aid, "name": sel_meta["name"], "hours": hours,
               "status": status_words(geo, aid),
               "binding_now": bool(now and aid in now["areas"])}
        hover = f"{sel_meta['name']}" + (f" · {hours:,.0f} h" if hours is not None else "")
        main = [f for f in feats if f.get("role") in ("protected", "interconnector")]
        ref = [f for f in feats if f.get("role") == "reference"]
        _add_area_features(fig, main, NOW_COLOR, 5, str(sel_meta["name"]), hover, aid)
        _add_area_features(fig, ref, REFERENCE_LINE_COLOR, 3, "Reference lines", "", None)
    elif now_mode:
        for aid in df["area_id"]:
            feats = _drawable(geo, aid)
            if feats:
                _add_area_features(fig, feats, NOW_COLOR, 5, str(names[aid]),
                                   f"{names[aid]} · binding now", aid)
    else:
        top = top_line_areas(df, geo)
        cols = ramp_colors(list(top["mwh"].fillna(0) if sort == "energy" else top["hours"]))
        for (_, r), col in zip(top.iterrows(), cols):
            hover = (f"{r['name']} · {r['hours']:,.0f} h · {fpct(r['pct_period'])}"
                     + ("" if _isnan(r["mwh"]) else f" · {fgwh(r['mwh'] / 1000)} GWh"))
            _add_area_features(fig, _drawable(geo, r["area_id"]), col, 5,
                               str(r["name"]), hover, r["area_id"])

    # -- 3. single-plant halos
    halo_areas = [(a, names[a]) for a in df["area_id"]
                  if geo.get("area_status", {}).get(a) == "single plant"][:MAP_TOP_HALOS]
    if sel_meta is not None:
        halo_areas = [(sel_meta["area_id"], sel_meta["name"])] \
            if geo.get("area_status", {}).get(sel_meta["area_id"]) == "single plant" else []
    hours_of = dict(zip(df["area_id"], df["hours"]))
    if halo_areas:
        ph = ",".join("?" * len(halo_areas))
        au = q(f"SELECT area_id, duid FROM area_units WHERE area_id IN ({ph})",
               [a for a, _ in halo_areas])
        hl_lon, hl_lat, hl_txt, hl_cd = [], [], [], []
        nm = dict(halo_areas)
        for _, r in au.iterrows():
            c = geo["units"].get(r["duid"])
            if c:
                h = hours_of.get(r["area_id"])
                hl_lon.append(c[0]); hl_lat.append(c[1]); hl_cd.append(f"a:{r['area_id']}")
                hl_txt.append(f"{nm[r['area_id']]} · "
                              + (f"{h:,.0f} h" if h is not None and not pd.isna(h)
                                 else "binding now"))
        if hl_lon:
            fig.add_trace(go.Scattermap(
                lon=hl_lon, lat=hl_lat, mode="markers", name="Single-plant limits",
                marker=dict(size=34, color=HALO_COLOR, opacity=0.25),
                hoverinfo="text", text=hl_txt, customdata=hl_cd))

    # -- 4. units
    if now_mode:
        values = {d: min(-a, LOCAL_PRICE_CAP) for d, a in now["adj"].items() if a < 0}
        cmax, ctitle = LOCAL_PRICE_CAP, "Local price adjustment, $/MWh below regional price"
    else:
        tot = unit_totals(p)
        if measure == "gwh":
            values = dict(zip(tot["duid"], tot["gwh_con"].fillna(0.0)))
            cmax = max(1, math.ceil(max(values.values(), default=0)))
            ctitle = "GWh curtailed while constrained"
        else:
            values = dict(zip(tot["duid"], tot["pct_dis"].fillna(0.0)))
            cmax, ctitle = 50, "% of intervals held back"
    udf, n_missing = unit_frame(geo, values)
    area_of = top_area_names(list(udf["duid"]), p) if len(udf) else {}
    capmax = max(float(udf["capacity_mw"].max()) if udf["capacity_mw"].notna().any() else 1.0, 1.0)
    cap = udf["capacity_mw"].fillna(MIN_UNIT_MW)
    udf["size"] = 6 + 16 * np.sqrt(cap / capmax)
    udf = udf.sort_values("value", kind="stable").reset_index(drop=True)
    sel_duids = set()
    if sel_meta is not None:
        sel_duids = set(q("SELECT duid FROM area_units WHERE area_id = ?",
                          [sel_meta["area_id"]])["duid"])
    texts = []
    for _, r in udf.iterrows():
        site = r["site"] if not _isnan(r["site"]) else r["duid"]
        if now_mode:
            adj = now["adj"].get(r["duid"])
            val = (f"Local price adjustment −${abs(adj):,.0f}/MWh" if adj is not None and adj < 0
                   else "No local price adjustment")
            who = "Most often held back by (90 days)"
        elif measure == "gwh":
            val, who = f"{fgwh(r['value'])} GWh curtailed while constrained", "Most often held back by"
        else:
            val, who = f"{fpct(r['value'])} of intervals held back", "Most often held back by"
        a = area_of.get(r["duid"])
        texts.append(f"<b>{esc(site)}</b><br>{esc(fuel_label(r['fuel']))} · "
                     f"{fnum(r['capacity_mw'])} MW<br>{val}"
                     + (f"<br>{who}: {esc(a[1])}" if a else ""))
    marker = dict(size=list(udf["size"]), color=list(udf["value"]), colorscale=UNIT_SCALE,
                  cmin=0, cmax=cmax,
                  colorbar=dict(title=dict(text=ctitle, side="right", font=dict(size=11)),
                                thickness=12, len=0.6, x=1.0, xanchor="right", y=0.5,
                                bgcolor="rgba(255,252,240,0.7)", tickfont=dict(size=10)))
    if sel_meta is not None:
        marker["opacity"] = [1.0 if d in sel_duids else 0.25 for d in udf["duid"]]
    else:
        marker["opacity"] = 0.9
    fig.add_trace(go.Scattermap(
        lon=list(udf["lon"]), lat=list(udf["lat"]), mode="markers", name="Units",
        marker=marker, hoverinfo="text", text=texts,
        customdata=[f"u:{d}" for d in udf["duid"]], showlegend=False))

    # -- view
    if sel is not None:
        pts = _feature_points(geo.get("area_lines", {}).get(sel["area_id"], []))
        pts += [(r["lon"], r["lat"]) for _, r in udf.iterrows() if r["duid"] in sel_duids]
        lon, lat, zoom = bbox_view(pts) if pts else MAP_BASE_VIEWS.get(region, MAP_BASE_VIEWS["NEM"])
    else:
        lon, lat, zoom = MAP_BASE_VIEWS[region]
    fig.update_layout(
        map=dict(style="carto-positron", center=dict(lon=lon, lat=lat), zoom=zoom),
        margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, -apple-system, sans-serif", size=12, color=INK),
        legend=dict(x=0.01, y=0.99, xanchor="left", yanchor="top",
                    bgcolor="rgba(255,252,240,0.85)", font=dict(size=11)))

    side = []
    for _, r in df.head(MAP_SIDE_LIST).iterrows():
        side.append({"area_id": r["area_id"], "name": r["name"],
                     "gwh": None if "mwh" not in df.columns or _isnan(r["mwh"]) else float(r["mwh"]) / 1000,
                     "hours": None if pd.isna(r["hours"]) else float(r["hours"]),
                     "tag": area_tag(geo, r["area_id"])})
    return {"fig": fig, "side": side, "selected": sel, "sort": sort,
            "rising": rising_areas(load_ranks()), "n_missing": n_missing,
            "center": (lon, lat), "zoom": zoom, "period": p, "geo": geo,
            "now_ts": now["ts"] if now else None, "range": range_slug}


def map_html(region: str, range_slug: str, measure: str, area_id: str | None,
             sort: str = "energy") -> str:
    try:
        m = build_map(region, range_slug, measure, area_id or None, sort)
    except NoData as e:
        return placeholder(str(e))
    except Exception:
        return placeholder("The map could not be built from the current data.")
    p, geo, sel = m["period"], m["geo"], m["selected"]
    now_mode = range_slug == "now"
    link_range = DEFAULT_RANGE if now_mode else p.slug
    div_id = f"netw-map-{int(datetime.now().timestamp() * 1000)}-{next(_plot_counter)}"
    cfg = json.dumps({"displaylogo": False, "responsive": True, "scrollZoom": True})
    script = (
        f'<script>(function(){{var id="{div_id}",f={m["fig"].to_json()};'
        'function go(u){if(window.htmx){htmx.ajax("GET",u,{target:"#tab-body",'
        'swap:"innerHTML"});history.pushState({},"",u);}else{window.location.href=u;}}'
        f'Plotly.newPlot(id,f.data,f.layout,{cfg}).then(function(gd){{'
        'gd.on("plotly_click",function(e){var pt=e.points&&e.points[0];'
        'if(!pt||pt.customdata==null)return;var c=String(pt.customdata);'
        'var k=c.slice(0,2),v=encodeURIComponent(c.slice(2));'
        f'if(k==="u:")go("/network/unit?duid="+v+"&range={link_range}");'
        f'else if(k==="a:")go("/network/area?id="+v+"&range={link_range}");}});}});}})();</script>')
    if now_mode:
        sub = (f'<div class="sub">Latest dispatch interval, ending {fdt(m["now_ts"])} NEM '
               f'time. Lines are the network constraints binding now; dots are coloured by '
               f'each unit’s local price adjustment.</div>')
    else:
        sub = _period_sub(p, f"{_region_text(region)} map")
    sel_html = ""
    if sel:
        hrs = ("binding now" if sel["binding_now"] else "not binding now") if now_mode \
            else f'{fhours(sel["hours"])} hours in the period'
        sel_html = (f'<div class="selbar"><b>{esc(sel["name"])}</b> · {hrs} · '
                    f'{esc(sel["status"])} · '
                    + link(map_url(region, range_slug, measure, None, m["sort"]), "Clear selection") + ' · '
                    + link(area_url(sel["area_id"], link_range), "Area page") + '</div>')
    items = []
    for r in m["side"]:
        if r["hours"] is None:
            h = "binding now"
        else:
            h = (f'{fgwh(r["gwh"])} GWh · ' if r["gwh"] is not None else "") + f'{fhours(r["hours"])} h'
        rise = (" " + RISING_TAG) if r["area_id"] in m["rising"] else ""
        eq = link(area_url(r["area_id"], link_range, "equation"), "equation")
        items.append(
            f'<li>{link(map_url(region, range_slug, measure, r["area_id"], m["sort"]), r["name"])}'
            f'{rise}<span class="side-meta"><span class="tag">{esc(r["tag"])}</span> {h}'
            f' · {eq}</span></li>')
    heading = ("constraints binding now" if now_mode else
               "energy held back" if m["sort"] == "energy" else "binding hours")
    side = (f'<div class="side"><h3>Top areas by {heading}</h3>'
            f'<ul>{"".join(items) or "<li>None in this selection.</li>"}</ul></div>')
    note = (f'<div class="note">{esc(MAP_SENTENCE)} {fnum(m["n_missing"])} units of 5 MW '
            f'and over are not drawn for lack of coordinates. {esc(geo.get("attribution", ""))}'
            '</div>')
    return wrap(sub + sel_html
                + f'<div class="mapwrap"><div><div id="{div_id}" class="mapbox"></div>'
                  f'{script}</div>{side}</div>' + note + note_html())
