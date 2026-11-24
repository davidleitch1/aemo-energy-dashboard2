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
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go

DATA_DIR = "/Users/davidleitch/aemo_production/data"
SUMMARY_DB = f"{DATA_DIR}/network_summary.duckdb"
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
PASA_TRANSMISSION_URL = "/pasa/transmission"

# Flexoki Light
INK, MUTED, BORDER = "#403E3C", "#878580", "#E6E4D9"
CAUSES = ["System normal", "Outage", "Unit-specific limit"]
CAUSE_COLORS = {"System normal": "#205EA6", "Outage": "#BC5215",
                "Unit-specific limit": "#5E409D"}
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
.netw .cbar { display:flex; height:8px; width:90px; border-radius:2px;
              overflow:hidden; margin-top:4px; }
.netw .cbar span { display:block; height:100%; }
.netw .legend { display:flex; flex-wrap:wrap; gap:14px; font-size:12px;
                color:#403E3C; margin:6px 0 0; }
.netw .legend i { display:inline-block; width:10px; height:10px;
                  border-radius:2px; margin-right:5px; }
.netw .row3 { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; }
.netw .row3 h3 { margin-top:10px; }
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


def area_url(area_id: str, rng: str) -> str:
    return f"/network/area?id={quote(str(area_id), safe='')}&range={rng}"


def unit_url(duid: str, rng: str) -> str:
    return f"/network/unit?duid={quote(str(duid), safe='')}&range={rng}"


def tiles_html(tiles: list[tuple[str, str]]) -> str:
    inner = "".join(f'<div class="tile"><div class="k">{esc(k)}</div>'
                    f'<div class="v">{v}</div></div>' for k, v in tiles)
    return f'<div class="strip">{inner}</div>'


def table_html(headers: list[tuple[str, bool]], rows: list[list[str]],
               empty: str = "No rows in this period.") -> str:
    """headers: (label, right_aligned). Cells are pre-rendered HTML."""
    if not rows:
        return f'<div class="sub">{esc(empty)}</div>'
    head = "".join(f'<th class="{"r" if r else ""}">{esc(h)}</th>' for h, r in headers)
    body = "".join(
        "<tr>" + "".join(
            f'<td class="{"r" if headers[i][1] else ""}">{c}</td>'
            for i, c in enumerate(row)) + "</tr>" for row in rows)
    return (f'<div class="tbl-wrap"><table><thead><tr>{head}</tr></thead>'
            f'<tbody>{body}</tbody></table></div>')


def cause_bar(sn: float, ou: float, us: float) -> str:
    shares = cause_shares(sn, ou, us)
    if not sum(shares):
        return "–"
    title = " · ".join(f"{c} {s:.0f}%" for c, s in zip(CAUSES, shares))
    segs = "".join(f'<span style="width:{s:.2f}%;background:{CAUSE_COLORS[c]}"></span>'
                   for c, s in zip(CAUSES, shares))
    return f'<div class="cbar" title="{esc(title)}">{segs}</div>'


def cause_legend() -> str:
    return '<div class="legend">' + "".join(
        f'<span><i style="background:{CAUSE_COLORS[c]}"></i>{c}</span>'
        for c in CAUSES) + "</div>"


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
    fig = go.Figure()
    for name, s in series.items():
        s = s.reindex(months).fillna(0.0)
        fig.add_trace(go.Bar(
            x=months, y=s.values, name=name, marker_color=colors[name],
            hovertemplate=f"{esc(name)}<br>%{{x|%b %Y}}: %{{y:,.1f}}<extra></extra>"))
    _layout(fig, height, barmode="stack", bargap=0.2)
    fig.update_yaxes(title=dict(text=ytitle, font=dict(size=11, color=MUTED)),
                     tickformat=",.0f")
    fig.update_xaxes(tickformat="%b %Y")
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

def hotspot_areas(p: Period, region: str) -> pd.DataFrame:
    """One row per area that bound in the period, most binding hours first."""
    cols = _columns("area")
    mw = "a.units_mw" if "units_mw" in cols else "a.weighted_mw"
    reg_sql, params = "", [p.start, p.end]
    ar = area_region(region)
    if ar:
        reg_sql, params = "WHERE a.region = ?", params + [ar]
    df = q(f"""
        WITH ad AS (
            SELECT area_id, SUM(n_intervals) AS n, SUM(n_sysnormal) AS sn,
                   SUM(n_outage) AS ou, SUM(n_unit_specific) AS us,
                   SUM(constrained_mwh) AS mwh, SUM(constrained_value) AS val
              FROM area_daily WHERE day BETWEEN ? AND ?
             GROUP BY area_id HAVING SUM(n_intervals) > 0)
        SELECT ad.*, COALESCE(a.name, ad.area_id) AS name, a.region, a.limittype,
               a.n_units, {mw} AS units_mw
          FROM ad JOIN area a USING (area_id) {reg_sql}
         ORDER BY ad.n DESC, ad.area_id""", params)
    df["hours"] = df["n"] / 12
    df["pct_period"] = df["n"] / p.n_intervals * 100
    return df


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


def hotspots_html(region: str, range_slug: str) -> str:
    try:
        p = get_period(range_slug)
        df = hotspot_areas(p, region)
        if df.empty:
            return placeholder(f"No constraint bound in {_region_text(region)} "
                               f"for the {p.label}.")
        sn, ou, us = _sum_cause(df)
        sh = cause_shares(sn, ou, us)
        tiles = tiles_html([
            ("Hours in period", fnum(p.hours)),
            ("Areas that bound", fnum(len(df))),
            ("System normal share", f"{sh[0]:.0f}%"),
            ("Outage share", f"{sh[1]:.0f}%"),
            ("Unit-specific share", f"{sh[2]:.0f}%"),
            ("Curtailed while constrained", f"{df['mwh'].sum() / 1000:,.1f} GWh"),
            ("Value of that curtailment", fmoney_m(df["val"].sum() / 1e6)),
        ])
        rows = []
        for _, r in df.head(25).iterrows():
            rows.append([
                link(area_url(r["area_id"], p.slug), r["name"]),
                esc(r["region"] or "–"),
                fnum(r["hours"], 1), f"{r['pct_period']:.1f}%",
                cause_bar(r["sn"], r["ou"], r["us"]),
                fnum(r["n_units"]), fnum(r["units_mw"]),
                esc(r["limittype"] or "–"),
                fnum(r["mwh"] / 1000, 1), fnum(r["val"] / 1e6, 1)])
        table = table_html(
            [("Area", False), ("Region", False), ("Hours", True), ("% of period", True),
             ("Cause split", False), ("Units", True), ("MW behind", True),
             ("Limit type", False), ("GWh curtailed", True), ("$m", True)], rows)
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
            + tiles
            + '<h3>Top 25 congestion areas by binding hours</h3>' + cause_legend()
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
                esc(r["reason"] or "–"), fnum(r["n"] / 12, 1)]
        rows.append(row)
    return rows


def area_html(area_id: str, range_slug: str) -> str:
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
            ("Binding hours", fnum(n / 12, 1)),
            ("% of period", f"{n / p.n_intervals * 100:.1f}%"),
            ("System normal share", f"{sh[0]:.0f}%"),
            ("Outage share", f"{sh[1]:.0f}%"),
            ("Unit-specific share", f"{sh[2]:.0f}%"),
            ("Units behind it", fnum(a["n_units"])),
            ("MW behind it", fnum(a["units_mw"])),
            ("Curtailed while constrained",
             "–" if _isnan(ad["mwh"]) else f"{ad['mwh'] / 1000:,.1f} GWh"),
            ("Value of that curtailment",
             fmoney_m(None if _isnan(ad["val"]) else ad["val"] / 1e6)),
        ])
        m = _area_monthly([area_id], p)
        series = {c: (m.set_index("month")[k] / 12) for c, k in
                  zip(CAUSES, ("sn", "ou", "us"))}
        chart = stacked_monthly(p, series, CAUSE_COLORS, "constraint-hours per month", 320)
        crows = [[esc(r["constraintid"]), fnum(r["n"] / 12, 1), esc(r["cause"] or "–"),
                  esc(r["limittype"] or "–"), describe(r["description"], r["definition_rule"])]
                 for _, r in cons.iterrows()]
        ctable = table_html([("Constraint", False), ("Hours", True), ("Cause", False),
                             ("Limit type", False), ("AEMO description", False)], crows,
                            "No constraint of this area bound in the period.")
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
                f' · {esc(area_id)}</div>'
                + (f'<div class="sub">AEMO description: {esc(desc)}</div>' if desc else
                   '<div class="sub">AEMO description: pending</div>')
                + _period_sub(p))
        return wrap(head + tiles
                    + '<h3>Constraint-hours by cause, per month</h3>' + cause_legend() + chart
                    + '<h3>Constraints that bound in the period</h3>' + ctable
                    + '<h3>Units behind it</h3>' + utable
                    + '<h3>Associated outages</h3>' + otable + note_html())
    except NoData as e:
        return placeholder(str(e))


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
        c2 = hbar(CAUSES, [t["by_cause"][c] / 12 for c in CAUSES],
                  [CAUSE_COLORS[c] for c in CAUSES], 300)
        rc = t["by_region_cause"]
        regs = [r for r in ("NSW", "QLD", "VIC", "SA", "TAS", "Other")
                if r in rc.index.get_level_values(0)]
        fig = go.Figure()
        for c in CAUSES:
            fig.add_trace(go.Bar(
                y=regs, x=[rc.get((r, c), 0) / 12 for r in regs], name=c,
                orientation="h", marker_color=CAUSE_COLORS[c],
                hovertemplate=f"{c}<br>%{{y}}: %{{x:,.0f}} h<extra></extra>"))
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
            + cause_legend()
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
                   f"{r['pct_dis']:.1f}%"]
            if battery:
                row.append(f"{r['pct_chg']:.1f}%")
            row += [fnum(r["gwh_con"], 1), fnum(r["gwh_other"], 1), fnum(r["value_m"], 1),
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
                  f"{dis / p.n_intervals * 100:.1f}%")]
        if is_batt:
            tiles.append(("Intervals charge-limited", f"{chg / p.n_intervals * 100:.1f}%"))
        tiles += [("Curtailed while constrained", f"{ud['mwh'].sum() / 1000:,.1f} GWh"),
                  ("Other curtailment", f"{ud['other'].sum() / 1000:,.1f} GWh"),
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
                     GROUP BY ALL ORDER BY n DESC, d.constraintid LIMIT 30""",
                 [duid, p.start, p.end])
        crows = [[esc(r["constraintid"]),
                  link(area_url(r["area_id"], p.slug), r["area_name"])
                  if not _isnan(r["area_id"]) else "–",
                  fnum(r["n"] / 12, 1), esc(r["direction"]),
                  describe(r["description"], r["definition_rule"])]
                 for _, r in cons.iterrows()]
        ctable = table_html([("Constraint", False), ("Area", False), ("Hours", True),
                             ("Direction", False), ("AEMO description", False)], crows,
                            "No network constraint included this unit in the period.")
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


def outages_in_progress(now: pd.Timestamp, limit: int = 40) -> pd.DataFrame:
    ph = ",".join("?" * len(OUTAGE_EXCLUDED))
    return q(f"""
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
         GROUP BY outageid ORDER BY starttime DESC, outageid DESC LIMIT {int(limit)}""",
             [now, now, *OUTAGE_EXCLUDED, now], db="constraints")


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
                esc(r["cause"] if not _isnan(r["cause"]) else "–"),
                esc(r["limittype"] if not _isnan(r["limittype"]) else "–"),
                describe(r["description"], r["definition_rule"]),
                fsigned(r["marginalvalue"], 1)])
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
        orows = [[esc(r["substation"] or "–"), esc(r["equipment"]), fdt(r["starttime"]),
                  fdt(r["endtime"]), esc(r["status"]), esc(r["reason"] or "–")]
                 for _, r in out.iterrows()]
        otable = table_html([("Substation", False), ("Equipment", False), ("Start", False),
                             ("End", False), ("Status", False), ("Reason", False)], orows,
                            "No transmission outage is in progress.")
        lp_note = (f" (local price adjustments: interval ending {fdt(lp_ts)})"
                   if lp_ts is not None and lp_ts != ts else "")
        return wrap(
            f'<div class="sub">Latest dispatch interval: ending {fdt(ts)} NEM time, '
            f'age {age} min{lp_note}.</div>'
            + tiles_html([("Binding network constraints", fnum(len(b))),
                          ("Interval age", f"{age} min"),
                          ("Units with an adjustment", fnum(len(lp))),
                          ("Outages in progress", fnum(len(out)))])
            + '<h3>Binding network constraints</h3>' + btable
            + '<h3>Units with the largest local price adjustment</h3>' + utable
            + '<h3>Transmission outages in progress</h3>' + otable
            + '<div class="sub">Outages counted: start time before now, end time after now, '
              'status not WDRAWN, WD REQ, COMPLETE, RESUBMIT or UTP, and no actual end '
              'time before now. Start and end are as submitted. See also '
              f'<a href="{PASA_TRANSMISSION_URL}" hx-get="{PASA_TRANSMISSION_URL}" '
              'hx-target="#tab-body" hx-push-url="true">PASA transmission outages</a>.</div>'
            + note_html())
    except NoData as e:
        return placeholder(str(e))
    except Exception:
        return placeholder("Live constraint data is not available.")
