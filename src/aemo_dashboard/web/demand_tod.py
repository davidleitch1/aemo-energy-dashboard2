"""Demand by time of day, this period vs the same period a year earlier, with
storage load (battery charging, pumping) and battery discharge as separate
components, to see whether storage load or other demand moved the price.

Pure functions (no web framework); standalone server: demand_tod_app.py.

Components, mean MW per half-hour slot over the window (sum of regions):
- op:   demand30.demand = AEMO OPERATIONAL_DEMAND. Excludes battery charging
        and pump load; net of rooftop PV.
- roof: rooftop30 power, five parent regions only. op + roof = total demand.
- chg:  battery charging, -min(scada, 0) over Battery Storage DUIDs.
- pump: pumped-hydro pump loads (fuel NULL in duid_mapping), scada >= 0.
- dis:  battery discharge, max(scada, 0) over Battery Storage DUIDs.
- price: regional RRP mean; NEM = demand-weighted by operational demand.

Slots are labelled by interval end (00:00 plotted at 24:00), NEM time.
Prior window: shifted back 52 weeks (same weekday mix); YTD uses the same
calendar span a year earlier.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DB_PATH = "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb"
NEM_TZ = timezone(timedelta(hours=10))

REGIONS = [("NEM", "NEM"), ("NSW1", "NSW"), ("QLD1", "QLD"),
           ("VIC1", "VIC"), ("SA1", "SA"), ("TAS1", "TAS")]
RANGES = [("7d", "7D"), ("30d", "30D"), ("90d", "90D"),
          ("ytd", "YTD"), ("1y", "1Y")]
# Rules the HTML above relies on, every selector scoped to the .dtod wrapper so
# the module can be embedded in the main dashboard shell without clashes.
CSS = """
.dtod { padding: 8px 24px 24px; }
.dtod .sub, .dtod .note { color:#878580; font-size:12px; margin:6px 0; }
.dtod .note { line-height:1.45; }
.dtod table.summ { border-collapse:collapse; font-size:13px; margin:10px 0;
                   font-variant-numeric: tabular-nums; }
.dtod table.summ th { color:#878580; font-weight:500; font-size:11px;
                      text-align:right; padding:4px 12px;
                      border-bottom:1px solid #e6e4d9; }
.dtod table.summ td { text-align:right; padding:4px 12px;
                      border-bottom:1px solid #e6e4d9; }
.dtod table.summ td:first-child { text-align:left; }
.dtod table.summ .u { color:#878580; font-size:11px; }
.dtod .placeholder { padding:40px; text-align:center; color:#878580; }
@media (max-width:700px) { .dtod table.summ { font-size:11px; }
  .dtod table.summ th, .dtod table.summ td { padding:3px 5px; } }
"""

MAINLAND = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
PUMP_DUIDS = ("PUMP1", "PUMP2", "SNOWYP", "SHPUMP", "KIDSPHL1", "KIDSPHL2")

MIDDAY = (10.0, 15.0)    # slot-end hours, (lo, hi]
EVENING = (17.0, 21.0)

# Flexoki Light
PAPER, INK, MUTED, BORDER = "#FFFCF0", "#100F0F", "#878580", "#E6E4D9"
BASE_COLORS = {"op": "#205EA6", "total": "#BC5215"}   # blue, orange
PRICE_COLOR = "#100F0F"
COMP = {   # key: (label, colour)
    "op":   ("Operational demand", "#205EA6"),
    "roof": ("Rooftop PV", "#AD8301"),
    "chg":  ("Battery charging", "#24837B"),
    "pump": ("Pump load", "#66800B"),
    "dis":  ("Battery discharge (−)", "#5E409D"),
}


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        return conn.execute(sql, params or []).df()
    finally:
        conn.close()


def windows(range_slug: str, now: pd.Timestamp | None = None):
    """((s, e), (prior_s, prior_e), label) in naive NEM time."""
    now = now if now is not None else pd.Timestamp(
        datetime.now(NEM_TZ).replace(tzinfo=None))
    if range_slug == "ytd":
        s = pd.Timestamp(year=now.year, month=1, day=1)
        yr = pd.DateOffset(years=1)
        return (s, now), (s - yr, now - yr), "year to date"
    days = {"7d": (7, "last 7 days"), "30d": (30, "last 30 days"),
            "90d": (90, "last 90 days"), "1y": (365, "last 12 months")}
    n, label = days.get(range_slug, days["30d"])
    s = now - timedelta(days=n)
    wk52 = timedelta(weeks=52)
    return (s, now), (s - wk52, now - wk52), label


def _in(region: str) -> str:
    if region == "NEM":
        return str(MAINLAND)
    if region not in MAINLAND:
        raise ValueError(f"unknown region {region!r}")
    return f"('{region}')"


def load_components(region: str, s_ts, e_ts) -> pd.DataFrame:
    """48 rows indexed by slot-end hour x (0.5..24): op, roof, chg, pump, dis,
    price (mean MW / $ per slot over the window)."""
    rin = _in(region)
    p = [s_ts, e_ts]
    per_slot = "strftime(ts, '%H:%M')"
    parts = {
        "op": f"""SELECT ts, SUM(demand) v FROM (SELECT settlementdate ts,
                  demand FROM demand30 WHERE regionid IN {rin}
                  AND settlementdate >= ? AND settlementdate < ?) GROUP BY ts""",
        "roof": f"""SELECT settlementdate ts, SUM(GREATEST(power, 0)) v
                  FROM rooftop30 WHERE regionid IN {rin}
                  AND settlementdate >= ? AND settlementdate < ?
                  GROUP BY ts""",
        "batt": f"""SELECT s.settlementdate ts,
                  SUM(GREATEST(-s.scadavalue, 0)) chg,
                  SUM(GREATEST(s.scadavalue, 0)) dis
                  FROM scada30 s JOIN duid_mapping d ON s.duid = d.duid
                  WHERE d.fuel = 'Battery Storage' AND d.region IN {rin}
                  AND s.settlementdate >= ? AND s.settlementdate < ?
                  GROUP BY ts""",
        "pump": f"""SELECT s.settlementdate ts, SUM(GREATEST(s.scadavalue, 0)) v
                  FROM scada30 s JOIN duid_mapping d ON s.duid = d.duid
                  WHERE s.duid IN {PUMP_DUIDS} AND d.region IN {rin}
                  AND s.settlementdate >= ? AND s.settlementdate < ?
                  GROUP BY ts""",
        "price": f"""SELECT p.settlementdate ts,
                  SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) v
                  FROM prices30 p JOIN demand30 d
                    ON p.settlementdate = d.settlementdate
                   AND p.regionid = d.regionid
                  WHERE p.regionid IN {rin}
                  AND p.settlementdate >= ? AND p.settlementdate < ?
                  GROUP BY ts""",
    }
    slots = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]
    out = pd.DataFrame(index=pd.Index(slots, name="slot"))
    for key, sub in parts.items():
        if key == "batt":
            df = q(f"SELECT {per_slot} slot, AVG(chg) chg, AVG(dis) dis "
                   f"FROM ({sub}) GROUP BY slot", p).set_index("slot")
            out["chg"], out["dis"] = df["chg"], df["dis"]
        else:
            df = q(f"SELECT {per_slot} slot, AVG(v) v FROM ({sub}) "
                   f"WHERE v IS NOT NULL GROUP BY slot", p).set_index("slot")
            out[key] = df["v"]
    # Storage components are genuinely zero where no unit ran (e.g. TAS).
    out[["chg", "dis", "pump"]] = out[["chg", "dis", "pump"]].fillna(0.0)
    hm = out.index.str.split(":").map(lambda t: int(t[0]) + int(t[1]) / 60)
    out["x"] = [24.0 if v == 0 else v for v in hm]
    return out.reset_index().sort_values("x").set_index("x")


def demand_line(c: pd.DataFrame, base: str, chg: bool, pump: bool,
                dis: bool) -> pd.Series:
    s = c["op"] + (c["roof"] if base == "total" else 0)
    if chg:
        s = s + c["chg"]
    if pump:
        s = s + c["pump"]
    if dis:
        s = s - c["dis"]
    return s


def line_label(base: str, chg: bool, pump: bool, dis: bool) -> str:
    lbl = "Total demand" if base == "total" else "Operational demand"
    adds = (["+ charging"] if chg else []) + (["+ pumping"] if pump else []) \
        + (["− discharge"] if dis else [])
    return lbl + (" " + " ".join(adds) if adds else "")


def _components(bases: list[str], chg: bool, pump: bool, dis: bool):
    keys = ["op"] + (["roof"] if "total" in bases else [])
    keys += (["chg"] if chg else []) + (["pump"] if pump else []) \
        + (["dis"] if dis else [])
    return keys


def _hhmm(x: float) -> str:
    return f"{int(x):02d}:{int(round((x % 1) * 60)):02d}"


def build_figure(cur, pri, bases, chg, pump, dis, cur_lbl, pri_lbl):
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        row_heights=[0.44, 0.24, 0.32], vertical_spacing=0.06)
    span = [f"{_hhmm(x - 0.5)}–{_hhmm(x)}" for x in cur.index]

    for base in bases:
        lbl = line_label(base, chg, pump, dis)   # full label, hover only
        short = "Total" if base == "total" else "Operational"
        col = BASE_COLORS[base]
        for df, period, dash, w in [(cur, cur_lbl, "solid", 2.4),
                                    (pri, pri_lbl, "dash", 1.8)]:
            fig.add_trace(go.Scatter(
                x=df.index, y=demand_line(df, base, chg, pump, dis),
                name=f"{short} · {period}", mode="lines",
                line=dict(color=col, width=w, dash=dash),
                legendgroup="demand", customdata=span,
                hovertemplate=f"{period}: %{{y:,.0f}} MW<extra>{lbl}</extra>",
            ), row=1, col=1)

    for df, period, dash, w in [(cur, cur_lbl, "solid", 2.2),
                                (pri, pri_lbl, "dash", 1.6)]:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["price"], name=f"Spot price · {period}",
            mode="lines", line=dict(color=PRICE_COLOR, width=w, dash=dash),
            legendgroup="price",
            hovertemplate=f"{period}: $%{{y:,.0f}}/MWh<extra>Spot</extra>",
        ), row=2, col=1)

    keys = _components(bases, chg, pump, dis)
    net = pd.Series(0.0, index=cur.index)
    for k in keys:
        sign = -1 if k == "dis" else 1
        d = sign * (cur[k] - pri[k])
        net = net + d
        name, col = COMP[k]
        fig.add_trace(go.Bar(
            x=cur.index, y=d, name=f"Δ {name}", width=0.42,
            marker=dict(color=col, opacity=0.8), legendgroup="delta",
            hovertemplate=f"Δ {name}: %{{y:+,.0f}} MW<extra></extra>",
        ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=cur.index, y=net, name="Δ net", mode="lines",
        line=dict(color=INK, width=1.6), legendgroup="delta",
        hovertemplate="Δ net: %{y:+,.0f} MW<extra></extra>",
    ), row=3, col=1)

    fig.update_layout(
        barmode="relative", bargap=0, paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        font=dict(family="Inter, -apple-system, sans-serif", size=12,
                  color=INK),
        height=860, margin=dict(l=70, r=24, t=10, b=40),
        hovermode="x unified",
        legend=dict(orientation="v", x=1.01, y=1, font=dict(size=11),
                    bgcolor="rgba(0,0,0,0)", tracegroupgap=24),
    )
    fig.update_xaxes(range=[0, 24.25], tickvals=list(range(0, 25, 3)),
                     ticktext=[f"{h:02d}:00" for h in range(0, 25, 3)],
                     gridcolor=BORDER, gridwidth=0.5, showline=False)
    fig.update_xaxes(title=dict(text="Time of day (NEM time, half-hour "
                                "ending)", font=dict(size=11, color=MUTED)),
                     row=3, col=1)
    # Storage options in the axis title keep the legend names short.
    adds = " ".join((["+ charging"] if chg else []) + (["+ pumping"] if pump
                    else []) + (["− discharge"] if dis else []))
    demand_title = "Demand, average MW" + (f"<br>{adds}" if adds else "")
    for row, title, fmt in [(1, demand_title, ",.0f"),
                            (2, "Spot $/MWh", "$,.0f"),
                            (3, "Change vs last year, MW", "+,.0f")]:
        fig.update_yaxes(title=dict(text=title, font=dict(size=11,
                                                          color=MUTED)),
                         gridcolor=BORDER, gridwidth=0.5, zeroline=True,
                         zerolinecolor=MUTED if row == 3 else BORDER,
                         tickformat=fmt.replace("$", ""),
                         tickprefix="$" if "$" in fmt else "",
                         row=row, col=1)
    return fig


def _band_mean(s: pd.Series, band) -> float:
    lo, hi = band
    return s[(s.index > lo) & (s.index <= hi)].mean()


def summary_table(cur, pri) -> str:
    rows = [(k, COMP[k][0].replace(" (−)", "")) for k in COMP] + \
        [("price", "Spot price")]
    head = ("<tr><th></th><th>This period</th><th>Year earlier</th>"
            "<th>Δ all day</th><th>Δ 10:00–15:00</th>"
            "<th>Δ 17:00–21:00</th></tr>")
    body = []
    for k, label in rows:
        is_px = k == "price"
        f = (lambda v: "–" if pd.isna(v) else
             (("−$" if v < 0 else "$") + f"{abs(v):,.0f}")) if is_px else \
            (lambda v: "–" if pd.isna(v) else f"{v:,.0f}")
        fd = (lambda v: "–" if pd.isna(v) else
              (("−$" if v < 0 else "+$") + f"{abs(v):,.0f}")) if is_px else \
            (lambda v: "–" if pd.isna(v) else f"{v:+,.0f}")
        d = cur[k] - pri[k]
        unit = "$/MWh" if is_px else "MW"
        body.append(
            f"<tr><td>{label} <span class='u'>{unit}</span></td>"
            f"<td>{f(cur[k].mean())}</td><td>{f(pri[k].mean())}</td>"
            f"<td>{fd(d.mean())}</td><td>{fd(_band_mean(d, MIDDAY))}</td>"
            f"<td>{fd(_band_mean(d, EVENING))}</td></tr>")
    return f'<table class="summ">{head}{"".join(body)}</table>'


def _content_body(region: str, range_slug: str, bases: list[str],
                 chg: bool, pump: bool, dis: bool) -> str:
    (s, e), (ps, pe), range_label = windows(range_slug)
    cur = load_components(region, s, e)
    pri = load_components(region, ps, pe)
    region_label = dict(REGIONS).get(region, region)
    if cur["op"].isna().all():
        return f'<div class="placeholder">No demand data for {region_label}.</div>'
    fmt = "%-d %b %Y"
    # Short legend names; the subtitle carries both date ranges.
    cur_lbl, pri_lbl = "this period", "year earlier"
    div_id = f"demand-tod-plot-{int(datetime.now().timestamp() * 1000)}"
    fig = build_figure(cur, pri, bases, chg, pump, dis, cur_lbl, pri_lbl)
    cfg = json.dumps({"displaylogo": False, "responsive": True})
    gap = ""
    if pri["op"].isna().any() or pri["price"].isna().any():
        gap = " The year-earlier window has missing slots."
    return (
        f'<div class="sub">{region_label}, {range_label} '
        f'({s.strftime(fmt)} → {e.strftime(fmt)}) against '
        f'{ps.strftime(fmt)} → {pe.strftime(fmt)}. Solid = this period, '
        f'dashed = a year earlier.{gap}</div>'
        f'{summary_table(cur, pri)}'
        f'<div id="{div_id}" style="height:860px"></div>'
        f'<script>(function(){{var f={fig.to_json()};'
        f'Plotly.newPlot("{div_id}",f.data,f.layout,{cfg});}})();</script>'
        f'<div class="note">Operational demand is AEMO OPERATIONAL_DEMAND: '
        f'grid-supplied, net of rooftop PV, excluding battery charging and '
        f'pump load. Total demand adds rooftop PV. Battery charging and '
        f'discharge are from 30-minute SCADA; pump load is the pumped-hydro '
        f'pump units (Tumut 3, Shoalhaven, Wivenhoe, Kidston). The bottom '
        f'panel stacks the year-on-year change in each component included '
        f'by the checkboxes; discharge enters with a minus sign because it '
        f'reduces the load other generators meet. Spot price is '
        f'{"demand-weighted across regions" if region == "NEM" else "the regional price"}'
        f'. The year-earlier window is 52 weeks back (YTD: the same calendar '
        f'span).</div>'
    )


def content_html(region: str, range_slug: str, bases: list[str], chg: bool,
                 pump: bool, dis: bool) -> str:
    """Scoped wrapper: the CSS above only applies inside .dtod."""
    return (f'<div class="dtod">'
            f'{_content_body(region, range_slug, bases, chg, pump, dis)}</div>')
