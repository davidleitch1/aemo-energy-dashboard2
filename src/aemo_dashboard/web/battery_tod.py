"""Battery time-of-day profile: average charge / discharge MW by half-hour
slot, with volume-weighted charge and discharge prices on a second axis.

Pure functions (no web framework) so the main dashboard can import this module
the way it imports pasa_data. Standalone test server: battery_tod_app.py.

Conventions
- Source: scada30 (interval-end labels) x duid_mapping fuel = 'Battery Storage'.
  Battery BDUs are signed: discharge > 0, charge < 0.
- A slot is the half hour ENDING at the label; the 00:00 label is plotted at
  24:00 (it covers 23:30-24:00). All times NEM time (UTC+10).
- Average MW = mean over the window's days of the fleet's summed MW in that slot.
- Charge / discharge price = sum(MW x own-region RRP) / sum(MW): what the fleet
  paid / earned per MWh, not a plain mean of prices.
- Spot reference: region RRP mean; NEM = demand-weighted across regions.
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
           ("VIC1", "VIC"), ("SA1", "SA")]   # TAS: no registered batteries
RANGES = [("7d", "7D"), ("30d", "30D"), ("90d", "90D"),
          ("ytd", "YTD"), ("1y", "1Y")]
MAINLAND = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
MIN_VOLUME_SHARE = 0.10   # hide a slot's price below 10% of that side's peak MW

# Flexoki Light
PAPER, INK, MUTED, BORDER = "#FFFCF0", "#100F0F", "#878580", "#E6E4D9"
DISCHARGE_COLOR = "#5E409D"   # purple 600 (dashboard battery colour)
CHARGE_COLOR = "#24837B"      # cyan 600
DISCHARGE_PRICE_COLOR = "#A02F6F"  # magenta 600
CHARGE_PRICE_COLOR = "#205EA6"     # blue 600
SPOT_COLOR = "#6F6E69"             # base 600


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        return conn.execute(sql, params or []).df()
    finally:
        conn.close()


def window(range_slug: str, now: pd.Timestamp | None = None
           ) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    """(start, end, label) in naive NEM time."""
    now = now if now is not None else pd.Timestamp(
        datetime.now(NEM_TZ).replace(tzinfo=None))
    days = {"7d": (7, "last 7 days"), "30d": (30, "last 30 days"),
            "90d": (90, "last 90 days"), "1y": (365, "last 12 months")}
    if range_slug == "ytd":
        return pd.Timestamp(year=now.year, month=1, day=1), now, "year to date"
    n, label = days.get(range_slug, days["30d"])
    return now - timedelta(days=n), now, label


def _region_clause(region: str, col: str) -> str:
    if region == "NEM":
        return f"{col} IN {MAINLAND}"
    if region not in MAINLAND:
        raise ValueError(f"unknown region {region!r}")
    return f"{col} = '{region}'"


def load_profile(region: str, s_ts: pd.Timestamp, e_ts: pd.Timestamp
                 ) -> pd.DataFrame:
    """48 rows: x (slot end, hours 0.5..24), slot, dis_mw, chg_mw, dis_px,
    chg_px, spot, dis_mwh, chg_mwh, dis_val, chg_val, n_days."""
    batt = q(f"""
        WITH g AS (
            SELECT s.settlementdate AS ts, d.region, s.scadavalue AS mw
              FROM scada30 s JOIN duid_mapping d ON s.duid = d.duid
             WHERE d.fuel = 'Battery Storage'
               AND {_region_clause(region, 'd.region')}
               AND s.settlementdate >= ? AND s.settlementdate < ?
        ),
        per AS (
            SELECT g.ts,
                   SUM(GREATEST(mw, 0))          AS dis,
                   SUM(GREATEST(-mw, 0))         AS chg,
                   SUM(GREATEST(mw, 0)  * p.rrp) AS dis_val,
                   SUM(GREATEST(-mw, 0) * p.rrp) AS chg_val
              FROM g JOIN prices30 p
                ON p.settlementdate = g.ts AND p.regionid = g.region
             GROUP BY g.ts
        )
        SELECT strftime(ts, '%H:%M') AS slot,
               AVG(dis) AS dis_mw, AVG(chg) AS chg_mw,
               SUM(dis) * 0.5 AS dis_mwh, SUM(chg) * 0.5 AS chg_mwh,
               SUM(dis_val) * 0.5 AS dis_val, SUM(chg_val) * 0.5 AS chg_val,
               COUNT(*) AS n_days
          FROM per GROUP BY slot
    """, [s_ts, e_ts])

    if region == "NEM":
        spot = q(f"""
            WITH nem AS (
                SELECT p.settlementdate AS ts,
                       SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) AS rrp
                  FROM prices30 p JOIN demand30 d
                    ON p.settlementdate = d.settlementdate
                   AND p.regionid = d.regionid
                 WHERE p.regionid IN {MAINLAND}
                   AND p.settlementdate >= ? AND p.settlementdate < ?
                 GROUP BY p.settlementdate
            )
            SELECT strftime(ts, '%H:%M') AS slot, AVG(rrp) AS spot
              FROM nem WHERE rrp IS NOT NULL GROUP BY slot
        """, [s_ts, e_ts])
    else:
        spot = q(f"""
            SELECT strftime(settlementdate, '%H:%M') AS slot, AVG(rrp) AS spot
              FROM prices30
             WHERE regionid = '{region}'
               AND settlementdate >= ? AND settlementdate < ?
             GROUP BY slot
        """, [s_ts, e_ts])

    slots = pd.DataFrame({"slot": [f"{h:02d}:{m:02d}"
                                   for h in range(24) for m in (0, 30)]})
    df = slots.merge(batt, on="slot", how="left").merge(spot, on="slot",
                                                        how="left")
    hm = df["slot"].str.split(":", expand=True).astype(int)
    df["x"] = hm[0] + hm[1] / 60
    df.loc[df["x"] == 0, "x"] = 24.0
    df = df.sort_values("x").reset_index(drop=True)
    df["dis_px"] = df["dis_val"] / df["dis_mwh"].where(df["dis_mwh"] > 0)
    df["chg_px"] = df["chg_val"] / df["chg_mwh"].where(df["chg_mwh"] > 0)
    # Plotted price only where that side's volume is material: a 60 MW
    # discharge sliver at 15:00 that catches a spike otherwise sets the axis.
    for side in ("dis", "chg"):
        mw = df[f"{side}_mw"]
        df[f"{side}_px_plot"] = df[f"{side}_px"].where(
            mw >= MIN_VOLUME_SHARE * mw.max())
    return df


def summary(df: pd.DataFrame) -> dict:
    dis_mwh, chg_mwh = df["dis_mwh"].sum(), df["chg_mwh"].sum()
    dis_px = df["dis_val"].sum() / dis_mwh if dis_mwh else float("nan")
    chg_px = df["chg_val"].sum() / chg_mwh if chg_mwh else float("nan")
    return {
        "dis_gwh": dis_mwh / 1000, "chg_gwh": chg_mwh / 1000,
        "dis_px": dis_px, "chg_px": chg_px, "spread": dis_px - chg_px,
        "ratio": dis_mwh / chg_mwh * 100 if chg_mwh else float("nan"),
        "net_rev_m": (df["dis_val"].sum() - df["chg_val"].sum()) / 1e6,
    }


def _hhmm(x: float) -> str:
    return f"{int(x):02d}:{int(round((x % 1) * 60)):02d}"


def build_figure(df: pd.DataFrame, region_label: str) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    span = [f"{_hhmm(x - 0.5)}–{_hhmm(x)}" for x in df["x"]]

    fig.add_trace(go.Bar(
        x=df["x"], y=df["dis_mw"], name="Discharge MW",
        marker=dict(color=DISCHARGE_COLOR, opacity=0.75), width=0.42,
        customdata=span,
        hovertemplate="%{customdata}<br>Discharge %{y:,.0f} MW<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Bar(
        x=df["x"], y=-df["chg_mw"], name="Charge MW",
        marker=dict(color=CHARGE_COLOR, opacity=0.75), width=0.42,
        customdata=list(zip(span, df["chg_mw"])),
        hovertemplate="%{customdata[0]}<br>Charge %{customdata[1]:,.0f} MW"
                      "<extra></extra>",
    ), secondary_y=False)
    for col, name, color, dash in [
        ("dis_px_plot", "Discharge $/MWh", DISCHARGE_PRICE_COLOR, "solid"),
        ("chg_px_plot", "Charge $/MWh", CHARGE_PRICE_COLOR, "solid"),
        ("spot", "Spot $/MWh" + (" (demand-wtd)" if region_label == "NEM"
                                 else ""), SPOT_COLOR, "dot"),
    ]:
        fig.add_trace(go.Scatter(
            x=df["x"], y=df[col], name=name, mode="lines",
            line=dict(color=color, width=2.2 if dash == "solid" else 1.5,
                      dash=dash, shape="spline", smoothing=0.4),
            customdata=span, connectgaps=False,
            hovertemplate=f"%{{customdata}}<br>{name} $%{{y:,.0f}}"
                          "<extra></extra>",
        ), secondary_y=True)

    fig.update_layout(
        barmode="relative", bargap=0,
        paper_bgcolor=PAPER, plot_bgcolor=PAPER,
        font=dict(family="Inter, -apple-system, sans-serif", size=12,
                  color=INK),
        height=520, margin=dict(l=64, r=64, t=16, b=40),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0,
                    font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(
        range=[0, 24.25], tickvals=list(range(0, 25, 3)),
        ticktext=[f"{h:02d}:00" for h in range(0, 25, 3)],
        title=dict(text="Time of day (NEM time, half-hour ending)",
                   font=dict(size=11, color=MUTED)),
        gridcolor=BORDER, gridwidth=0.5, showline=False,
    )
    fig.update_yaxes(
        title=dict(text="Average MW  (charge below zero)",
                   font=dict(size=11, color=MUTED)),
        gridcolor=BORDER, gridwidth=0.5, zeroline=True,
        zerolinecolor=MUTED, zerolinewidth=1, tickformat=",.0f",
        secondary_y=False,
    )
    fig.update_yaxes(
        title=dict(text="$/MWh", font=dict(size=11, color=MUTED)),
        showgrid=False, zeroline=False, tickprefix="$", tickformat=",.0f",
        rangemode="tozero", secondary_y=True,
    )
    return fig


def _fmt(v: float, fmt: str) -> str:
    """fmt.format(v); a leading '$' moves outside the sign: −$36/MWh."""
    if pd.isna(v):
        return "–"
    if fmt.startswith("$") and v < 0:
        return "−" + fmt.format(-v)
    return fmt.format(v)


def content_html(region: str, range_slug: str) -> str:
    """Stats strip + chart, ready to drop into a page or an HTMX swap.

    The plot div id is unique per render: htmx's settle step copies the old
    element's attributes onto a new element with the same id, and Plotly
    initialising against those stale attributes garbles the chart."""
    div_id = f"battery-tod-plot-{int(datetime.now().timestamp() * 1000)}"
    s_ts, e_ts, range_label = window(range_slug)
    region_label = dict(REGIONS).get(region, region)
    df = load_profile(region, s_ts, e_ts)
    if df["dis_mwh"].fillna(0).sum() + df["chg_mwh"].fillna(0).sum() == 0:
        return (f'<div class="placeholder">No battery output in '
                f'{region_label} for the {range_label}.</div>')
    st = summary(df)
    tiles = [
        ("Charged", _fmt(st["chg_gwh"], "{:,.1f} GWh")),
        ("Discharged", _fmt(st["dis_gwh"], "{:,.1f} GWh")),
        ("Discharge ÷ charge", _fmt(st["ratio"], "{:.0f}%")),
        ("Avg charge price", _fmt(st["chg_px"], "${:,.0f}/MWh")),  # may be < 0
        ("Avg discharge price", _fmt(st["dis_px"], "${:,.0f}/MWh")),
        ("Spread", _fmt(st["spread"], "${:,.0f}/MWh")),
        ("Net energy revenue", _fmt(st["net_rev_m"], "${:,.1f}m")),
    ]
    strip = "".join(f'<div class="tile"><div class="k">{k}</div>'
                    f'<div class="v">{v}</div></div>' for k, v in tiles)
    fig_json = build_figure(df, region_label).to_json()
    cfg = json.dumps({"displaylogo": False, "responsive": True})
    first, last = s_ts.strftime("%-d %b %Y"), e_ts.strftime("%-d %b %Y %H:%M")
    return (
        f'<div class="sub">{region_label} batteries, {range_label} '
        f'({first} → {last}). Prices are volume-weighted: what the fleet paid '
        f'to charge and earned on discharge in each half hour.</div>'
        f'<div class="strip">{strip}</div>'
        f'<div id="{div_id}" style="height:520px"></div>'
        f'<script>(function(){{var f={fig_json};'
        f'Plotly.newPlot("{div_id}",f.data,f.layout,{cfg});}})();</script>'
        f'<div class="note">Source: AEMO SCADA (30-min) and regional prices. '
        f'Discharge ÷ charge is energy out over energy in at the meter, not '
        f'round-trip efficiency of any one unit. Charge and discharge prices '
        f'are left off the chart in half hours where that side runs below '
        f'{MIN_VOLUME_SHARE:.0%} of its peak MW; the tiles include every '
        f'MWh.</div>'
    )
