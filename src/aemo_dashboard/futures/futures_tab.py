"""
NEM Electricity Futures tab for the AEMO dashboard.

Sub-tabs: Forward Curve, Forward Expectations, Single Contract.
Reads futures.csv (ASX Energy weekly settlement prices) and spot prices from DuckDB.
"""

import os
import re
import logging
from datetime import timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import panel as pn

logger = logging.getLogger(__name__)

# ── Flexoki Light palette ──────────────────────────────────────────────
PAPER = "#FFFCF0"
BLACK = "#100F0F"
TEXT = "#403E3C"
MUTED = "#6F6E69"
UI = "#E6E4D9"
BLUE = "#205EA6"
ORANGE = "#BC5215"
CYAN = "#24837B"
GREEN = "#66800B"
MAGENTA = "#A02F6F"

REGION_COLORS = {"NSW": BLUE, "QLD": ORANGE, "SA": CYAN, "VIC": GREEN}
REGIONS = ["NSW", "QLD", "SA", "VIC"]

DATA_DIR = Path(os.getenv(
    "AEMO_DATA_PATH",
    "/Users/davidleitch/aemo_production/data",
))

DB_PATH = os.getenv(
    "AEMO_DUCKDB_PATH",
    "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb",
)

# ── Data loading ───────────────────────────────────────────────────────

COL_RE = re.compile(
    r"(?:ASX Energy Contract\s+)?(\w+)\s+(\d{4})\s+Q(\d)(?:\s+Base Load Futures Price.*)?$"
)


def _strip_column(col):
    """Strip long ASX prefix/suffix to short form 'NSW 2026 Q2'."""
    m = COL_RE.match(col)
    if m:
        return f"{m.group(1)} {m.group(2)} Q{m.group(3)}"
    return col


def _load_futures():
    """Load futures.csv, strip long column names, return DataFrame indexed by date."""
    path = DATA_DIR / "futures.csv"
    if not path.exists():
        logger.error(f"futures.csv not found at {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["Time (UTC+10)"])
    df = df.rename(columns={"Time (UTC+10)": "date"}).set_index("date")
    df.columns = [_strip_column(c) for c in df.columns]
    return df


def _parse_contract_columns(columns):
    """Return {region: {(year, quarter): col_name}}."""
    mapping = {}
    pat = re.compile(r"(\w+)\s+(\d{4})\s+Q(\d)")
    for col in columns:
        m = pat.match(col)
        if m:
            region, year, quarter = m.group(1), int(m.group(2)), int(m.group(3))
            mapping.setdefault(region, {})[(year, quarter)] = col
    return mapping


def _load_spot_weekly():
    """Load weekly average spot prices from DuckDB, return {region: Series}."""
    result = {}
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute("""
            SELECT
                date_trunc('week', settlementdate) AS week,
                REPLACE(regionid, '1', '') AS region,
                AVG(rrp) AS avg_price
            FROM prices30
            WHERE regionid IN ('NSW1','QLD1','SA1','VIC1')
            GROUP BY 1, 2
            ORDER BY 1
        """).fetchdf()
        con.close()
        for region in REGIONS:
            subset = df[df["region"] == region].set_index("week")["avg_price"]
            result[region] = subset
    except Exception as e:
        logger.error(f"Failed to load spot prices from DuckDB: {e}")
    return result


# ── Plotly layout helper ──────────────────────────────────────────────


def _apply_layout(fig, title, y_title="$/MWh"):
    fig.update_layout(
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        font=dict(family="Inter, -apple-system, system-ui, sans-serif", color=TEXT, size=13),
        title=dict(text=title, font=dict(color=BLACK, size=16), x=0.0, xanchor="left"),
        xaxis=dict(showgrid=False, zeroline=False, showline=False, tickfont=dict(color=TEXT)),
        yaxis=dict(
            title=y_title, showgrid=True, gridcolor=UI, gridwidth=1,
            zeroline=False, showline=False, tickfont=dict(color=TEXT),
        ),
        legend=dict(bgcolor=PAPER, bordercolor=PAPER, font=dict(color=BLACK, size=11)),
        margin=dict(l=60, r=30, t=50, b=60),
        annotations=[dict(
            text="<i>Data: Global-roam, calculations, plot: ITK</i>",
            xref="paper", yref="paper", x=1.0, y=-0.12,
            showarrow=False, font=dict(size=9, color=MUTED), xanchor="right",
        )],
    )
    return fig


def _quarter_label(year, q):
    return f"{year} Q{q}"


# ── Chart builders ────────────────────────────────────────────────────


def _build_forward_curve(futures_df, contract_map, region):
    today = futures_df.index[-1]
    now_year, now_quarter = today.year, (today.month - 1) // 3 + 1

    snapshots = [
        ("Today", today),
        ("3 months ago", today - timedelta(days=91)),
        ("1 year ago", today - timedelta(days=365)),
    ]
    colors = [BLUE, ORANGE, CYAN]
    dashes = ["solid", "dash", "dot"]

    contracts = contract_map.get(region, {})
    future_keys = sorted(
        [(y, q) for y, q in contracts if (y, q) >= (now_year, now_quarter)]
    )

    fig = go.Figure()
    for (label, snap_date), color, dash in zip(snapshots, colors, dashes):
        idx = futures_df.index.searchsorted(snap_date)
        idx = min(idx, len(futures_df) - 1)
        row = futures_df.iloc[idx]
        actual_date = futures_df.index[idx]

        x_labels, y_vals = [], []
        for y, q in future_keys:
            col = contracts[(y, q)]
            val = row[col]
            if pd.notna(val):
                x_labels.append(_quarter_label(y, q))
                y_vals.append(val)

        display_label = f"{label} ({actual_date.strftime('%d %b %Y')})"
        fig.add_trace(go.Scatter(
            x=x_labels, y=y_vals, mode="lines+markers",
            name=display_label,
            line=dict(color=color, width=2.5, dash=dash),
            marker=dict(size=7),
            hovertemplate="%{x}: $%{y:.1f}/MWh<extra></extra>",
        ))

    _apply_layout(fig, f"{region} Forward Curve")
    return fig


def _cal_forward_average(df, contracts, cal_year):
    cols = [contracts.get((cal_year, q)) for q in range(1, 5)]
    cols = [c for c in cols if c is not None and c in df.columns]
    if not cols:
        return pd.Series(np.nan, index=df.index)
    return df[cols].mean(axis=1)


def _build_forward_expectations(futures_df, contract_map, region):
    contracts = contract_map.get(region, {})
    today = futures_df.index[-1]
    current_year = today.year

    cal1_year = current_year + 1
    cal2_year = current_year + 2

    cal1 = _cal_forward_average(futures_df, contracts, cal1_year)
    cal2 = _cal_forward_average(futures_df, contracts, cal2_year)

    cutoff = pd.Timestamp("2024-01-01")
    cal1 = cal1[cal1.index >= cutoff].dropna()
    cal2 = cal2[cal2.index >= cutoff].dropna()

    fig = go.Figure()
    for series, label, color in [
        (cal1, f"Cal {cal1_year} (Cal+1)", BLUE),
        (cal2, f"Cal {cal2_year} (Cal+2)", ORANGE),
    ]:
        if series.empty:
            continue
        fig.add_trace(go.Scatter(
            x=series.index, y=series.values, mode="lines",
            name=label, line=dict(color=color, width=2.5),
            hovertemplate="%{x|%d %b %Y}: $%{y:.1f}/MWh<extra></extra>",
        ))
        for pt_idx in [0, -1]:
            fig.add_trace(go.Scatter(
                x=[series.index[pt_idx]], y=[series.values[pt_idx]],
                mode="markers+text",
                marker=dict(size=8, color=color, line=dict(color="white", width=1.5)),
                text=[f"${series.values[pt_idx]:.0f}"],
                textposition="middle right" if pt_idx == -1 else "middle left",
                textfont=dict(color=color, size=11),
                showlegend=False, hoverinfo="skip",
            ))

    _apply_layout(fig, f"{region} Calendar Year Forward Averages")
    return fig


def _build_futures_vs_spot(futures_df, contract_map, spot_weekly, region):
    contracts = contract_map.get(region, {})
    today = futures_df.index[-1]
    cal2_year = today.year + 2

    cal2 = _cal_forward_average(futures_df, contracts, cal2_year)
    cutoff = pd.Timestamp("2024-01-01")
    cal2 = cal2[cal2.index >= cutoff].dropna()

    fig = go.Figure()

    if not cal2.empty:
        fig.add_trace(go.Scatter(
            x=cal2.index, y=cal2.values, mode="lines",
            name=f"Cal {cal2_year} Forward",
            line=dict(color=BLUE, width=2.5),
            hovertemplate="%{x|%d %b %Y}: $%{y:.1f}/MWh<extra></extra>",
        ))

    spot_series = spot_weekly.get(region)
    if spot_series is not None and not spot_series.empty:
        trail = spot_series.rolling(52, min_periods=26).mean()
        trail = trail[trail.index >= cutoff].dropna()
        if not trail.empty:
            fig.add_trace(go.Scatter(
                x=trail.index, y=trail.values, mode="lines",
                name="Trailing 12-month Spot",
                line=dict(color=ORANGE, width=2.5),
                hovertemplate="%{x|%d %b %Y}: $%{y:.1f}/MWh<extra></extra>",
            ))

    _apply_layout(fig, f"{region} — Cal+2 Forward vs Trailing Spot")
    return fig


def _build_single_contract(futures_df, contract_map, year, quarter):
    fig = go.Figure()
    for region in REGIONS:
        contracts = contract_map.get(region, {})
        col = contracts.get((year, quarter))
        if col is None or col not in futures_df.columns:
            continue
        series = futures_df[col].dropna()
        if series.empty:
            continue
        fig.add_trace(go.Scatter(
            x=series.index, y=series.values, mode="lines",
            name=region, line=dict(color=REGION_COLORS[region], width=2.5),
            hovertemplate=f"{region}: $%{{y:.1f}}/MWh<extra></extra>",
        ))

    _apply_layout(fig, f"All Regions — {year} Q{quarter} Base Load Futures")
    return fig


# ── Public factory ────────────────────────────────────────────────────

PLOTLY_CFG = dict(sizing_mode="stretch_width", height=480)


def create_futures_tab():
    """Create the Futures tab content. Returns a pn.Column."""
    logger.info("Creating futures tab...")

    futures_df = _load_futures()
    if futures_df.empty:
        return pn.Column(
            pn.pane.Markdown("# Electricity Futures"),
            pn.pane.Markdown("**Error:** futures.csv not found or empty."),
            sizing_mode="stretch_width",
        )

    contract_map = _parse_contract_columns(futures_df.columns)
    spot_weekly = _load_spot_weekly()

    # Contract choices for single-contract tab
    all_keys = set()
    for rc in contract_map.values():
        all_keys.update(rc.keys())
    contract_choices = sorted(all_keys)
    contract_labels = [_quarter_label(y, q) for y, q in contract_choices]

    # ── Widgets ────────────────────────────────────────────────────
    region_select = pn.widgets.Select(
        name="Region", options=REGIONS, value="NSW", width=120,
    )
    contract_select = pn.widgets.Select(
        name="Contract",
        options=dict(zip(contract_labels, contract_choices)),
        value=contract_choices[-4] if len(contract_choices) >= 4 else contract_choices[0],
        width=150,
    )

    # ── Chart panes ────────────────────────────────────────────────
    fwd_curve_pane = pn.pane.Plotly(
        _build_forward_curve(futures_df, contract_map, "NSW"), **PLOTLY_CFG,
    )
    fwd_expect_pane = pn.pane.Plotly(
        _build_forward_expectations(futures_df, contract_map, "NSW"), **PLOTLY_CFG,
    )
    fvs_pane = pn.pane.Plotly(
        _build_futures_vs_spot(futures_df, contract_map, spot_weekly, "NSW"), **PLOTLY_CFG,
    )
    single_pane = pn.pane.Plotly(
        _build_single_contract(futures_df, contract_map, *contract_select.value), **PLOTLY_CFG,
    )

    # ── Callbacks ──────────────────────────────────────────────────
    def _on_region(event):
        r = event.new
        fwd_curve_pane.object = _build_forward_curve(futures_df, contract_map, r)
        fwd_expect_pane.object = _build_forward_expectations(futures_df, contract_map, r)
        fvs_pane.object = _build_futures_vs_spot(futures_df, contract_map, spot_weekly, r)

    def _on_contract(event):
        y, q = event.new
        single_pane.object = _build_single_contract(futures_df, contract_map, y, q)

    region_select.param.watch(_on_region, "value")
    contract_select.param.watch(_on_contract, "value")

    # ── Last-updated label ─────────────────────────────────────────
    last_date = futures_df.index[-1].strftime("%d %b %Y")
    info_md = pn.pane.Markdown(
        f"*Data through {last_date}*",
        styles={"color": MUTED, "font-size": "12px"},
    )

    # ── Sub-tabs ───────────────────────────────────────────────────
    sub_tabs = pn.Tabs(
        ("Forward Curve", pn.Column(fwd_curve_pane)),
        ("Forward Expectations", pn.Column(fwd_expect_pane, fvs_pane)),
        ("Single Contract", pn.Column(pn.Row(contract_select), single_pane)),
        sizing_mode="stretch_width",
    )

    tab = pn.Column(
        pn.Row(region_select, pn.Spacer(width=20), info_md, align="start"),
        sub_tabs,
        sizing_mode="stretch_width",
    )

    logger.info("Futures tab created successfully")
    return tab
