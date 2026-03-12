"""
STTM Gas Ex-Post Price & Volume tab for the AEMO dashboard.

Reads from sttm_expost table in DuckDB (populated by update_sttm_prices.py).
Sub-tabs: Prices (existing chart) and Volume (demand/withdrawal charts).
"""

import os
import logging

import duckdb
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import panel as pn
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── Flexoki Light theme ──────────────────────────────────────────
PAPER = "#FFFCF0"
BLACK = "#100F0F"
TEXT = "#403E3C"
MUTED = "#6F6E69"
UI = "#E6E4D9"
BLUE = "#205EA6"
ORANGE = "#BC5215"
CYAN = "#24837B"
GREEN = "#66800B"

REGIONS = {
    "Sydney (NSW)": "SYD",
    "Brisbane (QLD)": "BRI",
    "Adelaide (SA)": "ADL",
    "STTM Average": "AVG",
}

REGION_COLORS = {
    "SYD": BLUE,
    "BRI": ORANGE,
    "ADL": CYAN,
    "AVG": BLACK,
}

PRESETS = {
    "Last 3 months": 90,
    "Last 6 months": 180,
    "Last 1 year": 365,
    "Last 2 years": 730,
    "Last 5 years": 1825,
    "All data": None,
}

DB_PATH = os.getenv(
    "AEMO_DUCKDB_PATH",
    "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb",
)


def _load_prices():
    """Load all STTM ex-post prices from DuckDB and compute average."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(
        "SELECT gas_date, hub, expost_price AS price FROM sttm_expost ORDER BY gas_date"
    ).df()
    conn.close()

    df["gas_date"] = pd.to_datetime(df["gas_date"])

    # Compute STTM average
    pivot = df.pivot(index="gas_date", columns="hub", values="price")
    avg = pivot[["SYD", "ADL", "BRI"]].mean(axis=1)
    avg_df = pd.DataFrame({"gas_date": avg.index, "hub": "AVG", "price": avg.values})
    df = pd.concat([df, avg_df], ignore_index=True).sort_values("gas_date").reset_index(drop=True)

    return df


def _load_volumes():
    """Load network allocation (demand) data from DuckDB."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(
        "SELECT gas_date, hub, network_allocation "
        "FROM sttm_expost "
        "WHERE network_allocation IS NOT NULL "
        "ORDER BY gas_date"
    ).df()
    conn.close()

    df["gas_date"] = pd.to_datetime(df["gas_date"])
    # Convert GJ to TJ
    df["demand_tj"] = df["network_allocation"] / 1000.0
    return df


def _build_figure(df, region_code, start_date, end_date):
    """Build Plotly figure for a region and date range."""
    mask = (
        (df["hub"] == region_code)
        & (df["gas_date"] >= pd.Timestamp(start_date))
        & (df["gas_date"] <= pd.Timestamp(end_date))
    )
    sub = df.loc[mask].copy()
    color = REGION_COLORS[region_code]
    region_label = [k for k, v in REGIONS.items() if v == region_code][0]

    fig = go.Figure()

    if sub.empty:
        fig.add_annotation(
            text="No data for selected period", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(size=16, color=MUTED),
        )
    else:
        # Clip extreme outliers for display (STTM start-up spikes)
        sub["price_display"] = sub["price"].clip(upper=100)

        fig.add_trace(go.Scatter(
            x=sub["gas_date"], y=sub["price_display"],
            mode="lines",
            line=dict(color=color, width=1.5),
            name="Ex-post price",
            customdata=sub["price"],
            hovertemplate="%{x|%d %b %Y}<br>$%{customdata:.2f}/GJ<extra></extra>",
        ))

        # Stats annotation
        mean_price = sub["price"].mean()
        max_price = sub["price"].max()
        max_date = sub.loc[sub["price"].idxmax(), "gas_date"]
        latest = sub.iloc[-1]

        stats_text = (
            f"Latest: ${latest['price']:.2f}/GJ ({latest['gas_date'].strftime('%d %b %Y')})<br>"
            f"Period avg: ${mean_price:.2f}/GJ<br>"
            f"Peak: ${max_price:.2f}/GJ ({max_date.strftime('%d %b %Y')})"
        )
        fig.add_annotation(
            text=stats_text, xref="paper", yref="paper",
            x=0.01, y=0.99, xanchor="left", yanchor="top",
            showarrow=False,
            font=dict(size=11, color=TEXT),
            bgcolor=UI, bordercolor=UI, opacity=0.9,
        )

    fig.update_layout(
        title=dict(
            text=f"{region_label} — STTM Ex-Post Gas Price",
            font=dict(size=16, color=BLACK, family="Inter, sans-serif"),
            x=0.0, xanchor="left",
        ),
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        font=dict(color=TEXT, family="Inter, sans-serif"),
        xaxis=dict(gridcolor=UI, gridwidth=0.5),
        yaxis=dict(
            title="$/GJ",
            gridcolor=UI, gridwidth=0.5,
            zeroline=False,
            fixedrange=False,
        ),
        height=500,
        margin=dict(l=50, r=30, t=50, b=30),
        showlegend=False,
        annotations=list(fig.layout.annotations or []) + [
            dict(
                text="Data: AEMO STTM, calculations & plot ITK",
                xref="paper", yref="paper",
                x=1.0, y=-0.08, xanchor="right", yanchor="bottom",
                showarrow=False,
                font=dict(size=10, color=MUTED, family="Inter, sans-serif"),
            ),
        ],
    )

    return fig


# ── Volume charts ────────────────────────────────────────────────

# Colors for year overlay (most recent = most prominent)
YEAR_COLORS = {
    0: BLUE,    # current year
    1: ORANGE,  # previous year
    2: CYAN,    # two years ago
}
YEAR_WIDTHS = {0: 2.0, 1: 1.5, 2: 1.0}


def _build_total_demand_figure(vol_df):
    """Total STTM demand (all hubs), 7-day MA, last 3 calendar years overlaid."""
    if vol_df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No volume data available", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(size=16, color=MUTED),
        )
        _apply_volume_layout(fig, "Total STTM Gas Demand (all hubs)")
        return fig

    # Sum across hubs per day
    daily = vol_df.groupby("gas_date")["demand_tj"].sum().reset_index()
    daily = daily.sort_values("gas_date")
    daily["ma7"] = daily["demand_tj"].rolling(7, min_periods=1).mean()
    daily["year"] = daily["gas_date"].dt.year
    daily["day_of_year"] = daily["gas_date"].dt.dayofyear

    current_year = daily["year"].max()
    years = sorted(daily["year"].unique())
    # Take last 3 years
    show_years = years[-3:] if len(years) >= 3 else years

    fig = go.Figure()
    for yr in show_years:
        yr_data = daily[daily["year"] == yr].copy()
        age = current_year - yr  # 0 = current, 1 = last, 2 = two ago
        color = YEAR_COLORS.get(age, MUTED)
        width = YEAR_WIDTHS.get(age, 1.0)
        dash = None if age == 0 else "dot" if age == 2 else "dash"

        fig.add_trace(go.Scatter(
            x=yr_data["day_of_year"],
            y=yr_data["ma7"],
            mode="lines",
            line=dict(color=color, width=width, dash=dash),
            name=str(yr),
            hovertemplate=f"{yr}<br>Day %{{x}}<br>%{{y:.1f}} TJ/day<extra></extra>",
        ))

    _apply_volume_layout(fig, "Total STTM Gas Demand (all hubs)")
    fig.update_layout(
        xaxis=dict(
            title="Day of year",
            range=[1, 366],
            dtick=30,
        ),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            font=dict(size=12),
        ),
    )
    return fig


def _build_hub_demand_figure(vol_df, selected_hubs=None):
    """Per-hub demand, 7-day MA, last 3 calendar years overlaid."""
    if vol_df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No volume data available", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(size=16, color=MUTED),
        )
        _apply_volume_layout(fig, "STTM Gas Demand by Hub")
        return fig

    if selected_hubs is None:
        selected_hubs = ["SYD", "BRI", "ADL"]

    vol_df = vol_df[vol_df["hub"].isin(selected_hubs)].copy()
    vol_df = vol_df.sort_values(["hub", "gas_date"])
    vol_df["ma7"] = vol_df.groupby("hub")["demand_tj"].transform(
        lambda s: s.rolling(7, min_periods=1).mean()
    )
    vol_df["year"] = vol_df["gas_date"].dt.year
    vol_df["day_of_year"] = vol_df["gas_date"].dt.dayofyear

    current_year = vol_df["year"].max()
    years = sorted(vol_df["year"].unique())
    show_years = years[-3:] if len(years) >= 3 else years

    hub_colors = {"SYD": BLUE, "BRI": ORANGE, "ADL": CYAN}
    hub_labels = {"SYD": "Sydney", "BRI": "Brisbane", "ADL": "Adelaide"}

    fig = go.Figure()
    for hub in selected_hubs:
        hub_data = vol_df[vol_df["hub"] == hub]
        for yr in show_years:
            yr_data = hub_data[hub_data["year"] == yr].copy()
            if yr_data.empty:
                continue
            age = current_year - yr
            color = hub_colors.get(hub, MUTED)
            width = YEAR_WIDTHS.get(age, 1.0)
            dash = None if age == 0 else "dot" if age == 2 else "dash"

            fig.add_trace(go.Scatter(
                x=yr_data["day_of_year"],
                y=yr_data["ma7"],
                mode="lines",
                line=dict(color=color, width=width, dash=dash),
                name=f"{hub_labels.get(hub, hub)} {yr}",
                hovertemplate=f"{hub_labels.get(hub, hub)} {yr}<br>Day %{{x}}<br>%{{y:.1f}} TJ/day<extra></extra>",
            ))

    _apply_volume_layout(fig, "STTM Gas Demand by Hub")
    fig.update_layout(
        xaxis=dict(
            title="Day of year",
            range=[1, 366],
            dtick=30,
        ),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            font=dict(size=11),
        ),
    )
    return fig


def _apply_volume_layout(fig, title):
    """Apply Flexoki Light layout to volume charts."""
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=16, color=BLACK, family="Inter, sans-serif"),
            x=0.0, xanchor="left",
        ),
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        font=dict(color=TEXT, family="Inter, sans-serif"),
        xaxis=dict(gridcolor=UI, gridwidth=0.5),
        yaxis=dict(
            title="TJ/day",
            gridcolor=UI, gridwidth=0.5,
            zeroline=False,
            fixedrange=False,
        ),
        height=450,
        margin=dict(l=50, r=30, t=60, b=40),
        annotations=[
            dict(
                text="Data: AEMO STTM, calculations & plot ITK",
                xref="paper", yref="paper",
                x=1.0, y=-0.12, xanchor="right", yanchor="bottom",
                showarrow=False,
                font=dict(size=10, color=MUTED, family="Inter, sans-serif"),
            ),
        ],
    )


# ── Tab builders ─────────────────────────────────────────────────

def _create_price_tab(all_prices, date_min, date_max):
    """Create the Prices sub-tab (existing functionality)."""
    region_select = pn.widgets.Select(
        name="Region", options=list(REGIONS.keys()), value="Sydney (NSW)",
        width=160,
    )
    period_select = pn.widgets.Select(
        name="Period", options=list(PRESETS.keys()), value="Last 2 years",
        width=140,
    )
    chart_pane = pn.pane.Plotly(None, sizing_mode="stretch_width", height=530)
    status = pn.pane.HTML("", styles={"color": MUTED, "font-size": "12px"})

    def update(event=None):
        region_code = REGIONS[region_select.value]
        days = PRESETS[period_select.value]

        if days is None:
            start = date_min
        else:
            start = date_max - timedelta(days=days)

        fig = _build_figure(all_prices, region_code, start, date_max)
        chart_pane.object = fig

        mask = (
            (all_prices["hub"] == region_code)
            & (all_prices["gas_date"] >= pd.Timestamp(start))
            & (all_prices["gas_date"] <= pd.Timestamp(date_max))
        )
        n = mask.sum()
        status.object = f"{n:,} days — {start.strftime('%d %b %Y')} to {date_max.strftime('%d %b %Y')}"

    region_select.param.watch(update, "value")
    period_select.param.watch(update, "value")
    update()

    return pn.Column(
        pn.Row(region_select, period_select, status, sizing_mode="stretch_width"),
        chart_pane,
        sizing_mode="stretch_width",
        styles={"background-color": PAPER},
    )


def _create_volume_tab(vol_df):
    """Create the Volume sub-tab with total and per-hub demand charts."""
    if vol_df.empty:
        return pn.pane.HTML(
            '<div style="padding:20px;color:#6F6E69;">No volume data available yet. '
            'Run update_sttm_prices.py --bootstrap to load historical data.</div>'
        )

    # Total demand chart
    total_chart = pn.pane.Plotly(
        _build_total_demand_figure(vol_df),
        sizing_mode="stretch_width",
        height=480,
    )

    # Hub demand chart with hub selector
    hub_options = {"All hubs": ["SYD", "BRI", "ADL"]}
    hub_options.update({
        "Sydney (SYD)": ["SYD"],
        "Brisbane (BRI)": ["BRI"],
        "Adelaide (ADL)": ["ADL"],
    })
    hub_select = pn.widgets.Select(
        name="Hub", options=list(hub_options.keys()), value="All hubs",
        width=160,
    )
    hub_chart = pn.pane.Plotly(None, sizing_mode="stretch_width", height=480)

    def update_hub(event=None):
        hubs = hub_options[hub_select.value]
        hub_chart.object = _build_hub_demand_figure(vol_df, hubs)

    hub_select.param.watch(update_hub, "value")
    update_hub()

    vol_min = vol_df["gas_date"].min().strftime("%d %b %Y")
    vol_max = vol_df["gas_date"].max().strftime("%d %b %Y")
    vol_status = pn.pane.HTML(
        f'<span style="color:{MUTED};font-size:12px;">'
        f'Volume data: {vol_min} to {vol_max} — 7-day rolling average, TJ/day</span>',
    )

    return pn.Column(
        vol_status,
        total_chart,
        pn.Row(hub_select, sizing_mode="stretch_width"),
        hub_chart,
        sizing_mode="stretch_width",
        styles={"background-color": PAPER},
    )


def create_sttm_gas_tab():
    """Create the STTM Gas tab panel with Price and Volume sub-tabs."""
    logger.info("Creating STTM Gas tab")

    # Load price data
    try:
        all_prices = _load_prices()
        date_max = all_prices["gas_date"].max()
        date_min = all_prices["gas_date"].min()
        logger.info("STTM gas: %d rows, %s to %s", len(all_prices), date_min.date(), date_max.date())
    except Exception as e:
        logger.error("Failed to load STTM gas data: %s", e)
        return pn.pane.HTML(
            f'<div style="padding:20px;color:#AF3029;">Error loading STTM data: {e}</div>'
        )

    # Load volume data
    try:
        vol_df = _load_volumes()
        logger.info("STTM volume: %d rows", len(vol_df))
    except Exception as e:
        logger.warning("Failed to load STTM volume data: %s", e)
        vol_df = pd.DataFrame(columns=["gas_date", "hub", "network_allocation", "demand_tj"])

    # Build sub-tabs
    price_tab = _create_price_tab(all_prices, date_min, date_max)
    volume_tab = _create_volume_tab(vol_df)

    return pn.Tabs(
        ("Prices", price_tab),
        ("Volume", volume_tab),
        sizing_mode="stretch_width",
        tabs_location="above",
        dynamic=True,
    )
