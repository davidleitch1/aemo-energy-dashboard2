"""
Oil futures tab for the AEMO dashboard.

Renders the WTI crude forward curve (spot ≈ front month out to ~1 year)
comparing three snapshots — today, 1 month ago, and 1 year ago — so the
whole curve's shift and change in shape (contango / backwardation) is
visible at a glance. A second sub-tab tracks the front-month price history.

Data comes from ``oil_futures.parquet`` (see ``oil_data.py``); refresh it
with ``aemo-oil-update`` (or ``--backfill`` for the one-time history seed).
"""

import logging

import pandas as pd
import plotly.graph_objects as go
import panel as pn

from ..shared.flexoki_theme import (
    FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT,
)
from . import oil_data

logger = logging.getLogger(__name__)

# ── Flexoki Light palette (shared theme) ──────────────────────────────
PAPER = FLEXOKI_PAPER
BLACK = FLEXOKI_BLACK
TEXT = FLEXOKI_BASE[800]
MUTED = FLEXOKI_BASE[600]
UI = FLEXOKI_BASE[100]
BLUE = FLEXOKI_ACCENT["blue"]
ORANGE = FLEXOKI_ACCENT["orange"]
CYAN = FLEXOKI_ACCENT["cyan"]

PLOTLY_CFG = dict(sizing_mode="stretch_width")


# ── Layout helper ──────────────────────────────────────────────────────

def _apply_layout(fig, title, x_title="", y_title="US$/bbl"):
    fig.update_layout(
        autosize=True,
        height=480,
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        font=dict(family="Inter, -apple-system, system-ui, sans-serif", color=TEXT, size=13),
        title=dict(text=title, font=dict(color=BLACK, size=16), x=0.0, xanchor="left"),
        xaxis=dict(
            title=x_title, showgrid=False, zeroline=False, showline=False,
            tickfont=dict(color=TEXT),
        ),
        yaxis=dict(
            title=y_title, showgrid=True, gridcolor=UI, gridwidth=1,
            zeroline=False, showline=False, tickfont=dict(color=TEXT),
        ),
        legend=dict(bgcolor=PAPER, bordercolor=PAPER, font=dict(color=BLACK, size=11)),
        margin=dict(l=60, r=30, t=50, b=60),
        annotations=[dict(
            text="<i>Data: Yahoo Finance (NYMEX WTI), calculations & plot: ITK</i>",
            xref="paper", yref="paper", x=1.0, y=-0.14,
            showarrow=False, font=dict(size=9, color=MUTED), xanchor="right",
        )],
    )
    return fig


# ── Chart builders ─────────────────────────────────────────────────────

def _build_curve_comparison(df):
    """Overlay the WTI curve at today / 1 month ago / 1 year ago on a
    common tenor axis, so shape and level shifts line up."""
    today = df["observation_date"].max()
    snapshots = [
        ("Today", today, BLUE, "solid"),
        ("1 month ago", today - pd.DateOffset(months=1), ORANGE, "dash"),
        ("1 year ago", today - pd.DateOffset(years=1), CYAN, "dot"),
    ]

    fig = go.Figure()
    tick_vals, tick_text = None, None

    for label, target, color, dash in snapshots:
        snap = oil_data.curve_as_of(df, target)
        if snap.empty:
            continue
        obs = snap.attrs.get("obs_date", target)

        # Use the *today* curve's delivery months as calendar tick labels
        # (tenors align across snapshots, so this reads as a calendar axis).
        if tick_vals is None and label == "Today":
            tick_vals = snap["tenor"].tolist()
            tick_text = [d.strftime("%b %y") for d in snap["delivery_month"]]

        fig.add_trace(go.Scatter(
            x=snap["tenor"],
            y=snap["price"],
            mode="lines+markers",
            name=f"{label} ({pd.Timestamp(obs).strftime('%d %b %Y')})",
            line=dict(color=color, width=2.5, dash=dash),
            marker=dict(size=7),
            customdata=[d.strftime("%b %Y") for d in snap["delivery_month"]],
            hovertemplate="%{customdata}: $%{y:.2f}/bbl<extra></extra>",
        ))

    if tick_vals:
        fig.update_xaxes(tickmode="array", tickvals=tick_vals, ticktext=tick_text)

    _apply_layout(fig, "WTI Crude Forward Curve — spot to 1 year", x_title="Delivery month")
    return fig


def _build_front_month(df):
    """Front-of-curve WTI price over the available history."""
    series = oil_data.front_month_series(df)
    fig = go.Figure()
    if not series.empty:
        fig.add_trace(go.Scatter(
            x=series.index, y=series.values, mode="lines",
            name="WTI front month",
            line=dict(color=BLUE, width=2.5),
            hovertemplate="%{x|%d %b %Y}: $%{y:.2f}/bbl<extra></extra>",
        ))
        last = series.iloc[-1]
        fig.add_trace(go.Scatter(
            x=[series.index[-1]], y=[last], mode="markers+text",
            marker=dict(size=8, color=BLUE, line=dict(color="white", width=1.5)),
            text=[f"${last:.2f}"], textposition="middle right",
            textfont=dict(color=BLUE, size=11),
            showlegend=False, hoverinfo="skip",
        ))
    _apply_layout(fig, "WTI Front-Month Price History", x_title="")
    return fig


# ── Public factory ─────────────────────────────────────────────────────

def create_oil_tab():
    """Create the Oil futures tab content. Returns a pn.Column."""
    logger.info("Creating oil tab...")

    df = oil_data.load_curve_data()
    if df.empty:
        return pn.Column(
            pn.pane.Markdown("# Oil Futures (WTI)"),
            pn.pane.Markdown(
                "**No data yet.** Seed the store by running "
                "`aemo-oil-update --backfill` in an environment with access "
                "to Yahoo Finance, then reload this tab."
            ),
            sizing_mode="stretch_width",
        )

    curve_pane = pn.pane.Plotly(_build_curve_comparison(df), **PLOTLY_CFG)
    front_pane = pn.pane.Plotly(_build_front_month(df), **PLOTLY_CFG)

    last_date = df["observation_date"].max().strftime("%d %b %Y")
    info_md = pn.pane.Markdown(
        f"*Data through {last_date}*",
        styles={"color": MUTED, "font-size": "12px"},
    )

    sub_tabs = pn.Tabs(
        ("Forward Curve", pn.Column(curve_pane, sizing_mode="stretch_width")),
        ("Front-Month History", pn.Column(front_pane, sizing_mode="stretch_width")),
        sizing_mode="stretch_width",
    )

    tab = pn.Column(
        pn.Row(info_md, align="start"),
        sub_tabs,
        sizing_mode="stretch_width",
    )
    logger.info("Oil tab created successfully")
    return tab
