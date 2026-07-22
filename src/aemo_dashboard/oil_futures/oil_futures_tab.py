"""
Oil Futures Curve tab for the AEMO dashboard.

Shows the crude oil forward curve from the front (near-spot) month out to
~a year, overlaying three snapshots so you can compare the shape of the curve
(contango / backwardation) over time:

    - Today
    - 1 month ago
    - 1 year ago

Reads ``$AEMO_DATA_PATH/oil_futures.csv`` (produced by
``scripts/update_oil_futures.py``). File layout:
    index ``date``            : observation (settlement) date
    columns ``<Bench> <YYYY-MM>``: contract delivery month, e.g. ``WTI 2026-08``
    cell value                : that contract's settlement/close on that date

The x-axis is *months ahead of each observation date*, so all three snapshots
line up from their own spot; hover reveals the actual delivery month.
"""

import os
import re
import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import panel as pn

from ..shared.flexoki_theme import FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT

logger = logging.getLogger(__name__)

# ── Flexoki Light palette (shared theme) ──────────────────────────────
PAPER = FLEXOKI_PAPER
BLACK = FLEXOKI_BLACK
TEXT = FLEXOKI_BASE[800]
MUTED = FLEXOKI_BASE[600]
UI = FLEXOKI_BASE[100]
BLUE = FLEXOKI_ACCENT['blue']
ORANGE = FLEXOKI_ACCENT['orange']
CYAN = FLEXOKI_ACCENT['cyan']

# One horizon of ~13 forward months (front + 12).
MAX_MONTHS_AHEAD = 12

DATA_DIR = Path(os.getenv(
    "AEMO_DATA_PATH",
    "/Users/davidleitch/aemo_production/data",
))

# Column form: "WTI 2026-08" / "Brent 2026-08"
COL_RE = re.compile(r"^(\w+)\s+(\d{4})-(\d{2})$")

MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ── Data loading ──────────────────────────────────────────────────────

def _load_oil():
    """Load oil_futures.csv, return DataFrame indexed by observation date."""
    path = DATA_DIR / "oil_futures.csv"
    if not path.exists():
        logger.error(f"oil_futures.csv not found at {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()
    return df


def _parse_columns(columns):
    """Return {benchmark: {(year, month): column_name}}."""
    out = {}
    for col in columns:
        m = COL_RE.match(col)
        if not m:
            continue
        bench, year, month = m.group(1), int(m.group(2)), int(m.group(3))
        out.setdefault(bench, {})[(year, month)] = col
    return out


def _delivery_label(year, month):
    return f"{MONTH_ABBR[month]} {year}"


# ── Plotly layout helper ──────────────────────────────────────────────

def _apply_layout(fig, title, y_title="US$/bbl"):
    fig.update_layout(
        autosize=True,
        height=480,
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        font=dict(family="Inter, -apple-system, system-ui, sans-serif", color=TEXT, size=13),
        title=dict(text=title, font=dict(color=BLACK, size=16), x=0.0, xanchor="left"),
        xaxis=dict(
            title="Months ahead", showgrid=False, zeroline=False, showline=False,
            tickfont=dict(color=TEXT), dtick=1,
        ),
        yaxis=dict(
            title=y_title, showgrid=True, gridcolor=UI, gridwidth=1,
            zeroline=False, showline=False, tickfont=dict(color=TEXT),
        ),
        legend=dict(bgcolor=PAPER, bordercolor=PAPER, font=dict(color=BLACK, size=11)),
        margin=dict(l=60, r=30, t=50, b=60),
        annotations=[dict(
            text="<i>Data: Yahoo Finance, calculations, plot: ITK</i>",
            xref="paper", yref="paper", x=1.0, y=-0.12,
            showarrow=False, font=dict(size=9, color=MUTED), xanchor="right",
        )],
    )
    return fig


# ── Chart builder ─────────────────────────────────────────────────────

def _build_forward_curve(oil_df, contract_map, benchmark):
    """Overlay Today / 1 month ago / 1 year ago forward curves for a benchmark."""
    contracts = contract_map.get(benchmark, {})
    fig = go.Figure()

    if oil_df.empty or not contracts:
        return _apply_layout(fig, f"{benchmark} Oil Forward Curve")

    today = oil_df.index[-1]
    snapshots = [
        ("Today", today, BLUE, "solid"),
        ("1 month ago", today - timedelta(days=30), ORANGE, "dash"),
        ("1 year ago", today - timedelta(days=365), CYAN, "dot"),
    ]

    for label, target_date, color, dash in snapshots:
        idx = oil_df.index.searchsorted(target_date, side="right") - 1
        if idx < 0:
            continue  # no observation at/before this date yet
        row = oil_df.iloc[idx]
        obs_date = oil_df.index[idx]
        obs_year, obs_month = obs_date.year, obs_date.month

        # Forward strip as of the observation date: delivery >= obs month,
        # out to MAX_MONTHS_AHEAD months.
        points = []
        for (dy, dm), col in contracts.items():
            months_ahead = (dy - obs_year) * 12 + (dm - obs_month)
            if 0 <= months_ahead <= MAX_MONTHS_AHEAD:
                val = row.get(col)
                if pd.notna(val):
                    points.append((months_ahead, float(val), _delivery_label(dy, dm)))

        if len(points) < 2:
            continue  # not enough of the curve to draw
        points.sort()
        x_vals = [p[0] for p in points]
        y_vals = [p[1] for p in points]
        deliveries = [p[2] for p in points]

        display_label = f"{label} ({obs_date.strftime('%d %b %Y')})"
        fig.add_trace(go.Scatter(
            x=x_vals, y=y_vals, mode="lines+markers",
            name=display_label,
            line=dict(color=color, width=2.5, dash=dash),
            marker=dict(size=7),
            customdata=deliveries,
            hovertemplate="%{customdata} (+%{x}m): $%{y:.2f}/bbl<extra></extra>",
        ))

    _apply_layout(fig, f"{benchmark} Oil Forward Curve")
    return fig


# ── Public factory ────────────────────────────────────────────────────

PLOTLY_CFG = dict(sizing_mode="stretch_width")


def create_oil_futures_tab():
    """Create the Oil futures tab content. Returns a pn.Column."""
    logger.info("Creating oil futures tab...")

    oil_df = _load_oil()
    if oil_df.empty:
        return pn.Column(
            pn.pane.Markdown("# Oil Futures Curve"),
            pn.pane.Markdown(
                "**No data yet.** Run `scripts/update_oil_futures.py` on the "
                "production machine to create `oil_futures.csv` in "
                "`$AEMO_DATA_PATH`, then reload."
            ),
            sizing_mode="stretch_width",
        )

    contract_map = _parse_columns(oil_df.columns)
    benchmarks = sorted(contract_map.keys())
    if not benchmarks:
        return pn.Column(
            pn.pane.Markdown("# Oil Futures Curve"),
            pn.pane.Markdown("**Error:** no recognisable contract columns in oil_futures.csv."),
            sizing_mode="stretch_width",
        )

    default_bench = "WTI" if "WTI" in benchmarks else benchmarks[0]

    benchmark_select = pn.widgets.Select(
        name="Benchmark", options=benchmarks, value=default_bench, width=140,
    )

    curve_pane = pn.pane.Plotly(
        _build_forward_curve(oil_df, contract_map, default_bench), **PLOTLY_CFG,
    )

    def _on_benchmark(event):
        curve_pane.object = _build_forward_curve(oil_df, contract_map, event.new)

    benchmark_select.param.watch(_on_benchmark, "value")

    last_date = oil_df.index[-1].strftime("%d %b %Y")
    info_md = pn.pane.Markdown(
        f"*Data through {last_date}*",
        styles={"color": MUTED, "font-size": "12px"},
    )

    tab = pn.Column(
        pn.Row(benchmark_select, pn.Spacer(width=20), info_md, align="start"),
        pn.Column(curve_pane, sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
    )

    logger.info("Oil futures tab created successfully")
    return tab
