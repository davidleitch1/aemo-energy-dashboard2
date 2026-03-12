"""
Today Tab - NEM at a Glance
===========================
Completely redesigned Today tab based on mockup V4.

Layout: 3 columns
- Left: Past 24 Hours (Generation mix, Key Events, Renewable Gauge)
- Center: Prices Now (Price chart with LOESS, Price table)
- Right: Looking Ahead (Forecast table, Market Notices)
"""

import os
import pandas as pd
import numpy as np
import panel as pn
import plotly.graph_objects as go
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from pathlib import Path
import pickle

DUCKDB_PATH = os.getenv('AEMO_DUCKDB_PATH')

# Import LOESS for price smoothing
try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOESS = True
except ImportError:
    HAS_LOESS = False

from ..shared.logging_config import get_logger
from ..shared.config import Config
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT
)

# Import forecast and notices components
from .forecast_components import fetch_predispatch_forecasts, create_forecast_table
from .market_notices import fetch_market_notices, create_notices_panel

# Import PASA outage components for generator outage summary on Today tab
from ..pasa.change_detector import ChangeDetector
from ..pasa.pasa_tab import (
    create_generator_fuel_summary, load_gen_info,
    DEFAULT_DATA_PATH as PASA_DATA_PATH,
)

logger = get_logger(__name__)

# Get config
config = Config()
DATA_PATH = config.data_dir

# =============================================================================
# FLEXOKI THEME CONSTANTS
# =============================================================================
FLEXOKI = {
    'background': FLEXOKI_PAPER,
    'foreground': FLEXOKI_BLACK,
    'text': FLEXOKI_BASE[800],
    'muted': FLEXOKI_BASE[600],
    'ui': FLEXOKI_BASE[100],
    'ui_border': FLEXOKI_BASE[200],
}

# Region colors
REGION_COLORS = {
    'NSW1': FLEXOKI_ACCENT['green'],
    'QLD1': FLEXOKI_ACCENT['orange'],
    'SA1': FLEXOKI_ACCENT['magenta'],
    'TAS1': FLEXOKI_ACCENT['cyan'],
    'VIC1': FLEXOKI_ACCENT['purple'],
}

# Fuel colors for generation chart
FUEL_COLORS = {
    'Coal': '#6F6E69',
    'Wind': FLEXOKI_ACCENT['green'],
    'Solar': FLEXOKI_ACCENT['yellow'],
    'Rooftop Solar': FLEXOKI_ACCENT['orange'],
    'Battery': FLEXOKI_ACCENT['purple'],
    'Hydro': FLEXOKI_ACCENT['blue'],
    'Gas': FLEXOKI_ACCENT['red'],
    'Other': FLEXOKI_BASE[400],
}

# Gauge fuel colors
GAUGE_FUEL_COLORS = {
    'hydro': FLEXOKI_ACCENT['blue'],
    'wind': FLEXOKI_ACCENT['green'],
    'solar': FLEXOKI_ACCENT['yellow'],
    'rooftop': FLEXOKI_ACCENT['orange'],
}


# =============================================================================
# DATA LOADING
# =============================================================================

def load_price_data(hours=24):
    """Load recent price data from DuckDB (preferred) or parquet fallback."""
    try:
        if DUCKDB_PATH:
            import duckdb
            conn = duckdb.connect(DUCKDB_PATH, read_only=True)
            prices_df = conn.execute(
                "SELECT settlementdate AS SETTLEMENTDATE, regionid AS REGIONID, rrp AS RRP "
                "FROM prices5 ORDER BY settlementdate DESC LIMIT ?",
                [hours * 12 * 5 + 100]  # ~hours worth of 5-min data across 5 regions, with buffer
            ).df()
            conn.close()
            logger.info(f"Loaded {len(prices_df)} price records from DuckDB")
        else:
            prices_path = DATA_PATH / 'prices5.parquet'
            prices_df = pd.read_parquet(prices_path)
            if 'settlementdate' in prices_df.columns:
                prices_df = prices_df.rename(columns={
                    'settlementdate': 'SETTLEMENTDATE',
                    'regionid': 'REGIONID',
                    'rrp': 'RRP'
                })

        prices_df['SETTLEMENTDATE'] = pd.to_datetime(prices_df['SETTLEMENTDATE'])
        end_time = prices_df['SETTLEMENTDATE'].max()
        start_time = end_time - timedelta(hours=hours)
        prices_df = prices_df[prices_df['SETTLEMENTDATE'] >= start_time]

        return prices_df, end_time

    except Exception as e:
        logger.error(f"Error loading price data: {e}")
        return pd.DataFrame(), datetime.now()


def load_generation_data(hours=24):
    """Load recent generation data aggregated by fuel type, including rooftop solar."""
    try:
        if DUCKDB_PATH:
            import duckdb
            conn = duckdb.connect(DUCKDB_PATH, read_only=True)
            cutoff = datetime.now() - timedelta(hours=hours)
            scada_df = conn.execute(
                "SELECT settlementdate AS SETTLEMENTDATE, duid AS DUID, scadavalue AS MW "
                "FROM scada5 WHERE settlementdate >= ? "
                "ORDER BY settlementdate",
                [cutoff]
            ).df()
            conn.close()
            logger.info(f"Loaded {len(scada_df)} scada records from DuckDB")
        else:
            scada_path = DATA_PATH / 'scada5.parquet'
            scada_df = pd.read_parquet(scada_path)
            if 'settlementdate' in scada_df.columns:
                scada_df = scada_df.rename(columns={
                    'settlementdate': 'SETTLEMENTDATE',
                    'duid': 'DUID',
                    'scadavalue': 'MW'
                })

        scada_df['SETTLEMENTDATE'] = pd.to_datetime(scada_df['SETTLEMENTDATE'])
        end_time = scada_df['SETTLEMENTDATE'].max()
        start_time = end_time - timedelta(hours=hours)
        scada_df = scada_df[scada_df['SETTLEMENTDATE'] >= start_time]

        # Load generator info for fuel mapping
        gen_info_path = DATA_PATH / 'gen_info.pkl'
        with open(gen_info_path, 'rb') as f:
            gen_info = pickle.load(f)

        if isinstance(gen_info, dict):
            gen_info_df = pd.DataFrame.from_dict(gen_info, orient='index').reset_index()
            gen_info_df = gen_info_df.rename(columns={'index': 'DUID'})
        else:
            gen_info_df = gen_info

        # Find fuel column
        fuel_col = None
        for col in ['Fuel', 'fuel', 'fuel_type', 'FUEL_TYPE', 'fuel_source_descriptor']:
            if col in gen_info_df.columns:
                fuel_col = col
                break

        if fuel_col is None or 'DUID' not in gen_info_df.columns:
            return pd.DataFrame(), end_time

        merged = scada_df.merge(gen_info_df[['DUID', fuel_col]], on='DUID', how='left')
        merged = merged.rename(columns={fuel_col: 'FUEL_TYPE'})

        # Standardize fuel types
        fuel_map = {
            'Black Coal': 'Coal', 'Brown Coal': 'Coal', 'Coal': 'Coal',
            'Natural Gas / Fuel Oil': 'Gas', 'Natural Gas': 'Gas', 'Gas': 'Gas',
            'CCGT': 'Gas', 'OCGT': 'Gas', 'Gas other': 'Gas',
            'Water': 'Hydro', 'Hydro': 'Hydro',
            'Wind': 'Wind', 'Solar': 'Solar',
            'Battery': 'Battery', 'Battery Storage': 'Battery',
            'Biomass': 'Other', 'Bagasse': 'Other', 'Other': 'Other',
        }
        merged['FUEL_TYPE'] = merged['FUEL_TYPE'].map(lambda x: fuel_map.get(str(x), 'Other'))

        agg = merged.groupby(['SETTLEMENTDATE', 'FUEL_TYPE'])['MW'].sum().reset_index()

        # Load rooftop solar using the shared adapter (same as Generation Stack tab)
        try:
            from ..shared.rooftop_adapter import load_rooftop_data as load_rooftop_adapter

            rooftop_data = load_rooftop_adapter()

            if not rooftop_data.empty and len(agg) > 0:
                # Convert to datetime index
                rooftop_data['settlementdate'] = pd.to_datetime(rooftop_data['settlementdate'])
                rooftop_data = rooftop_data.set_index('settlementdate')

                # Filter to time range
                rooftop_data = rooftop_data[
                    (rooftop_data.index >= start_time) & (rooftop_data.index <= end_time)
                ]

                # Sum all regions for NEM total
                rooftop_series = rooftop_data.sum(axis=1)

                # Get SCADA timestamps
                scada_times = sorted(agg['SETTLEMENTDATE'].unique())

                # Align rooftop to SCADA times
                rooftop_aligned = rooftop_series.reindex(pd.DatetimeIndex(scada_times))

                # Forward-fill missing values (up to 2 hours = 24 periods at 5-min)
                rooftop_aligned = rooftop_aligned.ffill(limit=24)

                # Fill remaining NaN with 0
                rooftop_aligned = rooftop_aligned.fillna(0)

                # Create rooftop rows for aggregated data
                rooftop_rows = pd.DataFrame({
                    'SETTLEMENTDATE': rooftop_aligned.index,
                    'FUEL_TYPE': 'Rooftop Solar',
                    'MW': rooftop_aligned.values
                })

                agg = pd.concat([agg, rooftop_rows], ignore_index=True)
                logger.debug(f"Added {len(rooftop_rows)} rooftop solar data points via adapter")

        except Exception as e:
            logger.warning(f"Error loading rooftop data via adapter: {e}")

        return agg, end_time

    except Exception as e:
        logger.error(f"Error loading generation data: {e}")
        return pd.DataFrame(), datetime.now()


def load_demand_data():
    """Load current NEM demand (operational + rooftop) and historical records from DuckDB."""
    stats = {
        'current_mw': 0, 'hour_record_mw': 0, 'alltime_record_mw': 0,
        'alltime_min_mw': 15000, 'current_hour': 0, 'latest_time': None,
    }
    try:
        if not DUCKDB_PATH:
            return stats
        import duckdb
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        regions = "('NSW1','QLD1','VIC1','SA1','TAS1')"

        # Latest settlement period: operational demand + rooftop
        row = conn.execute(f"""
            WITH latest AS (
                SELECT MAX(settlementdate) AS ts FROM demand30
                WHERE regionid IN {regions}
            ),
            dem AS (
                SELECT SUM(demand) AS op_demand
                FROM demand30, latest
                WHERE settlementdate = latest.ts AND regionid IN {regions}
            ),
            roof AS (
                SELECT COALESCE(SUM(power), 0) AS rooftop
                FROM rooftop30, latest
                WHERE settlementdate = latest.ts AND regionid IN {regions}
            )
            SELECT dem.op_demand + roof.rooftop AS total_mw,
                   EXTRACT(HOUR FROM latest.ts) AS hr,
                   latest.ts
            FROM dem, roof, latest
        """).fetchone()

        if row and row[0]:
            stats['current_mw'] = row[0]
            stats['current_hour'] = int(row[1])
            stats['latest_time'] = row[2]

        # Hour-of-day record (demand + rooftop, same hour across all history)
        hour_rec = conn.execute(f"""
            SELECT MAX(period_total) FROM (
                SELECT d.settlementdate,
                       SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
                FROM demand30 d
                LEFT JOIN rooftop30 r
                    ON d.settlementdate = r.settlementdate AND d.regionid = r.regionid
                WHERE d.regionid IN {regions}
                  AND EXTRACT(HOUR FROM d.settlementdate) = ?
                GROUP BY d.settlementdate
            )
        """, [stats['current_hour']]).fetchone()

        if hour_rec and hour_rec[0]:
            stats['hour_record_mw'] = hour_rec[0]

        # All-time record (demand + rooftop)
        alltime = conn.execute(f"""
            SELECT MAX(period_total) FROM (
                SELECT d.settlementdate,
                       SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
                FROM demand30 d
                LEFT JOIN rooftop30 r
                    ON d.settlementdate = r.settlementdate AND d.regionid = r.regionid
                WHERE d.regionid IN {regions}
                GROUP BY d.settlementdate
            )
        """).fetchone()

        if alltime and alltime[0]:
            stats['alltime_record_mw'] = alltime[0]

        # All-time minimum (for gauge range)
        alltime_min = conn.execute(f"""
            SELECT MIN(period_total) FROM (
                SELECT d.settlementdate,
                       SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
                FROM demand30 d
                LEFT JOIN rooftop30 r
                    ON d.settlementdate = r.settlementdate AND d.regionid = r.regionid
                WHERE d.regionid IN {regions}
                GROUP BY d.settlementdate
                HAVING SUM(d.demand) > 0
            )
        """).fetchone()

        if alltime_min and alltime_min[0]:
            stats['alltime_min_mw'] = alltime_min[0]

        conn.close()
        logger.info(f"Demand data: current={stats['current_mw']:.0f} MW, "
                     f"hour({stats['current_hour']}:00) record={stats['hour_record_mw']:.0f} MW, "
                     f"all-time={stats['alltime_record_mw']:.0f} MW")
    except Exception as e:
        logger.error(f"Error loading demand data: {e}")

    return stats


# =============================================================================
# DEMAND GAUGE (green-to-red gradient)
# =============================================================================

def _demand_gradient_color(frac):
    """Get color from the green-to-red 12-step Flexoki gradient at fraction 0-1."""
    gradient = [
        '#66800B', '#7A9000', '#8D9E00', '#A09400',
        '#AD8301', '#B87508', '#C26510', '#BC5215',
        '#B5441A', '#AE3620', '#A62D25', '#AF3029',
    ]
    idx = min(int(frac * len(gradient)), len(gradient) - 1)
    return gradient[idx]


def create_demand_gauge(demand_stats, forecast_peak_mw=None):
    """Create demand gauge as matplotlib Dial — thin gradient arc with big number."""
    from matplotlib.patches import Wedge

    current_gw = demand_stats['current_mw'] / 1000
    alltime_max_gw = demand_stats['alltime_record_mw'] / 1000
    alltime_min_gw = demand_stats['alltime_min_mw'] / 1000

    # Range: 80% of historical min to 105% of historical max
    gauge_min = round(alltime_min_gw * 0.8)
    gauge_max = round(alltime_max_gw * 1.05)
    span = max(gauge_max - gauge_min, 1)

    fig, ax = plt.subplots(figsize=(4.8, 2.6))
    fig.patch.set_facecolor(FLEXOKI['background'])
    ax.set_facecolor(FLEXOKI['background'])

    cx, cy = 0.5, 0.22      # Arc center (axes coords)
    r_outer = 0.42           # Arc radius
    arc_width = 0.045        # Arc thickness

    # Unfilled background arc (full semicircle)
    bg_wedge = Wedge((cx, cy), r_outer, 0, 180, width=arc_width,
                      facecolor=FLEXOKI['ui'], edgecolor='none',
                      transform=ax.transAxes, zorder=2)
    ax.add_patch(bg_wedge)

    # Filled gradient portion up to current value
    value_frac = max(0, min(1, (current_gw - gauge_min) / span))
    n_segments = max(1, int(value_frac * 60))
    for i in range(n_segments):
        frac = i / max(1, n_segments)
        overall_frac = frac * value_frac
        start_angle = 180 - overall_frac * 180
        extent = -(180 * value_frac / n_segments) - 0.3
        color = _demand_gradient_color(overall_frac)
        w = Wedge((cx, cy), r_outer, start_angle + extent, start_angle,
                   width=arc_width, facecolor=color, edgecolor='none',
                   transform=ax.transAxes, zorder=3)
        ax.add_patch(w)

    # Dot marker at current position on arc
    angle = np.radians(180 - value_frac * 180)
    r_mid = r_outer - arc_width / 2
    dx = cx + r_mid * np.cos(angle)
    dy = cy + r_mid * np.sin(angle)
    ax.plot(dx, dy, 'o', color=FLEXOKI['foreground'], markersize=7,
            transform=ax.transAxes, zorder=10)

    # Big number centered in the arc
    ax.text(cx, 0.28, f'{current_gw:.1f}', ha='center', va='center',
            fontsize=30, fontweight='bold', color=FLEXOKI['foreground'],
            transform=ax.transAxes)
    ax.text(cx, 0.14, 'GW', ha='center', va='center',
            fontsize=13, color=FLEXOKI['muted'], transform=ax.transAxes)

    # Scale labels at min, mid, max
    ax.text(cx - r_outer - 0.02, cy - 0.05, f'{gauge_min:.0f}', ha='center',
            fontsize=8, color=FLEXOKI['muted'], transform=ax.transAxes)
    ax.text(cx + r_outer + 0.02, cy - 0.05, f'{gauge_max:.0f}', ha='center',
            fontsize=8, color=FLEXOKI['muted'], transform=ax.transAxes)
    mid_val = (gauge_min + gauge_max) / 2
    ax.text(cx, cy + r_outer + 0.04, f'{mid_val:.0f}', ha='center',
            fontsize=8, color=FLEXOKI['muted'], transform=ax.transAxes)

    # Title
    ax.text(cx, 0.92, 'NEM Demand', ha='center', va='center',
            fontsize=13, fontweight='bold', color=FLEXOKI['foreground'],
            transform=ax.transAxes)

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect('equal')
    ax.axis('off')
    fig.subplots_adjust(left=0.02, right=0.98, top=0.98, bottom=0.02)

    # Build legend HTML (unchanged from before)
    hour_record_gw = demand_stats['hour_record_mw'] / 1000
    hour_label = f"{demand_stats['current_hour']:02d}:00"

    legend_items = [
        f"<span>Now: <b>{current_gw:.1f} GW</b></span>",
        f"<span>{hour_label} record: <b>{hour_record_gw:.1f} GW</b></span>",
        f"<span>All-time: <b>{alltime_max_gw:.1f} GW</b></span>",
    ]
    if forecast_peak_mw and forecast_peak_mw > 0:
        forecast_gw = forecast_peak_mw / 1000
        legend_items.append(f"<span>Forecast peak: <b>{forecast_gw:.1f} GW</b></span>")

    legend_html = f"""
    <div style="display: flex; justify-content: center; gap: 12px; flex-wrap: wrap;
                font-size: 10px; color: {FLEXOKI['text']}; padding: 5px 0;">
        {''.join(legend_items)}
    </div>
    """

    return fig, legend_html


# =============================================================================
# BATTERY STORED ENERGY GAUGE (linear bar with 1h-ago reference)
# =============================================================================

def load_battery_stored():
    """Load NEM battery stored energy from BDU5 table (current and 1h ago)."""
    stats = {
        'stored_gwh': 0, 'stored_1h_gwh': 0,
        'recent_max_gwh': 11.0, 'latest_time': None,
    }
    if not DUCKDB_PATH:
        return stats
    try:
        import duckdb
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            # Latest stored energy (sum across mainland regions, TAS is NaN)
            latest = con.execute("""
                SELECT settlementdate,
                       SUM(bdu_energy_storage) as stored_mwh
                FROM bdu5
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM bdu5)
                  AND regionid IN ('NSW1','QLD1','VIC1','SA1')
                GROUP BY settlementdate
            """).fetchdf()
            if len(latest) > 0:
                stats['stored_gwh'] = latest['stored_mwh'].iloc[0] / 1000
                stats['latest_time'] = latest['settlementdate'].iloc[0]

            # 1 hour ago (closest period)
            one_hr = con.execute("""
                SELECT settlementdate,
                       SUM(bdu_energy_storage) as stored_mwh
                FROM bdu5
                WHERE regionid IN ('NSW1','QLD1','VIC1','SA1')
                  AND settlementdate <= (
                      SELECT MAX(settlementdate) - INTERVAL '55 minutes' FROM bdu5
                  )
                GROUP BY settlementdate
                ORDER BY settlementdate DESC
                LIMIT 1
            """).fetchdf()
            if len(one_hr) > 0:
                stats['stored_1h_gwh'] = one_hr['stored_mwh'].iloc[0] / 1000

            # 30-day rolling max as capacity proxy
            row = con.execute("""
                SELECT MAX(total) as max_mwh
                FROM (
                    SELECT SUM(bdu_energy_storage) as total
                    FROM bdu5
                    WHERE regionid IN ('NSW1','QLD1','VIC1','SA1')
                      AND settlementdate >= CAST(NOW() - INTERVAL '30 days' AS TIMESTAMP)
                    GROUP BY settlementdate
                )
            """).fetchone()
            if row and row[0]:
                stats['recent_max_gwh'] = row[0] / 1000

        finally:
            con.close()
    except Exception as e:
        logger.error(f"Error loading battery stored energy: {e}")
    return stats


def create_battery_gauge(batt_stats):
    """Create a horizontal linear bar gauge for NEM battery stored energy.

    Returns a matplotlib figure showing current stored GWh as a filled bar
    with a dashed marker for the 1-hour-ago level.
    """
    import matplotlib.patches as mpatches

    stored = batt_stats['stored_gwh']
    stored_1h = batt_stats['stored_1h_gwh']
    max_gwh = batt_stats['recent_max_gwh']

    fig, ax = plt.subplots(figsize=(4.8, 1.6))
    fig.patch.set_facecolor(FLEXOKI['background'])
    ax.set_facecolor(FLEXOKI['background'])

    bar_height = 0.5
    y_center = 0.5

    # Gradient bar (full range, filled up to current value)
    n_seg = 250
    for i in range(n_seg):
        frac = i / n_seg
        x_start = frac * max_gwh
        x_width = max_gwh / n_seg * 1.01

        # Interpolate: red -> orange -> yellow -> green
        if frac < 0.33:
            t = frac / 0.33
            r = int(175 * (1 - t) + 188 * t)
            g = int(48 * (1 - t) + 101 * t)
            b = int(41 * (1 - t) + 21 * t)
        elif frac < 0.66:
            t = (frac - 0.33) / 0.33
            r = int(188 * (1 - t) + 173 * t)
            g = int(101 * (1 - t) + 131 * t)
            b = int(21 * (1 - t) + 1 * t)
        else:
            t = (frac - 0.66) / 0.34
            r = int(173 * (1 - t) + 102 * t)
            g = int(131 * (1 - t) + 128 * t)
            b = int(1 * (1 - t) + 11 * t)

        color = f'#{r:02x}{g:02x}{b:02x}'
        ax.barh(y_center, x_width, left=x_start, height=bar_height,
                color=color, edgecolor='none')

    # Grey out unfilled portion
    if stored < max_gwh:
        ax.barh(y_center, max_gwh - stored, left=stored, height=bar_height,
                color=FLEXOKI['ui'], edgecolor='none', alpha=0.78)

    # Rounded border
    rect = mpatches.FancyBboxPatch(
        (0, y_center - bar_height / 2), max_gwh, bar_height,
        boxstyle="round,pad=0.06", linewidth=1.5,
        edgecolor=FLEXOKI['ui_border'], facecolor='none', zorder=5,
    )
    ax.add_patch(rect)

    marker_top = y_center + bar_height / 2 + 0.06
    marker_bot = y_center - bar_height / 2 - 0.06

    # 1-hour-ago marker (dashed, lighter)
    if stored_1h > 0 and abs(stored - stored_1h) > 0.05:
        ax.plot([stored_1h, stored_1h], [marker_bot, marker_top],
                color=FLEXOKI['muted'], linewidth=2, linestyle=(0, (4, 3)),
                solid_capstyle='round', zorder=6)
        # Label on the side with more room
        if stored_1h < stored:
            ha, x_off = 'right', -0.12
        else:
            ha, x_off = 'left', 0.12
        ax.text(stored_1h + x_off, marker_top + 0.04, '1h ago',
                ha=ha, va='bottom', fontsize=7, color=FLEXOKI['muted'],
                fontstyle='italic')

    # Current value marker (solid, bold)
    ax.plot([stored, stored], [marker_bot, marker_top],
            color=FLEXOKI['foreground'], linewidth=3,
            solid_capstyle='round', zorder=7)

    # Value label above current marker
    ax.text(stored, marker_top + 0.12,
            f'{stored:.1f} GWh', ha='center', va='bottom',
            fontsize=13, fontweight='bold', color=FLEXOKI['foreground'])

    # Title
    ax.text(0, marker_top + 0.38,
            'NEM Battery Stored Energy', ha='left', va='bottom',
            fontsize=11, fontweight='bold', color=FLEXOKI['foreground'])

    # Tick marks below
    tick_interval = 2
    ticks = np.arange(0, max_gwh + 0.1, tick_interval)
    for t in ticks:
        ax.plot([t, t], [y_center - bar_height / 2 - 0.04, y_center - bar_height / 2],
                color=FLEXOKI['muted'], linewidth=1, zorder=6)
        ax.text(t, y_center - bar_height / 2 - 0.07, f'{t:.0f}',
                ha='center', va='top', fontsize=8, color=FLEXOKI['text'])
    ax.text(max_gwh + 0.12, y_center - bar_height / 2 - 0.07, 'GWh',
            ha='left', va='top', fontsize=8, color=FLEXOKI['muted'])

    ax.set_xlim(-0.3, max_gwh + 0.8)
    ax.set_ylim(-0.15, 1.45)
    ax.set_aspect('auto')
    ax.axis('off')
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout(pad=0.2)
    return fig


# =============================================================================
# PRICE CHART (LOESS smoothing, $1500 cap)
# =============================================================================

def _build_price_legend_html(latest_prices):
    """Build an HTML legend row showing region colors and latest prices."""
    items = []
    for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
        color = REGION_COLORS.get(region, '#888')
        val = latest_prices.get(region)
        if val is not None:
            label = f'{region}: ${val:.0f}'
        else:
            label = region
        items.append(
            f'<span style="display:inline-flex;align-items:center;margin-right:10px;">'
            f'<span style="display:inline-block;width:12px;height:3px;background:{color};'
            f'margin-right:4px;border-radius:1px;"></span>'
            f'<span style="font-size:11px;color:{FLEXOKI["text"]};font-family:-apple-system,sans-serif;">'
            f'{label}</span></span>'
        )
    return f'<div style="padding:2px 0 0 4px;">{"".join(items)}</div>'


def create_price_chart_matplotlib(prices_df):
    """Create matplotlib price chart with LOESS smoothing and $1500 cap.

    Returns (fig, legend_html) — legend is a separate HTML string for
    placement above the chart so it doesn't obscure the plot area.
    """
    empty_legend = _build_price_legend_html({})
    if prices_df is None or len(prices_df) == 0:
        fig, ax = plt.subplots(figsize=(4.5, 3.2))
        ax.text(0.5, 0.5, 'Price data loading...', ha='center', va='center')
        ax.set_facecolor(FLEXOKI['background'])
        fig.patch.set_facecolor(FLEXOKI['background'])
        return fig, empty_legend

    pivot = prices_df.pivot_table(
        index='SETTLEMENTDATE', columns='REGIONID', values='RRP', aggfunc='mean'
    ).sort_index()

    rows_to_take = min(120, len(pivot))
    raw = pivot.tail(rows_to_take)

    # Apply LOESS smoothing if available
    smoothed = pd.DataFrame(index=raw.index)
    for col in raw.columns:
        y = raw[col].values
        x = np.arange(len(y))
        valid_mask = ~np.isnan(y)

        if HAS_LOESS and valid_mask.sum() > 10:
            try:
                smoothed_result = lowess(y[valid_mask], x[valid_mask], frac=0.1, it=0, return_sorted=False)
                smoothed_vals = np.full(len(y), np.nan)
                smoothed_vals[valid_mask] = smoothed_result
                smoothed[col] = smoothed_vals
            except:
                smoothed[col] = raw[col].ewm(alpha=0.22).mean()
        else:
            smoothed[col] = raw[col].ewm(alpha=0.22).mean()

    # Create plot — taller now that legend is external
    fig, ax = plt.subplots(figsize=(4.5, 3.2))
    fig.patch.set_facecolor(FLEXOKI['background'])
    ax.set_facecolor(FLEXOKI['background'])

    Y_CAP = 1500
    spike_annotations = []
    latest_prices = {}

    for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
        if region not in smoothed.columns:
            continue

        color = REGION_COLORS.get(region, '#888')
        y_vals = smoothed[region].values
        x_vals = smoothed.index

        # Find spikes above cap
        max_val = raw[region].max()
        if max_val > Y_CAP:
            max_idx = raw[region].idxmax()
            spike_annotations.append((region, max_val, max_idx))

        # Clip for display
        y_clipped = np.clip(y_vals, None, Y_CAP)

        # Get latest value for external legend
        latest_val = raw[region].iloc[-1]
        latest_prices[region] = latest_val

        ax.plot(x_vals, y_clipped, color=color, linewidth=1.5)

    # Dynamic Y-axis - scale to what's actually displayed (smoothed values)
    smoothed_max = smoothed.max().max()
    if smoothed_max > 300:
        # High price event - scale to smoothed data with 15% headroom
        y_top = min(Y_CAP, max(smoothed_max * 1.15, 500))
    else:
        # Normal prices - tight scaling
        y_top = max(smoothed_max * 1.2, 200)
    ax.set_ylim(bottom=min(0, smoothed.min().min() - 10), top=y_top)

    # Add spike annotations
    for region, val, time in spike_annotations[:2]:
        ax.annotate(
            f'${val:,.0f}',
            xy=(time, Y_CAP),
            xytext=(0, 5),
            textcoords='offset points',
            fontsize=8,
            color=REGION_COLORS.get(region, '#888'),
            ha='center',
            fontweight='bold'
        )

    # Styling
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(FLEXOKI['ui_border'])
    ax.spines['bottom'].set_color(FLEXOKI['ui_border'])

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    ax.tick_params(colors=FLEXOKI['text'], labelsize=8)

    # Title — includes $/MWh so we skip Y-axis label
    end_time = raw.index[-1]
    title = f"Smoothed 5 min $/MWh as at {end_time.strftime('%d %b %H:%M')}"
    ax.set_title(title, fontsize=10, color=FLEXOKI['foreground'], loc='left')

    # No Y-axis label — unit is in title
    # No internal legend — moved to HTML row above chart

    # Attribution
    ax.text(0.99, 0.02, '©ITK', transform=ax.transAxes,
            fontsize=7, color=FLEXOKI['muted'], ha='right', va='bottom')

    plt.tight_layout()

    # Build external HTML legend
    legend_html = _build_price_legend_html(latest_prices)

    return fig, legend_html


# =============================================================================
# PRICE TABLE
# =============================================================================

def create_price_table_html(prices_df):
    """Create HTML price table showing last 7 intervals + hour average."""
    if prices_df is None or len(prices_df) == 0:
        return f'<div style="color: {FLEXOKI["muted"]}; padding: 10px;">Price data loading...</div>'

    pivot = prices_df.pivot_table(
        index='SETTLEMENTDATE', columns='REGIONID', values='RRP', aggfunc='mean'
    ).sort_index().tail(7)

    cols = [c for c in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'] if c in pivot.columns]

    # Calculate averages
    latest_time = pivot.index[-1]
    hour_start = latest_time - timedelta(hours=1)
    hour_data = prices_df[prices_df['SETTLEMENTDATE'] >= hour_start]
    hour_avg = hour_data.groupby('REGIONID')['RRP'].mean().round(0).to_dict()

    day_start = latest_time - timedelta(hours=24)
    day_data = prices_df[prices_df['SETTLEMENTDATE'] >= day_start]
    day_avg = day_data.groupby('REGIONID')['RRP'].mean().round(0).to_dict()

    # Build HTML
    html = f"""
    <style>
        .price-table {{
            width: 100%;
            border-collapse: collapse;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            font-size: 11px;
            background: {FLEXOKI['background']};
        }}
        .price-table th {{
            background: {FLEXOKI_ACCENT['cyan']};
            color: white;
            padding: 5px 6px;
            text-align: right;
            font-weight: bold;
        }}
        .price-table th:first-child {{
            text-align: left;
        }}
        .price-table td {{
            padding: 4px 6px;
            text-align: right;
            border-bottom: 1px solid {FLEXOKI_BASE[100]};
            font-family: 'SF Mono', Consolas, monospace;
        }}
        .price-table td:first-child {{
            text-align: left;
            font-weight: bold;
            font-family: -apple-system, sans-serif;
        }}
        .price-table tr.summary td {{
            font-weight: bold;
            background: {FLEXOKI_BASE[50]};
        }}
        .price-table-caption {{
            font-size: 13px;
            font-weight: bold;
            color: {FLEXOKI['foreground']};
            padding: 0 0 8px 0;
            margin-bottom: 0;
        }}
    </style>
    <div class="price-table-caption">5 minute spot $/MWh {latest_time.strftime('%d %b %H:%M')}</div>
    <table class="price-table">
        <thead>
            <tr><th>Time</th>"""

    for col in cols:
        html += f"<th>{col}</th>"
    html += "</tr></thead><tbody>"

    # Data rows
    for idx in pivot.index:
        time_str = idx.strftime('%H:%M')
        html += f"<tr><td>{time_str}</td>"
        for col in cols:
            val = pivot.loc[idx, col]
            html += f"<td>{val:,.0f}</td>"
        html += "</tr>"

    # Summary rows
    html += f'<tr class="summary"><td>Last hour average</td>'
    for col in cols:
        val = hour_avg.get(col, 0)
        html += f"<td>{val:,.0f}</td>"
    html += "</tr>"

    html += f'<tr class="summary"><td>Last 24 hr average</td>'
    for col in cols:
        val = day_avg.get(col, 0)
        html += f"<td>{val:,.0f}</td>"
    html += "</tr>"

    html += "</tbody></table>"
    return html


# =============================================================================
# GENERATION CHART (GW, stacked area)
# =============================================================================

def create_generation_stack(gen_df, title="Generation Mix (GW)"):
    """Create Plotly stacked area chart of generation by fuel in GW."""
    if gen_df is None or len(gen_df) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="Generation data loading...",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=FLEXOKI['muted'])
        )
        fig.update_layout(
            paper_bgcolor=FLEXOKI['background'],
            plot_bgcolor=FLEXOKI['background'],
            margin=dict(l=20, r=20, t=40, b=20),
            title=dict(text=title, font=dict(size=14, color=FLEXOKI['foreground']))
        )
        return fig

    pivot = gen_df.pivot_table(
        index='SETTLEMENTDATE', columns='FUEL_TYPE', values='MW', fill_value=0
    )

    # Convert MW to GW
    pivot = pivot / 1000

    fuel_order = ['Coal', 'Wind', 'Solar', 'Rooftop Solar', 'Battery', 'Hydro', 'Gas', 'Other']
    available_fuels = [f for f in fuel_order if f in pivot.columns]
    for f in pivot.columns:
        if f not in available_fuels:
            available_fuels.append(f)

    fig = go.Figure()
    for fuel in available_fuels:
        if fuel in pivot.columns:
            fig.add_trace(go.Scatter(
                x=pivot.index,
                y=pivot[fuel],
                name=fuel,
                stackgroup='generation',
                fillcolor=FUEL_COLORS.get(fuel, '#888'),
                line=dict(width=0.5, color='white'),
                hovertemplate=f'{fuel}: %{{y:.1f}} GW<extra></extra>'
            ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color=FLEXOKI['foreground'])),
        paper_bgcolor=FLEXOKI['background'],
        plot_bgcolor=FLEXOKI['background'],
        font=dict(color=FLEXOKI['text']),
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(orientation='v', yanchor='top', y=1, xanchor='left', x=1.02, font=dict(size=9)),
        xaxis=dict(showgrid=False, showline=False, tickformat='%H:%M\n%d %b'),
        yaxis=dict(showgrid=True, gridcolor=FLEXOKI['ui'], showline=False, tickformat='.0f'),
        hovermode='x unified',
    )
    return fig


# =============================================================================
# RENEWABLE GAUGE (stacked)
# =============================================================================

def create_renewable_gauge_stacked(gen_df):
    """Create renewable energy gauge with stacked fuel breakdown."""
    stats = {'renewable_pct': 0, 'hydro_pct': 0, 'wind_pct': 0, 'solar_pct': 0, 'rooftop_pct': 0}

    if gen_df is not None and len(gen_df) > 0 and 'SETTLEMENTDATE' in gen_df.columns:
        latest = gen_df[gen_df['SETTLEMENTDATE'] == gen_df['SETTLEMENTDATE'].max()]
        total_gen = latest['MW'].sum()

        if total_gen > 0:
            fuel_totals = latest.groupby('FUEL_TYPE')['MW'].sum()
            stats['hydro_pct'] = fuel_totals.get('Hydro', 0) / total_gen * 100
            stats['wind_pct'] = fuel_totals.get('Wind', 0) / total_gen * 100
            stats['solar_pct'] = fuel_totals.get('Solar', 0) / total_gen * 100
            stats['rooftop_pct'] = fuel_totals.get('Rooftop Solar', 0) / total_gen * 100
            stats['renewable_pct'] = stats['hydro_pct'] + stats['wind_pct'] + stats['solar_pct'] + stats['rooftop_pct']

    # Build gauge steps
    steps = []
    cum_hydro = stats['hydro_pct']
    cum_wind = cum_hydro + stats['wind_pct']
    cum_solar = cum_wind + stats['solar_pct']
    cum_total = cum_solar + stats['rooftop_pct']

    if stats['hydro_pct'] > 0:
        steps.append({'range': [0, cum_hydro], 'color': GAUGE_FUEL_COLORS['hydro']})
    if stats['wind_pct'] > 0:
        steps.append({'range': [cum_hydro, cum_wind], 'color': GAUGE_FUEL_COLORS['wind']})
    if stats['solar_pct'] > 0:
        steps.append({'range': [cum_wind, cum_solar], 'color': GAUGE_FUEL_COLORS['solar']})
    if stats['rooftop_pct'] > 0:
        steps.append({'range': [cum_solar, cum_total], 'color': GAUGE_FUEL_COLORS['rooftop']})

    # Add grey for non-renewable
    if cum_total < 100:
        steps.append({'range': [cum_total, 100], 'color': FLEXOKI_BASE[200]})

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=stats['renewable_pct'],
        number={'suffix': '%', 'font': {'size': 36, 'color': FLEXOKI['foreground']}, 'valueformat': '.0f'},
        title={'text': '<b>Renewable Energy %</b>', 'font': {'size': 14, 'color': FLEXOKI['foreground']}},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': FLEXOKI['ui_border'],
                    'tickfont': {'size': 10, 'color': FLEXOKI['muted']}},
            'bar': {'color': 'rgba(0,0,0,0)', 'thickness': 0},
            'bgcolor': FLEXOKI_BASE[100],
            'borderwidth': 0,
            'steps': steps,
            'threshold': {
                'line': {'color': FLEXOKI['foreground'], 'width': 3},
                'thickness': 0.85,
                'value': stats['renewable_pct']
            }
        }
    ))

    fig.update_layout(
        paper_bgcolor=FLEXOKI['background'],
        font={'color': FLEXOKI['foreground']},
        margin=dict(l=20, r=20, t=50, b=20),
        height=200,
        width=400,
    )

    # Create legend HTML
    legend_html = f"""
    <div style="display: flex; justify-content: center; gap: 15px; font-size: 10px; color: {FLEXOKI['text']}; padding: 5px 0;">
        <span><span style="display: inline-block; width: 14px; height: 10px; background: {GAUGE_FUEL_COLORS['hydro']};"></span>
        Hydro {stats['hydro_pct']:.0f}%</span>
        <span><span style="display: inline-block; width: 14px; height: 10px; background: {GAUGE_FUEL_COLORS['wind']};"></span>
        Wind {stats['wind_pct']:.0f}%</span>
        <span><span style="display: inline-block; width: 14px; height: 10px; background: {GAUGE_FUEL_COLORS['solar']};"></span>
        Solar {stats['solar_pct']:.0f}%</span>
        <span><span style="display: inline-block; width: 14px; height: 10px; background: {GAUGE_FUEL_COLORS['rooftop']};"></span>
        Rooftop {stats['rooftop_pct']:.0f}%</span>
    </div>
    """

    return fig, legend_html


# =============================================================================
# GENERATOR OUTAGES (from PASA tab, displayed on Today tab)
# =============================================================================

def load_outages_panel():
    """Build the generator outages summary for the Today tab.

    Reuses the PASA tab's create_generator_fuel_summary() with a clickable
    title that links to the full PASA tab (tab index 7).
    """
    try:
        detector = ChangeDetector(data_path=PASA_DATA_PATH)
        gen_info = load_gen_info()
        outage_col = create_generator_fuel_summary(detector, gen_info)

        # Replace the PASA component's own header with a clickable version
        # The first child of outage_col is the section_header pane
        if len(outage_col) > 0:
            # Extract the original header text to get the MW total
            original_header = outage_col[0]
            # Build a clickable replacement that links to PASA tab (index 7)
            outage_col[0] = pn.pane.HTML(f"""
            <a href="#" onclick="
                var tabs = document.querySelectorAll('.bk-tab');
                if (tabs && tabs.length > 7) {{ tabs[7].click(); }}
                return false;
            " style="text-decoration: none; cursor: pointer;">
                <h3 style="margin: 8px 0 6px 0; padding-bottom: 6px;
                    border-bottom: 1px solid {FLEXOKI['ui_border']};
                    color: {FLEXOKI['foreground']}; font-size: 15px; font-weight: 600;">
                    Generator Outages
                    <span style="font-size: 11px; font-weight: normal; color: {FLEXOKI['muted']};">
                        &nbsp;→ full PASA tab
                    </span>
                </h3>
            </a>
            """, sizing_mode='stretch_width')

        return outage_col

    except Exception as e:
        logger.error(f"Error loading outages for Today tab: {e}")
        return pn.pane.HTML(
            f'<div style="color: {FLEXOKI["muted"]}; font-style: italic; padding: 5px;">Outage data unavailable</div>',
            sizing_mode='stretch_width',
        )


# =============================================================================
# KEY EVENTS (commented out — replaced by generator outages panel above)
# =============================================================================

def create_key_events(prices_df, gen_df):
    """Generate key events from data analysis."""
    events = []

    if prices_df is None or len(prices_df) == 0:
        return events

    latest_time = prices_df['SETTLEMENTDATE'].max()
    day_start = latest_time - timedelta(hours=24)
    day_prices = prices_df[prices_df['SETTLEMENTDATE'] >= day_start]

    for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
        region_prices = day_prices[day_prices['REGIONID'] == region]
        if len(region_prices) == 0:
            continue

        max_price = region_prices['RRP'].max()
        min_price = region_prices['RRP'].min()

        if max_price > 300:
            max_idx = region_prices['RRP'].idxmax()
            max_time = region_prices.loc[max_idx, 'SETTLEMENTDATE']
            events.append({
                'type': 'spike',
                'color': FLEXOKI_ACCENT['red'],
                'text': f"{region.replace('1', '')} peaked at ${max_price:,.0f} at {max_time.strftime('%H:%M')}"
            })

        if min_price < -10:
            min_idx = region_prices['RRP'].idxmin()
            min_time = region_prices.loc[min_idx, 'SETTLEMENTDATE']
            events.append({
                'type': 'negative',
                'color': FLEXOKI_ACCENT['cyan'],
                'text': f"{region.replace('1', '')} went negative ${min_price:,.0f} at {min_time.strftime('%H:%M')}"
            })

    return events[:6]


# =============================================================================
# MAIN TAB CREATION
# =============================================================================

def _build_tab_content():
    """Build the actual tab content - called after loading indicator is shown.

    Returns (layout, updatable_panes) where updatable_panes is a dict of
    pane references that can be refreshed by _refresh_today_tab().
    """
    import time
    timings = {}
    total_start = time.time()

    logger.info("Building Today tab content...")

    # Load data
    t0 = time.time()
    prices_df, prices_end = load_price_data(hours=24)
    timings['load_prices'] = time.time() - t0
    logger.info(f"Loaded prices to {prices_end} ({timings['load_prices']:.2f}s)")

    t0 = time.time()
    gen_df, gen_end = load_generation_data(hours=24)
    timings['load_generation'] = time.time() - t0
    logger.info(f"Loaded generation to {gen_end} ({timings['load_generation']:.2f}s)")

    # Fetch forecast and notices
    t0 = time.time()
    predispatch_df, pd_run_time = fetch_predispatch_forecasts()
    timings['fetch_forecast'] = time.time() - t0
    logger.info(f"Fetched forecast ({timings['fetch_forecast']:.2f}s)")

    t0 = time.time()
    notices = fetch_market_notices(limit=10)
    timings['fetch_notices'] = time.time() - t0
    logger.info(f"Fetched notices ({timings['fetch_notices']:.2f}s)")

    # Load demand data
    t0 = time.time()
    demand_stats = load_demand_data()
    timings['load_demand'] = time.time() - t0

    t0 = time.time()
    batt_stats = load_battery_stored()
    timings['load_battery'] = time.time() - t0

    # Extract forecast peak demand from predispatch data
    forecast_peak_mw = None
    if predispatch_df is not None and len(predispatch_df) > 0:
        try:
            demand_col = None
            for col in ['DEMAND_FORECAST', 'demand_forecast']:
                if col in predispatch_df.columns:
                    demand_col = col
                    break
            if demand_col:
                ts_col = 'SETTLEMENTDATE' if 'SETTLEMENTDATE' in predispatch_df.columns else 'settlementdate'
                forecast_peak_mw = predispatch_df.groupby(ts_col)[demand_col].sum().max()
        except Exception as e:
            logger.warning(f"Could not extract forecast peak demand: {e}")

    # # Create key events (commented out — replaced by generator outages)
    # t0 = time.time()
    # events = create_key_events(prices_df, gen_df)
    # timings['create_events'] = time.time() - t0

    # === LEFT COLUMN: Past 24 Hours ===
    t0 = time.time()
    gen_chart = pn.pane.Plotly(
        create_generation_stack(gen_df),
        height=280,
        sizing_mode='stretch_width'
    )
    timings['gen_chart'] = time.time() - t0

    # # Events panel (commented out — replaced by generator outages)
    # events_html = f'<div style="font-size: 12px;"><h4 style="margin: 5px 0 8px 0; color: {FLEXOKI["foreground"]};">Key Events (24h)</h4>'
    # for event in events:
    #     events_html += f'<div style="margin: 4px 0; padding: 5px 8px; border-left: 3px solid {event["color"]}; background: {FLEXOKI["ui"]}; font-size: 11px;">{event["text"]}</div>'
    # if not events:
    #     events_html += f'<div style="color: {FLEXOKI["muted"]};">No significant events</div>'
    # events_html += "</div>"
    # events_panel = pn.pane.HTML(events_html, sizing_mode='stretch_width')

    # Generator outages panel (from PASA data)
    t0 = time.time()
    outages_content = load_outages_panel()
    # Wrap in Column for consistent refresh (slice assignment)
    if isinstance(outages_content, pn.Column):
        outages_panel = outages_content
    else:
        outages_panel = pn.Column(outages_content, sizing_mode='stretch_width')
    timings['outages'] = time.time() - t0

    # Gauge with legend
    t0 = time.time()
    gauge_fig, gauge_legend_html = create_renewable_gauge_stacked(gen_df)
    gauge_plotly = pn.pane.Plotly(gauge_fig, sizing_mode='fixed', width=400, height=200)
    gauge_legend = pn.pane.HTML(gauge_legend_html, width=400)
    gauge = pn.Column(
        gauge_plotly,
        gauge_legend,
        sizing_mode='fixed', width=400
    )
    timings['gauge'] = time.time() - t0

    # Battery stored energy gauge
    t0 = time.time()
    batt_fig = create_battery_gauge(batt_stats)
    battery_gauge = pn.pane.Matplotlib(batt_fig, sizing_mode='fixed', width=480, height=160)
    plt.close(batt_fig)
    timings['battery_gauge'] = time.time() - t0

    # === CENTER COLUMN: Prices Now ===
    t0 = time.time()
    price_fig, price_legend_html = create_price_chart_matplotlib(prices_df)
    price_legend = pn.pane.HTML(price_legend_html, sizing_mode='stretch_width')
    price_chart = pn.pane.Matplotlib(price_fig, sizing_mode='fixed', width=420, height=300)
    plt.close(price_fig)
    timings['price_chart'] = time.time() - t0

    t0 = time.time()
    price_table = pn.pane.HTML(
        create_price_table_html(prices_df),
        sizing_mode='stretch_width'
    )
    timings['price_table'] = time.time() - t0

    # Demand gauge
    t0 = time.time()
    demand_fig, demand_legend_html = create_demand_gauge(demand_stats, forecast_peak_mw)
    demand_gauge_pane = pn.pane.Matplotlib(demand_fig, sizing_mode='fixed', width=420, height=220)
    plt.close(demand_fig)
    demand_gauge_legend = pn.pane.HTML(demand_legend_html, width=420)
    timings['demand_gauge'] = time.time() - t0

    # === RIGHT COLUMN: Looking Ahead ===
    t0 = time.time()
    forecast_table = pn.pane.HTML(
        create_forecast_table(predispatch_df, pd_run_time),
        sizing_mode='stretch_width'
    )
    timings['forecast_table'] = time.time() - t0

    notices_html = f'<h4 style="margin: 5px 0 8px 0; color: {FLEXOKI["foreground"]};">Key Market Notices</h4>'
    notices_panel = pn.Column(
        pn.pane.HTML(notices_html),
        create_notices_panel(notices),
        sizing_mode='stretch_width'
    )

    # Section headers
    def section_header(text):
        return pn.pane.HTML(
            f'<h3 style="margin: 0; padding: 8px 0 5px 0; color: {FLEXOKI["foreground"]}; '
            f'border-bottom: 1px solid {FLEXOKI["ui_border"]};">{text}</h3>',
            sizing_mode='stretch_width'
        )

    # === BUILD LAYOUT (3 rows: tables, charts, gauges) ===

    # ROW 1: Tables & text (price table, forecast, notices)
    row1_left = pn.Column(
        price_table,
        width=400, sizing_mode='fixed',
    )
    row1_center = pn.Column(
        forecast_table,
        width=420, sizing_mode='fixed',
    )
    row1_right = pn.Column(
        notices_panel,
        width=380, sizing_mode='fixed',
    )
    row1 = pn.Row(
        row1_left, pn.Spacer(width=10),
        row1_center, pn.Spacer(width=10),
        row1_right,
        sizing_mode='fixed',
    )

    # ROW 2: Charts (price chart with legend, generation mix, outages)
    row2_left = pn.Column(
        price_legend,
        price_chart,
        width=420, sizing_mode='fixed',
    )
    row2_center = pn.Column(
        gen_chart,
        width=450, sizing_mode='fixed',
    )
    row2_right = pn.Column(
        outages_panel,
        width=380, sizing_mode='fixed',
    )
    row2 = pn.Row(
        row2_left, pn.Spacer(width=10),
        row2_center, pn.Spacer(width=10),
        row2_right,
        sizing_mode='fixed',
    )

    # ROW 3: Gauges (demand, renewable, battery)
    row3_left = pn.Column(
        demand_gauge_pane,
        demand_gauge_legend,
        width=400, sizing_mode='fixed',
    )
    row3_center = pn.Column(
        gauge,
        width=400, sizing_mode='fixed',
    )
    row3_right = pn.Column(
        battery_gauge,
        width=420, sizing_mode='fixed',
    )
    row3 = pn.Row(
        row3_left, pn.Spacer(width=10),
        row3_center, pn.Spacer(width=10),
        row3_right,
        sizing_mode='fixed',
    )

    # Stack the three rows
    layout = pn.Column(
        row1,
        pn.Spacer(height=10),
        row2,
        pn.Spacer(height=10),
        row3,
        sizing_mode='stretch_width',
        styles={'background': FLEXOKI['background'], 'padding': '10px 15px 15px 15px'},
    )

    # Log timing summary
    total_time = time.time() - total_start
    timings['total'] = total_time
    timing_str = ', '.join(f"{k}={v:.2f}s" for k, v in timings.items())
    logger.info(f"Today tab timings: {timing_str}")

    # Collect updatable pane references for periodic refresh
    updatable_panes = {
        'gen_chart': gen_chart,
        'outages_panel': outages_panel,
        'gauge_plotly': gauge_plotly,
        'gauge_legend': gauge_legend,
        'price_chart': price_chart,
        'price_legend': price_legend,
        'price_table': price_table,
        'demand_gauge_pane': demand_gauge_pane,
        'demand_gauge_legend': demand_gauge_legend,
        'battery_gauge': battery_gauge,
        'forecast_table': forecast_table,
        'notices_panel': notices_panel,
    }

    return layout, updatable_panes


def _refresh_today_tab(panes):
    """Refresh all data-driven panes in the Today tab.

    Called every 4.5 minutes by periodic callback.
    Updates panes in-place so the layout doesn't need to be rebuilt.
    """
    import time
    t0 = time.time()

    try:
        # Reload data
        prices_df, prices_end = load_price_data(hours=24)
        gen_df, gen_end = load_generation_data(hours=24)
        predispatch_df, pd_run_time = fetch_predispatch_forecasts()
        demand_stats = load_demand_data()
        batt_stats = load_battery_stored()
        notices = fetch_market_notices(limit=10)
        # events = create_key_events(prices_df, gen_df)  # commented out — replaced by outages

        # Extract forecast peak demand
        forecast_peak_mw = None
        if predispatch_df is not None and len(predispatch_df) > 0:
            try:
                demand_col = None
                for col in ['DEMAND_FORECAST', 'demand_forecast']:
                    if col in predispatch_df.columns:
                        demand_col = col
                        break
                if demand_col:
                    ts_col = 'SETTLEMENTDATE' if 'SETTLEMENTDATE' in predispatch_df.columns else 'settlementdate'
                    forecast_peak_mw = predispatch_df.groupby(ts_col)[demand_col].sum().max()
            except Exception:
                pass

        # Update generation chart
        panes['gen_chart'].object = create_generation_stack(gen_df)

        # Update generator outages panel
        new_outages = load_outages_panel()
        if isinstance(new_outages, pn.Column):
            panes['outages_panel'][:] = list(new_outages)
        else:
            panes['outages_panel'][:] = [new_outages]

        # Update gauge
        gauge_fig, gauge_legend_html = create_renewable_gauge_stacked(gen_df)
        panes['gauge_plotly'].object = gauge_fig
        panes['gauge_legend'].object = gauge_legend_html

        # Update price chart + legend (matplotlib - close old figure to prevent memory leak)
        new_price_fig, new_price_legend_html = create_price_chart_matplotlib(prices_df)
        panes['price_chart'].object = new_price_fig
        panes['price_legend'].object = new_price_legend_html
        plt.close(new_price_fig)

        # Update price table
        panes['price_table'].object = create_price_table_html(prices_df)

        # Update demand gauge (matplotlib — close old figure to prevent memory leak)
        demand_fig, demand_legend_html = create_demand_gauge(demand_stats, forecast_peak_mw)
        panes['demand_gauge_pane'].object = demand_fig
        panes['demand_gauge_legend'].object = demand_legend_html
        plt.close(demand_fig)

        # Update battery gauge
        new_batt_fig = create_battery_gauge(batt_stats)
        panes['battery_gauge'].object = new_batt_fig
        plt.close(new_batt_fig)

        # Update forecast
        panes['forecast_table'].object = create_forecast_table(predispatch_df, pd_run_time)

        # Update notices
        notices_html_header = f'<h4 style="margin: 5px 0 8px 0; color: {FLEXOKI["foreground"]};">Key Market Notices</h4>'
        panes['notices_panel'][:] = [
            pn.pane.HTML(notices_html_header),
            create_notices_panel(notices),
        ]

        elapsed = time.time() - t0
        logger.info(f"Today tab refreshed (prices to {prices_end}, gen to {gen_end}, {elapsed:.1f}s)")

    except Exception as e:
        logger.error(f"Error refreshing Today tab: {e}")
        # Don't crash - the tab will just show stale data until next refresh


def create_nem_dash_tab(dashboard_instance=None):
    """
    Create the Today tab with immediate loading indicator and auto-refresh.

    Shows a loading spinner immediately, then builds content after page loads.
    Registers a periodic callback (every 4.5 min) to refresh all data panes.
    """
    logger.info("Creating Today tab (NEM at a Glance)")

    # Create loading indicator HTML
    loading_html = f"""
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
                height:500px;color:{FLEXOKI['muted']};background:{FLEXOKI['background']};">
        <div style="width:50px;height:50px;border:4px solid {FLEXOKI['ui_border']};
                    border-top-color:{FLEXOKI_ACCENT['cyan']};border-radius:50%;
                    animation:spin 1s linear infinite;"></div>
        <p style="margin-top:20px;font-size:14px;">Loading NEM Dashboard...</p>
    </div>
    <style>@keyframes spin {{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>
    """

    # Create container that starts with loading indicator
    loading_pane = pn.pane.HTML(loading_html, sizing_mode='stretch_width')

    container = pn.Column(
        loading_pane,
        sizing_mode='stretch_width',
        styles={'background': FLEXOKI['background'], 'min-height': '600px'},
        name="Today"
    )

    # Schedule content loading to run AFTER page is served to browser
    def load_content():
        """Called after page loads in browser via websocket callback."""
        logger.info("onload callback triggered - building Today tab content")
        try:
            content, updatable_panes = _build_tab_content()
            container[:] = [content]  # Replace loading indicator with actual content
            logger.info("Today tab content loaded successfully")

            # Register periodic refresh every 4.5 minutes (270,000ms)
            pn.state.add_periodic_callback(
                lambda: _refresh_today_tab(updatable_panes),
                period=270000,
            )
            logger.info("Today tab auto-refresh registered (4.5 min interval)")

        except Exception as e:
            logger.error(f"Error building Today tab: {e}")
            container[:] = [pn.pane.HTML(
                f"<div style='padding:20px;text-align:center;color:{FLEXOKI['foreground']};'>"
                f"<h2>Error Loading</h2><p>{e}</p></div>"
            )]

    # pn.state.onload runs callback after page is served and websocket connects
    pn.state.onload(load_content)

    return container


# Alias for backward compatibility
def create_nem_dash_tab_with_updates(dashboard_instance=None, auto_update=True):
    """Create Today tab with auto-refresh enabled."""
    return create_nem_dash_tab(dashboard_instance)


if __name__ == "__main__":
    pn.extension(['bokeh', 'plotly'])
    tab = create_nem_dash_tab()
    pn.serve(tab, port=5555, show=True)
