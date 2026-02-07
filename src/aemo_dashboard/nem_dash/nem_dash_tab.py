"""
Today Tab - NEM at a Glance
===========================
Completely redesigned Today tab based on mockup V4.

Layout: 3 columns
- Left: Past 24 Hours (Generation mix, Key Events, Renewable Gauge)
- Center: Prices Now (Price chart with LOESS, Price table)
- Right: Looking Ahead (Forecast table, Market Notices)
"""

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
    """Load recent price data from parquet."""
    try:
        prices_path = DATA_PATH / 'prices5.parquet'
        prices_df = pd.read_parquet(prices_path)

        # Handle column name variations
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


# =============================================================================
# PRICE CHART (LOESS smoothing, $1500 cap)
# =============================================================================

def create_price_chart_matplotlib(prices_df):
    """Create matplotlib price chart with LOESS smoothing and $1500 cap."""
    if prices_df is None or len(prices_df) == 0:
        fig, ax = plt.subplots(figsize=(4.2, 3))
        ax.text(0.5, 0.5, 'Price data loading...', ha='center', va='center')
        ax.set_facecolor(FLEXOKI['background'])
        fig.patch.set_facecolor(FLEXOKI['background'])
        return fig

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

    # Create plot
    fig, ax = plt.subplots(figsize=(4.2, 3))
    fig.patch.set_facecolor(FLEXOKI['background'])
    ax.set_facecolor(FLEXOKI['background'])

    Y_CAP = 1500
    spike_annotations = []

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

        # Get latest value for legend
        latest_val = raw[region].iloc[-1]
        label = f"{region}: ${latest_val:.0f}"

        ax.plot(x_vals, y_clipped, color=color, linewidth=1.5, label=label)

    ax.set_ylim(bottom=min(0, raw.min().min() - 10), top=Y_CAP)

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

    ax.set_ylabel('$/MWh', fontsize=9, color=FLEXOKI['text'])

    # Title
    end_time = raw.index[-1]
    title = f"Smoothed 5 minute prices as at {end_time.strftime('%d %b %H:%M')} ({len(raw)} points)"
    ax.set_title(title, fontsize=10, color=FLEXOKI['foreground'], loc='left')

    # Legend
    ax.legend(loc='upper right', fontsize=7, framealpha=0.9,
              facecolor=FLEXOKI['background'], edgecolor=FLEXOKI['ui_border'])

    # Attribution
    ax.text(0.99, 0.02, '©ITK', transform=ax.transAxes,
            fontsize=7, color=FLEXOKI['muted'], ha='right', va='bottom')

    plt.tight_layout()
    return fig


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
            font-size: 12px;
            font-weight: bold;
            color: white;
            background: {FLEXOKI_ACCENT['cyan']};
            padding: 6px 8px;
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
        title={'text': 'Renewable Energy %', 'font': {'size': 14, 'color': FLEXOKI['foreground']}},
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
# KEY EVENTS
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

def create_nem_dash_tab(dashboard_instance=None):
    """Create the Today tab with 3-column layout matching mockup V4."""
    try:
        logger.info("Creating Today tab (NEM at a Glance)")

        # Load data
        prices_df, prices_end = load_price_data(hours=24)
        logger.info(f"Loaded prices to {prices_end}")

        gen_df, gen_end = load_generation_data(hours=24)
        logger.info(f"Loaded generation to {gen_end}")

        # Fetch forecast and notices
        logger.info("Fetching pre-dispatch forecast...")
        predispatch_df, pd_run_time = fetch_predispatch_forecasts()

        logger.info("Fetching market notices...")
        notices = fetch_market_notices(limit=10)

        # Create key events
        events = create_key_events(prices_df, gen_df)

        logger.info("Creating components...")

        # === LEFT COLUMN: Past 24 Hours ===
        gen_chart = pn.pane.Plotly(
            create_generation_stack(gen_df),
            height=280,
            sizing_mode='stretch_width'
        )

        # Events panel
        events_html = f'<div style="font-size: 12px;"><h4 style="margin: 5px 0 8px 0; color: {FLEXOKI["foreground"]};">Key Events (24h)</h4>'
        for event in events:
            events_html += f'<div style="margin: 4px 0; padding: 5px 8px; border-left: 3px solid {event["color"]}; background: {FLEXOKI["ui"]}; font-size: 11px;">{event["text"]}</div>'
        if not events:
            events_html += f'<div style="color: {FLEXOKI["muted"]};">No significant events</div>'
        events_html += "</div>"
        events_panel = pn.pane.HTML(events_html, sizing_mode='stretch_width')

        # Gauge with legend
        gauge_fig, gauge_legend_html = create_renewable_gauge_stacked(gen_df)
        gauge = pn.Column(
            pn.pane.Plotly(gauge_fig, sizing_mode='fixed', width=400, height=200),
            pn.pane.HTML(gauge_legend_html, width=400),
            sizing_mode='fixed', width=400
        )

        # === CENTER COLUMN: Prices Now ===
        price_fig = create_price_chart_matplotlib(prices_df)
        price_chart = pn.pane.Matplotlib(price_fig, sizing_mode='fixed', width=420, height=300)
        plt.close(price_fig)

        price_table = pn.pane.HTML(
            create_price_table_html(prices_df),
            sizing_mode='stretch_width'
        )

        # === RIGHT COLUMN: Looking Ahead ===
        forecast_table = pn.pane.HTML(
            create_forecast_table(predispatch_df, pd_run_time),
            sizing_mode='stretch_width'
        )

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

        # === BUILD LAYOUT ===
        left_col = pn.Column(
            section_header("Past 24 Hours"),
            gen_chart,
            events_panel,
            pn.Spacer(height=10),
            gauge,
            width=500, sizing_mode='fixed',
        )

        center_col = pn.Column(
            section_header("Prices Now"),
            price_chart,
            price_table,
            width=440, sizing_mode='fixed',
        )

        right_col = pn.Column(
            section_header("Looking Ahead"),
            forecast_table,
            pn.Spacer(height=15),
            notices_panel,
            width=350, sizing_mode='fixed',
        )

        main = pn.Row(
            left_col,
            pn.Spacer(width=15),
            center_col,
            pn.Spacer(width=15),
            right_col,
            sizing_mode='fixed'
        )

        # Wrap in container with background
        layout = pn.Column(
            main,
            sizing_mode='stretch_width',
            styles={'background': FLEXOKI['background'], 'padding': '10px 15px 15px 15px'},
            name="Today"
        )

        logger.info("Today tab created successfully")
        return layout

    except Exception as e:
        logger.error(f"Error creating Today tab: {e}")
        import traceback
        traceback.print_exc()
        return pn.Column(
            pn.pane.HTML(
                f"<div style='padding:20px;text-align:center;'>"
                f"<h2>Error Loading Today Tab</h2>"
                f"<p>Error: {e}</p>"
                f"<p>Please check the logs and try refreshing.</p>"
                f"</div>"
            ),
            name="Today",
            sizing_mode='stretch_width',
            height=600
        )


# Alias for backward compatibility
def create_nem_dash_tab_with_updates(dashboard_instance=None, auto_update=True):
    """Create Today tab - auto_update not yet implemented for new layout."""
    return create_nem_dash_tab(dashboard_instance)


if __name__ == "__main__":
    pn.extension(['bokeh', 'plotly'])
    tab = create_nem_dash_tab()
    pn.serve(tab, port=5555, show=True)
