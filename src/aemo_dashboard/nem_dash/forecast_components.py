"""
Forecast Components for Today Tab
=================================
P30 pre-dispatch price/demand/renewable forecasts with comparison to previous run.

Components:
- fetch_predispatch_forecasts(): Load latest P30 data from predispatch.parquet
  (collected continuously by unified_collector.py)
- create_forecast_table(): HTML table with 6-hour forecast + 24hr average
- Forecast caching for run-to-run comparison
"""

import pandas as pd
import panel as pn
import json
from datetime import datetime, timedelta
from pathlib import Path

from ..shared.logging_config import get_logger
from ..shared.config import Config
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT
)

logger = get_logger(__name__)

# Get data directory from config
config = Config()
DATA_PATH = config.data_dir
FORECAST_CACHE_PATH = DATA_PATH / 'forecast_cache.json'


def fetch_predispatch_forecasts():
    """
    Load pre-dispatch price/demand/renewable forecasts from predispatch.parquet.

    The parquet file is updated continuously by unified_collector.py every 4.5 minutes.

    Returns:
        tuple: (DataFrame with forecasts, run_time datetime) or (None, None) on error
    """
    try:
        parquet_path = DATA_PATH / 'predispatch.parquet'

        if not parquet_path.exists():
            logger.warning(f"Predispatch parquet not found: {parquet_path}")
            return None, None

        # Read the parquet file
        df = pd.read_parquet(parquet_path)

        if df.empty:
            logger.warning("Predispatch parquet is empty")
            return None, None

        # Get the latest run_time
        latest_run_time = df['run_time'].max()

        # Filter to latest run only
        df = df[df['run_time'] == latest_run_time].copy()

        # Convert run_time to datetime if it's a timestamp
        if hasattr(latest_run_time, 'to_pydatetime'):
            run_time = latest_run_time.to_pydatetime()
        else:
            run_time = pd.to_datetime(latest_run_time).to_pydatetime()

        # Rename columns to uppercase to match dashboard expectations
        rename_map = {
            'regionid': 'REGIONID',
            'settlementdate': 'SETTLEMENTDATE',
            'price_forecast': 'PRICE_FORECAST',
            'demand_forecast': 'DEMAND_FORECAST',
            'solar_forecast': 'SOLAR_FORECAST',
            'wind_forecast': 'WIND_FORECAST'
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # Ensure SETTLEMENTDATE is datetime
        if 'SETTLEMENTDATE' in df.columns:
            df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])

        # Select output columns (matching original function's output)
        output_cols = ['REGIONID', 'SETTLEMENTDATE', 'PRICE_FORECAST', 'DEMAND_FORECAST']
        if 'SOLAR_FORECAST' in df.columns:
            output_cols.append('SOLAR_FORECAST')
        if 'WIND_FORECAST' in df.columns:
            output_cols.append('WIND_FORECAST')

        df = df[output_cols].copy()

        logger.info(f"Loaded P30 forecast run {run_time.strftime('%H:%M')} with {len(df)} rows from parquet")
        return df, run_time

    except Exception as e:
        logger.error(f"Error loading predispatch from parquet: {e}")
        return None, None


def load_previous_forecast():
    """Load the previous forecast from cache."""
    try:
        if FORECAST_CACHE_PATH.exists():
            with open(FORECAST_CACHE_PATH, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Error loading forecast cache: {e}")
    return None


def save_current_forecast(summary):
    """Save current forecast summary for next comparison."""
    try:
        with open(FORECAST_CACHE_PATH, 'w') as f:
            json.dump(summary, f)
        logger.debug("Saved forecast cache")
    except Exception as e:
        logger.error(f"Error saving forecast cache: {e}")


def create_forecast_table(predispatch_df, run_time=None):
    """
    Create HTML table of forecast prices for next 6 hours + 24hr average.

    Args:
        predispatch_df: DataFrame from fetch_predispatch_forecasts()
        run_time: datetime of P30 run

    Returns:
        str: HTML string for the forecast table with summary
    """
    if predispatch_df is None or len(predispatch_df) == 0:
        return f'<div style="color: {FLEXOKI_BASE[600]}; padding: 20px; text-align: center;">Pre-dispatch data unavailable</div>'

    pivot = predispatch_df.pivot_table(
        index='SETTLEMENTDATE', columns='REGIONID', values='PRICE_FORECAST', aggfunc='mean'
    ).sort_index()

    # Get current time and find next 6 hourly periods
    now = datetime.now()
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    # Find hourly forecasts (on the hour or close to it)
    hourly_times = []
    for i in range(1, 25):  # Look ahead up to 24 hours
        target_hour = current_hour + timedelta(hours=i)
        # Find closest forecast time to this hour
        closest_idx = pivot.index.get_indexer([target_hour], method='nearest')[0]
        if closest_idx >= 0 and closest_idx < len(pivot):
            forecast_time = pivot.index[closest_idx]
            # Only include if within 20 minutes of the target hour
            if abs((forecast_time - target_hour).total_seconds()) < 1200:
                if forecast_time not in hourly_times:
                    hourly_times.append(forecast_time)
        if len(hourly_times) >= 6:
            break

    cols = [c for c in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'] if c in pivot.columns]

    # Calculate 24hr average
    avg_24h = pivot[cols].mean().round(0).astype(int)

    # Find peak values for each region to highlight
    peak_times = {}
    for col in cols:
        if len(hourly_times) > 0:
            hourly_vals = pivot.loc[hourly_times, col]
            peak_times[col] = hourly_vals.idxmax()

    # Build HTML table
    run_str = f" (P30: {run_time.strftime('%H:%M')}, source: AEMO)" if run_time else " (source: AEMO)"

    html = f"""
    <style>
        .forecast-table {{
            width: 100%;
            border-collapse: collapse;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            font-size: 12px;
            background: {FLEXOKI_PAPER};
        }}
        .forecast-table th {{
            background: {FLEXOKI_ACCENT['cyan']};
            color: white;
            padding: 6px 8px;
            text-align: right;
            font-weight: bold;
        }}
        .forecast-table th:first-child {{
            text-align: left;
        }}
        .forecast-table td {{
            padding: 5px 8px;
            text-align: right;
            border-bottom: 1px solid {FLEXOKI_BASE[100]};
            font-family: 'SF Mono', Consolas, monospace;
        }}
        .forecast-table td:first-child {{
            text-align: left;
            font-weight: bold;
            font-family: -apple-system, sans-serif;
        }}
        .forecast-table tr:last-child td {{
            border-bottom: 2px solid {FLEXOKI_BASE[300]};
        }}
        .forecast-table tr.summary td {{
            font-weight: bold;
            background: {FLEXOKI_BASE[50]};
        }}
        .forecast-table .high {{
            color: {FLEXOKI_ACCENT['orange']};
            font-weight: bold;
        }}
        .forecast-table .extreme {{
            color: {FLEXOKI_ACCENT['red']};
            font-weight: bold;
        }}
        .forecast-table .peak {{
            text-decoration: underline;
        }}
        .forecast-caption {{
            font-size: 13px;
            font-weight: bold;
            color: {FLEXOKI_BLACK};
            margin-bottom: 8px;
        }}
    </style>
    <div class="forecast-caption">Price Forecast $/MWh{run_str}</div>
    <table class="forecast-table">
        <thead>
            <tr>
                <th>Time</th>
    """

    # Header row
    for col in cols:
        html += f"<th>{col}</th>"
    html += "</tr></thead><tbody>"

    # Hourly rows
    for idx in hourly_times:
        time_str = idx.strftime('%H:%M')
        date_str = idx.strftime('%d/%m')
        # Show date if different from today
        if idx.date() != now.date():
            time_str = f"{time_str}<br><span style='font-size:9px;color:{FLEXOKI_BASE[600]}'>{date_str}</span>"

        html += f"<tr><td>{time_str}</td>"
        for col in cols:
            val = pivot.loc[idx, col]
            val_int = int(round(val))

            # Determine styling
            classes = []
            if val >= 300:
                classes.append('extreme')
            elif val >= 100:
                classes.append('high')
            if idx == peak_times.get(col):
                classes.append('peak')

            class_str = f' class="{" ".join(classes)}"' if classes else ''
            html += f"<td{class_str}>{val_int:,}</td>"
        html += "</tr>"

    # 24hr average row
    html += f'<tr class="summary"><td>24hr avg</td>'
    for col in cols:
        val = avg_24h.get(col, 0)
        html += f"<td>{val:,}</td>"
    html += "</tr>"

    # Previous 24hr average row (if available and from a different P30 run)
    previous = load_previous_forecast()
    current_run_time_str = run_time.isoformat() if run_time else None
    previous_run_time_str = previous.get('run_time') if previous else None

    # Always show previous if it exists (helps verify cache is working)
    if previous and previous.get('avg_24h'):
        is_same_run = previous_run_time_str == current_run_time_str
        prev_avg = previous['avg_24h']
        prev_time_str = ""
        if previous_run_time_str:
            try:
                prev_time = datetime.fromisoformat(previous_run_time_str)
                prev_time_str = f" ({prev_time.strftime('%H:%M')})"
            except:
                pass
        # Gray out if same run (indicates cache is current)
        row_style = f"color: {FLEXOKI_BASE[400]};" if is_same_run else f"color: {FLEXOKI_BASE[600]};"
        html += f'<tr class="summary" style="{row_style}"><td>Prev 24hr avg{prev_time_str}</td>'
        for col in cols:
            val = prev_avg.get(col, 0)
            html += f"<td>{val:,}</td>"
        html += "</tr>"

    html += "</tbody></table>"

    # Add forecast summary
    summary_parts = []
    current_summary = {}

    # Calculate NEM-wide totals
    if 'DEMAND_FORECAST' in predispatch_df.columns:
        demand_by_time = predispatch_df.groupby('SETTLEMENTDATE')['DEMAND_FORECAST'].sum()
        peak_demand = demand_by_time.max() / 1000  # Convert to GW
        peak_demand_time = demand_by_time.idxmax()
        summary_parts.append(f"Peak demand: <b>{peak_demand:.1f} GW</b> at {peak_demand_time.strftime('%H:%M')}")
        current_summary['peak_demand'] = peak_demand
        current_summary['peak_demand_time'] = peak_demand_time.strftime('%H:%M')

    # Renewable forecasts
    if 'WIND_FORECAST' in predispatch_df.columns and 'SOLAR_FORECAST' in predispatch_df.columns:
        renewables_by_time = predispatch_df.groupby('SETTLEMENTDATE')[['WIND_FORECAST', 'SOLAR_FORECAST']].sum()
        renewables_by_time['TOTAL'] = renewables_by_time['WIND_FORECAST'] + renewables_by_time['SOLAR_FORECAST']

        max_renewable = renewables_by_time['TOTAL'].max() / 1000  # GW
        max_renewable_time = renewables_by_time['TOTAL'].idxmax()

        # Get breakdown at peak
        wind_at_peak = renewables_by_time.loc[max_renewable_time, 'WIND_FORECAST'] / 1000
        solar_at_peak = renewables_by_time.loc[max_renewable_time, 'SOLAR_FORECAST'] / 1000

        summary_parts.append(f"Peak renewables: <b>{max_renewable:.1f} GW</b> at {max_renewable_time.strftime('%H:%M')} (Wind {wind_at_peak:.1f}, Solar {solar_at_peak:.1f})")
        current_summary['peak_renewables'] = max_renewable
        current_summary['peak_renewables_time'] = max_renewable_time.strftime('%H:%M')

    # Compare with previous forecast (only if from a different P30 run)
    if previous and current_summary and previous_run_time_str != current_run_time_str:
        changes = []
        if 'peak_demand' in previous and 'peak_demand' in current_summary:
            demand_diff = current_summary['peak_demand'] - previous['peak_demand']
            if abs(demand_diff) >= 0.1:
                arrow = '↑' if demand_diff > 0 else '↓'
                changes.append(f"demand {arrow}{abs(demand_diff):.1f} GW")

        if 'peak_renewables' in previous and 'peak_renewables' in current_summary:
            renew_diff = current_summary['peak_renewables'] - previous['peak_renewables']
            if abs(renew_diff) >= 0.1:
                arrow = '↑' if renew_diff > 0 else '↓'
                changes.append(f"renewables {arrow}{abs(renew_diff):.1f} GW")

        if changes:
            prev_time_str = ""
            if previous_run_time_str:
                try:
                    prev_time = datetime.fromisoformat(previous_run_time_str)
                    prev_time_str = f" (P30: {prev_time.strftime('%H:%M')})"
                except:
                    pass
            summary_parts.append(f"<span style='color:{FLEXOKI_BASE[600]}'>vs previous{prev_time_str}: {', '.join(changes)}</span>")

    # Save current forecast for next comparison - but ONLY if run_time is different
    # This preserves the previous forecast for comparison across multiple page loads
    if current_summary and run_time and current_run_time_str != previous_run_time_str:
        current_summary['run_time'] = current_run_time_str
        # Save 24hr avg prices for comparison
        current_summary['avg_24h'] = {col: int(avg_24h.get(col, 0)) for col in cols}
        save_current_forecast(current_summary)
        logger.info(f"Saved new forecast cache for P30 run {run_time.strftime('%H:%M')}")

    # Build summary HTML
    if summary_parts:
        html += f"""
        <div style="margin-top: 10px; padding: 8px; background: {FLEXOKI_BASE[50]};
                    border-radius: 4px; font-size: 11px; line-height: 1.6; color: {FLEXOKI_BASE[800]};">
            {'<br>'.join(summary_parts)}
        </div>
        """

    return html


def create_forecast_component():
    """
    Create the complete forecast component for the Today tab.

    Returns:
        Panel component containing the forecast table
    """
    try:
        predispatch_df, run_time = fetch_predispatch_forecasts()
        html = create_forecast_table(predispatch_df, run_time)
        return pn.pane.HTML(html, sizing_mode='stretch_width')
    except Exception as e:
        logger.error(f"Error creating forecast component: {e}")
        return pn.pane.HTML(
            f'<div style="color: {FLEXOKI_ACCENT["red"]}; padding: 10px;">Error loading forecast: {e}</div>'
        )
