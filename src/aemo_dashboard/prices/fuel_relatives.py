"""Fuel-relatives analysis for the Prices tab (Plotly).

Queries DuckDB for daily fuel-weighted prices, applies 90-day LOESS
smoothing, builds two Plotly charts (absolute prices and price index
normalised to Flat Load = 100).
"""

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from ..shared.flexoki_theme import FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT

logger = logging.getLogger(__name__)

FUEL_COLOR_MAP = {
    'Flat Load': FLEXOKI_BASE[600],
    'Wind': FLEXOKI_ACCENT['green'],
    'Solar': FLEXOKI_ACCENT['yellow'],
    'Coal': FLEXOKI_BASE[400],
    'Gas': FLEXOKI_ACCENT['red'],
    'Water': FLEXOKI_ACCENT['cyan'],
    'Battery Storage': FLEXOKI_ACCENT['purple'],
}


def _build_fuel_relatives_query(selected_fuel_region, start_dt, end_dt):
    """Return the DuckDB SQL query for daily fuel-weighted prices."""
    return f"""
        WITH daily_generation AS (
            SELECT
                DATE(g.settlementdate) as date,
                CASE
                    WHEN d.fuel IN ('Gas', 'Gas other', 'CCGT', 'OCGT') THEN 'Gas'
                    ELSE d.fuel
                END as fuel_type,
                g.duid,
                SUM(g.scadavalue) as daily_generation
            FROM generation_30min g
            JOIN duid_mapping d ON g.duid = d.duid
            WHERE g.settlementdate >= '{start_dt.isoformat()}'
              AND g.settlementdate <= '{end_dt.isoformat()}'
              AND d.region = '{selected_fuel_region}'
              AND d.fuel IN ('Coal', 'Gas', 'Gas other', 'Wind', 'Solar', 'Water', 'CCGT', 'OCGT', 'Battery Storage')
              AND NOT (d.fuel = 'Battery Storage' AND g.scadavalue < 0)
            GROUP BY DATE(g.settlementdate),
                     CASE
                         WHEN d.fuel IN ('Gas', 'Gas other', 'CCGT', 'OCGT') THEN 'Gas'
                         ELSE d.fuel
                     END,
                     g.duid
        ),
        daily_prices AS (
            SELECT
                settlementdate,
                DATE(settlementdate) as date,
                rrp
            FROM prices_30min
            WHERE settlementdate >= '{start_dt.isoformat()}'
              AND settlementdate <= '{end_dt.isoformat()}'
              AND regionid = '{selected_fuel_region}'
        ),
        fuel_weighted AS (
            SELECT
                dp.date,
                dg.fuel_type,
                SUM(g.scadavalue * dp.rrp) / NULLIF(SUM(g.scadavalue), 0) as weighted_price
            FROM generation_30min g
            JOIN duid_mapping d ON g.duid = d.duid
            JOIN daily_prices dp ON g.settlementdate = dp.settlementdate
            JOIN daily_generation dg ON DATE(g.settlementdate) = dg.date
                AND CASE
                    WHEN d.fuel IN ('Gas', 'Gas other', 'CCGT', 'OCGT') THEN 'Gas'
                    ELSE d.fuel
                END = dg.fuel_type
                AND g.duid = dg.duid
            WHERE g.settlementdate >= '{start_dt.isoformat()}'
              AND g.settlementdate <= '{end_dt.isoformat()}'
              AND d.region = '{selected_fuel_region}'
              AND NOT (d.fuel = 'Battery Storage' AND g.scadavalue < 0)
            GROUP BY dp.date, dg.fuel_type
        ),
        flat_load AS (
            SELECT
                DATE(settlementdate) as date,
                AVG(rrp) as flat_load_price
            FROM prices_30min
            WHERE settlementdate >= '{start_dt.isoformat()}'
              AND settlementdate <= '{end_dt.isoformat()}'
              AND regionid = '{selected_fuel_region}'
            GROUP BY DATE(settlementdate)
        )
        SELECT
            COALESCE(f.date, fw.date) as date,
            f.flat_load_price,
            fw.fuel_type,
            fw.weighted_price
        FROM flat_load f
        FULL OUTER JOIN fuel_weighted fw ON f.date = fw.date
        ORDER BY date, fuel_type
    """


def query_fuel_relatives(duckdb_conn, selected_fuel_region):
    """Execute the DuckDB query and return a pivoted daily-prices DataFrame.

    Returns
    -------
    daily_prices : DataFrame
        Index = date, columns = fuel types + 'Flat Load'.
    """
    start_dt = pd.to_datetime('2020-01-01')
    end_dt = pd.to_datetime('now')

    query = _build_fuel_relatives_query(selected_fuel_region, start_dt, end_dt)
    logger.info("Executing DuckDB query for fuel relatives...")
    result_df = duckdb_conn.execute(query).df()

    if result_df.empty:
        return pd.DataFrame()

    logger.info(f"Query returned {len(result_df)} rows")

    # Pivot fuel-weighted prices
    fuel_pivot = result_df[result_df['fuel_type'].notna()].pivot(
        index='date', columns='fuel_type', values='weighted_price',
    )

    # Flat load prices
    flat_load = result_df[['date', 'flat_load_price']].drop_duplicates('date').set_index('date')
    flat_load.columns = ['Flat Load']

    daily_prices = flat_load.join(fuel_pivot)

    # Fill gaps so we have a continuous date range
    date_range = pd.date_range(
        start=daily_prices.index.min(),
        end=daily_prices.index.max(),
        freq='D',
    )
    daily_prices = daily_prices.reindex(date_range)

    logger.info(f"Daily prices shape: {daily_prices.shape}, "
                f"range: {daily_prices.index.min()} to {daily_prices.index.max()}")

    return daily_prices


def apply_loess_smoothing(daily_prices):
    """Apply 90-day LOESS smoothing to each fuel column.

    Returns
    -------
    smoothed_data : DataFrame
    """
    from statsmodels.nonparametric.smoothers_lowess import lowess

    smoothed_data = pd.DataFrame(index=daily_prices.index)

    for col in daily_prices.columns:
        y = daily_prices[col].values.copy()

        if col == 'Battery Storage':
            y[y == 0] = np.nan

        mask = ~np.isnan(y)
        valid_count = mask.sum()
        logger.info(f"Column {col}: {valid_count} valid points out of {len(y)}")

        # Trim Battery Storage to start from first valid point
        if col == 'Battery Storage' and valid_count > 0:
            first_valid_idx = np.where(mask)[0][0]
            y[:first_valid_idx] = np.nan
            mask = ~np.isnan(y)
            valid_count = mask.sum()
            logger.info(f"  Battery Storage: trimmed to start from index {first_valid_idx}")

        if valid_count > 90:
            series = pd.Series(y, index=daily_prices.index)
            filled_series = series.ffill(limit=7).bfill(limit=7)

            try:
                filled_values = filled_series.values
                filled_mask = ~np.isnan(filled_values)

                if filled_mask.sum() > 90:
                    x_numeric = np.arange(len(filled_values))
                    frac = min(90.0 / filled_mask.sum(), 0.5)
                    logger.info(f"  Applying LOESS with frac={frac:.3f} for {col}")

                    smoothed = lowess(
                        filled_values[filled_mask],
                        x_numeric[filled_mask],
                        frac=frac, it=0,
                    )

                    smoothed_values = np.full(len(filled_values), np.nan)
                    smoothed_values[filled_mask] = smoothed[:, 1]

                    smoothed_series = pd.Series(smoothed_values, index=daily_prices.index)
                    smoothed_series = smoothed_series.interpolate(
                        method='linear', limit_direction='both',
                    )

                    if col == 'Battery Storage':
                        original_mask = ~np.isnan(daily_prices[col].values.copy())
                        if original_mask.sum() > 0:
                            first_valid = np.where(original_mask)[0][0]
                            smoothed_series.iloc[:first_valid] = np.nan

                    smoothed_data[col] = smoothed_series
                    logger.info(f"  Applied 90-day LOESS to {col}")
                else:
                    smoothed_data[col] = filled_series
            except Exception as e:
                logger.warning(f"  LOESS failed for {col}: {e}, using interpolation only")
                smoothed_data[col] = filled_series
        elif col == 'Battery Storage' and valid_count > 0:
            smoothed_data[col] = pd.Series(y, index=daily_prices.index)
        else:
            logger.info(f"  Insufficient data for {col} ({valid_count} points), skipping")

    return smoothed_data


def _apply_fuel_layout(fig, title):
    """Apply standard Flexoki layout to fuel relatives charts."""
    fig.update_layout(
        autosize=True,
        height=400,
        paper_bgcolor=FLEXOKI_PAPER,
        plot_bgcolor=FLEXOKI_PAPER,
        title=dict(text=title, font=dict(size=14, color=FLEXOKI_BLACK)),
        legend=dict(bgcolor=FLEXOKI_PAPER, font=dict(size=10)),
        margin=dict(l=60, r=30, t=40, b=40),
        xaxis=dict(title='Date', showgrid=False, tickfont=dict(color=FLEXOKI_BASE[800])),
        yaxis=dict(showgrid=False, tickfont=dict(color=FLEXOKI_BASE[800])),
        annotations=[dict(
            text='<i>data: AEMO, design: ITK</i>',
            xref='paper', yref='paper', x=1.0, y=-0.12,
            showarrow=False, font=dict(size=9, color=FLEXOKI_BASE[500]), xanchor='right',
        )],
    )


def build_fuel_relatives_chart(smoothed_data, selected_fuel_region, attribution_hook=None, flexoki_bg_hook=None):
    """Build the absolute fuel-weighted price chart (Plotly).

    Returns
    -------
    fig : go.Figure | None
    """
    if smoothed_data.empty or not smoothed_data.notna().any().any():
        return None

    fig = go.Figure()

    for col in smoothed_data.columns:
        color = FUEL_COLOR_MAP.get(col, FLEXOKI_BASE[500])
        dash = 'dot' if col == 'Flat Load' else 'solid'
        fig.add_trace(go.Scatter(
            x=smoothed_data.index,
            y=smoothed_data[col],
            name=col,
            mode='lines',
            line=dict(width=2, color=color, dash=dash),
            hovertemplate=f'{col}: $%{{y:.1f}}/MWh<extra></extra>',
        ))

    _apply_fuel_layout(
        fig,
        f'90-Day LOESS Smoothed Fuel-Weighted Prices - {selected_fuel_region} '
        f'(Gas combined, Battery discharge only)',
    )
    fig.update_yaxes(title='Price ($/MWh)')

    return fig


def build_price_index_chart(smoothed_data, selected_fuel_region, attribution_hook=None, flexoki_bg_hook=None):
    """Build the price-index chart (normalised to Flat Load = 100, Plotly).

    Returns
    -------
    fig : go.Figure | None
    """
    if 'Flat Load' not in smoothed_data.columns:
        return None

    indexed_data = pd.DataFrame(index=smoothed_data.index)
    for col in smoothed_data.columns:
        flat_vals = smoothed_data['Flat Load'].values
        fuel_vals = smoothed_data[col].values
        valid = (flat_vals != 0) & ~np.isnan(flat_vals)
        indexed = np.full(len(fuel_vals), np.nan)
        indexed[valid] = (fuel_vals[valid] / flat_vals[valid]) * 100
        indexed_data[col] = indexed

    fig = go.Figure()

    for col in indexed_data.columns:
        color = FUEL_COLOR_MAP.get(col, FLEXOKI_BASE[500])
        dash = 'dot' if col == 'Flat Load' else 'solid'
        fig.add_trace(go.Scatter(
            x=indexed_data.index,
            y=indexed_data[col],
            name=col,
            mode='lines',
            line=dict(width=2, color=color, dash=dash),
            hovertemplate=f'{col}: %{{y:.1f}}<extra></extra>',
        ))

    # Reference line at 100
    fig.add_hline(y=100, line_dash='dash', line_color='gray', line_width=1)

    _apply_fuel_layout(
        fig,
        f'Price Index Relative to Flat Load - {selected_fuel_region}',
    )
    fig.update_yaxes(title='Price Index (Flat Load = 100)')

    return fig
