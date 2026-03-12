"""Price-band analysis for the Prices tab.

Extracted from gen_dash.py — computes band contributions,
builds the butterfly stacked-bar charts, and the detail table.
"""

import logging

import holoviews as hv
import numpy as np
import pandas as pd

from ..shared.flexoki_theme import FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT

logger = logging.getLogger(__name__)

PRICE_BANDS = [
    ('Below $0', -float('inf'), 0),
    ('$0-$300', 0, 300),
    ('$301-$1000', 301, 1000),
    ('Above $1000', 1000, float('inf')),
]

BAND_ORDER = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']

BAND_COLORS = [
    FLEXOKI_ACCENT['red'],
    FLEXOKI_ACCENT['green'],
    FLEXOKI_ACCENT['orange'],
    FLEXOKI_ACCENT['magenta'],
]

DEFAULT_DEMANDS = {
    'NSW1': 7500,
    'QLD1': 6500,
    'VIC1': 5500,
    'SA1': 1500,
    'TAS1': 1000,
}


def compute_price_bands(original_price_data, selected_regions):
    """Compute price-band contributions for each region.

    Parameters
    ----------
    original_price_data : DataFrame
        Resampled (but unsmoothed) price data with ``REGIONID`` and ``RRP``.
    selected_regions : list[str]

    Returns
    -------
    band_contributions : list[dict]
        One entry per (region, band) combination.
    bands_df : DataFrame | None
        Same data as a DataFrame, or ``None`` when empty.
    """
    band_contributions = []

    for region in selected_regions:
        if 'RRP' not in original_price_data.columns:
            logger.warning("RRP column not found in original_price_data")
            continue

        region_data = original_price_data[original_price_data['REGIONID'] == region]['RRP']
        if region_data.empty:
            continue

        for band_name, low, high in PRICE_BANDS:
            if low == -float('inf'):
                band_mask = region_data < high
            elif high == float('inf'):
                band_mask = region_data >= low
            else:
                band_mask = (region_data >= low) & (region_data < high)

            band_data = region_data[band_mask]
            if len(band_data) > 0:
                band_proportion = len(band_data) / len(region_data)
                band_avg = band_data.mean()
                contribution = band_proportion * band_avg

                band_contributions.append({
                    'Region': region,
                    'Price Band': band_name,
                    'Contribution': contribution,
                    'Percentage': band_proportion * 100,
                    'Band Average': band_avg,
                })

    if not band_contributions:
        return band_contributions, None

    bands_df = pd.DataFrame(band_contributions)
    bands_df['Price Band'] = pd.Categorical(
        bands_df['Price Band'], categories=BAND_ORDER, ordered=True
    )
    bands_df = bands_df.sort_values(['Region', 'Price Band'])
    return band_contributions, bands_df


def build_band_charts(bands_df, date_range_text, flexoki_bg_hook):
    """Build the two stacked-bar charts (contribution + time distribution).

    Parameters
    ----------
    bands_df : DataFrame
        Output of :func:`compute_price_bands`.
    date_range_text : str
        Human-readable date range for titles.
    flexoki_bg_hook : callable
        Bokeh hook returned by ``dashboard._get_flexoki_background_hook()``.

    Returns
    -------
    contrib_plot, time_plot : hv.Overlay
    """
    # Chart 1: Price Contribution
    contrib_plot = bands_df.hvplot.bar(
        x='Region', y='Contribution', by='Price Band',
        stacked=True, responsive=True, height=250,
        xlabel='', ylabel='Price Contribution ($/MWh)',
        title=f'Price Band Contribution ({date_range_text})',
        color=BAND_COLORS, bgcolor=FLEXOKI_PAPER,
        legend='top', toolbar='above',
    ).opts(
        xrotation=0, show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        fontsize={'ticks': 10, 'title': 12, 'ylabel': 10},
        hooks=[flexoki_bg_hook],
    )

    # Labels for contribution bars
    contrib_overlays = []
    for region in bands_df['Region'].unique():
        region_bands = bands_df[bands_df['Region'] == region]
        cumulative = 0
        for _, row in region_bands.iterrows():
            label = f"${int(row['Contribution'])}"
            if row['Contribution'] > 3:
                y_pos = cumulative + row['Contribution'] / 2
                contrib_overlays.append(
                    hv.Text(region, y_pos, label).opts(
                        color=FLEXOKI_BLACK, fontsize=8,
                        text_align='center', text_baseline='middle',
                    )
                )
            cumulative += row['Contribution']
    if contrib_overlays:
        contrib_plot = contrib_plot * hv.Overlay(contrib_overlays)

    # Chart 2: Time Distribution
    time_plot = bands_df.hvplot.bar(
        x='Region', y='Percentage', by='Price Band',
        stacked=True, responsive=True, height=250,
        xlabel='Region', ylabel='Time Distribution (%)',
        title='Time in Each Price Band',
        color=BAND_COLORS, bgcolor=FLEXOKI_PAPER,
        legend='bottom', toolbar='above',
    ).opts(
        xrotation=0, show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        fontsize={'ticks': 10, 'title': 12, 'ylabel': 10, 'xlabel': 10},
        hooks=[flexoki_bg_hook],
    )

    # Labels for time bars
    time_overlays = []
    for region in bands_df['Region'].unique():
        region_bands = bands_df[bands_df['Region'] == region]
        cumulative = 0
        for _, row in region_bands.iterrows():
            if row['Percentage'] < 1:
                label = f"{row['Percentage']:.2f}%"
            else:
                label = f"{row['Percentage']:.0f}%"
            if row['Percentage'] > 5:
                y_pos = cumulative + row['Percentage'] / 2
                time_overlays.append(
                    hv.Text(region, y_pos, label).opts(
                        color=FLEXOKI_BLACK, fontsize=8,
                        text_align='center', text_baseline='middle',
                    )
                )
            cumulative += row['Percentage']
    if time_overlays:
        time_plot = time_plot * hv.Overlay(time_overlays)

    return contrib_plot, time_plot


def build_band_detail_table(
    bands_df,
    original_price_data,
    selected_regions,
    region_avg_demand,
):
    """Build the price-band details DataFrame for the Tabulator widget.

    Parameters
    ----------
    bands_df : DataFrame
    original_price_data : DataFrame
    selected_regions : list[str]
    region_avg_demand : dict[str, float]
        Average demand (MW) per region.

    Returns
    -------
    high_price_df : DataFrame | None
    """
    # Determine data resolution
    total_periods = len(
        original_price_data[original_price_data['REGIONID'] == selected_regions[0]]
    )
    if total_periods > 0:
        sorted_times = (
            original_price_data[original_price_data['REGIONID'] == selected_regions[0]]
            ['SETTLEMENTDATE'].sort_values()
        )
        if len(sorted_times) > 1:
            td = pd.to_datetime(sorted_times.iloc[1]) - pd.to_datetime(sorted_times.iloc[0])
            periods_per_hour = 60 / (td.total_seconds() / 60)
        else:
            periods_per_hour = 2
    else:
        periods_per_hour = 2

    total_hours = total_periods / periods_per_hour

    # Mean price per region
    region_means = {}
    for region in selected_regions:
        rd = original_price_data[original_price_data['REGIONID'] == region]['RRP']
        region_means[region] = rd.mean()

    rows = []
    for _, row in bands_df.iterrows():
        mean_price = region_means.get(row['Region'], 0)
        pct_contribution = (row['Contribution'] / mean_price * 100) if mean_price > 0 else 0
        hours_in_band = (row['Percentage'] / 100) * total_hours
        avg_demand_mw = region_avg_demand.get(row['Region'], 1500)
        revenue_millions = (row['Band Average'] * hours_in_band * avg_demand_mw) / 1_000_000

        if revenue_millions >= 1000:
            revenue_str = f"${revenue_millions / 1000:.1f}bn"
        else:
            revenue_str = f"${revenue_millions:.0f}m"

        rows.append({
            'Region': row['Region'],
            'Price Band': row['Price Band'],
            '% of Time': f"{row['Percentage']:.1f}%",
            'Avg Price': f"${row['Band Average']:.0f}",
            'Revenue': revenue_str,
            'Contribution': f"${row['Contribution']:.1f}",
            '% Contribution': f"{pct_contribution:.1f}%",
        })

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values(['Region', 'Price Band'])

    # Blank repeated region names for visual grouping
    df['_Region'] = df['Region']
    prev = None
    for idx in df.index:
        if df.loc[idx, 'Region'] == prev:
            df.loc[idx, '_Region'] = ''
        else:
            prev = df.loc[idx, 'Region']

    df = df[['_Region', 'Price Band', '% of Time', 'Avg Price',
             'Revenue', 'Contribution', '% Contribution']]
    df = df.rename(columns={'_Region': 'Region'})
    return df


def compute_region_avg_demand(query_manager, start_datetime, end_datetime, selected_regions):
    """Compute average demand per region from generation data.

    Falls back to static estimates if query fails.
    """
    region_avg_demand = {}
    try:
        gen_data = query_manager.query_generation_by_fuel(
            start_date=start_datetime,
            end_date=end_datetime,
            regions=selected_regions,
            aggregation='raw',
        )
        if not gen_data.empty:
            excluded_cols = [
                'timestamp', 'region', 'Transmission Flow',
                'Transmission Exports', 'Battery Storage',
            ]
            gen_cols = [c for c in gen_data.columns if c not in excluded_cols]
            gen_data['Total_MW'] = gen_data[gen_cols].sum(axis=1)
            for region in selected_regions:
                rd = gen_data[gen_data['region'] == region]
                if not rd.empty:
                    region_avg_demand[region] = rd['Total_MW'].mean()
                    logger.info(f"Average demand for {region}: {region_avg_demand[region]:.0f} MW")
                else:
                    region_avg_demand[region] = DEFAULT_DEMANDS.get(region, 1500)
        else:
            for region in selected_regions:
                region_avg_demand[region] = DEFAULT_DEMANDS.get(region, 1500)
    except Exception as e:
        logger.warning(f"Error calculating regional demand: {e}, using estimates")
        for region in selected_regions:
            region_avg_demand[region] = DEFAULT_DEMANDS.get(region, 1500)

    return region_avg_demand
