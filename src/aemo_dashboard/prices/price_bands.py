"""Price-band analysis for the Prices tab (Plotly).

Computes band contributions, builds butterfly stacked-bar charts,
and the detail table.
"""

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..shared.flexoki_theme import FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT

logger = logging.getLogger(__name__)

PRICE_BANDS = [
    ('Below $0', -float('inf'), 0),
    ('$0-$300', 0, 300),
    ('$301-$1000', 301, 1000),
    ('Above $1000', 1000, float('inf')),
]

BAND_ORDER = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']

BAND_COLORS = {
    'Below $0': FLEXOKI_ACCENT['red'],
    '$0-$300': FLEXOKI_ACCENT['green'],
    '$301-$1000': FLEXOKI_ACCENT['orange'],
    'Above $1000': FLEXOKI_ACCENT['magenta'],
}

DEFAULT_DEMANDS = {
    'NSW1': 7500,
    'QLD1': 6500,
    'VIC1': 5500,
    'SA1': 1500,
    'TAS1': 1000,
}


def compute_price_bands(original_price_data, selected_regions):
    """Compute price-band contributions for each region.

    Returns
    -------
    band_contributions : list[dict]
    bands_df : DataFrame | None
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


def build_band_charts(bands_df, date_range_text, flexoki_bg_hook=None):
    """Build butterfly chart: Time % (left) vs Contribution $/MWh (right).

    One subplot per region. Returns a single Plotly figure.

    Returns
    -------
    fig : go.Figure
    """
    regions = list(bands_df['Region'].unique())
    n_regions = len(regions)

    if n_regions == 0:
        fig = go.Figure()
        fig.update_layout(
            autosize=True, height=400,
            paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
            annotations=[dict(text='No data available', xref='paper', yref='paper',
                              x=0.5, y=0.5, showarrow=False,
                              font=dict(size=14, color=FLEXOKI_BASE[600]))],
        )
        return fig

    # Compute contribution % for butterfly right side
    region_means = bands_df.groupby('Region')['Contribution'].sum().to_dict()
    bands_df = bands_df.copy()
    bands_df['Contribution %'] = bands_df.apply(
        lambda row: (row['Contribution'] / region_means[row['Region']] * 100)
        if region_means.get(row['Region'], 0) > 0 else 0,
        axis=1,
    )

    # Grid layout
    if n_regions <= 2:
        rows, cols = 1, n_regions
    elif n_regions <= 4:
        rows, cols = 2, 2
    else:
        rows, cols = 2, 3

    height = 320 * rows
    subplot_titles = [
        f"{r}  (Flat load: ${region_means.get(r, 0):.0f}/MWh)" for r in regions
    ]

    fig = make_subplots(
        rows=rows, cols=cols,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.12,
        vertical_spacing=0.25,
    )

    for idx, region in enumerate(regions):
        row = idx // cols + 1
        col = idx % cols + 1
        region_df = bands_df[bands_df['Region'] == region]

        for band in BAND_ORDER:
            band_row = region_df[region_df['Price Band'] == band]
            if band_row.empty:
                continue

            time_pct = band_row['Percentage'].values[0]
            contrib_pct = band_row['Contribution %'].values[0]
            contrib_dollars = band_row['Contribution'].values[0]
            color = BAND_COLORS[band]

            # Time % bar extends LEFT (negative) — lighter shade
            fig.add_trace(go.Bar(
                y=[band],
                x=[-time_pct],
                orientation='h',
                marker=dict(color=color, opacity=0.5),
                text=f'{time_pct:.1f}%',
                textposition='outside',
                textfont=dict(size=9, color=FLEXOKI_BASE[800]),
                hovertemplate=f'{band}: {time_pct:.1f}% of time<extra>{region}</extra>',
                showlegend=False,
            ), row=row, col=col)

            # Contribution % bar extends RIGHT (positive) — full color
            # Label: show $/MWh contribution
            bar_width = abs(contrib_pct)
            if bar_width > 12:
                text_label = f'${contrib_dollars:.0f}'
                text_pos = 'inside'
                text_color = 'white'
            elif bar_width > 5:
                text_label = f'${contrib_dollars:.0f}'
                text_pos = 'inside'
                text_color = 'white'
            elif abs(contrib_dollars) > 1:
                text_label = f'${contrib_dollars:.0f}'
                text_pos = 'outside'
                text_color = color
            else:
                text_label = ''
                text_pos = 'none'
                text_color = color

            fig.add_trace(go.Bar(
                y=[band],
                x=[contrib_pct],
                orientation='h',
                marker=dict(color=color),
                text=text_label,
                textposition=text_pos,
                textfont=dict(size=10, color=text_color),
                hovertemplate=(
                    f'{band}: ${contrib_dollars:.0f}/MWh '
                    f'({contrib_pct:.1f}% of flat load)<extra>{region}</extra>'
                ),
                showlegend=False,
            ), row=row, col=col)

        # Symmetric x-axis
        max_val = max(
            100,
            region_df['Percentage'].max() if not region_df.empty else 100,
            abs(region_df['Contribution %']).max() if not region_df.empty else 100,
        )
        x_limit = max_val + 15

        fig.update_xaxes(
            range=[-x_limit, x_limit],
            showgrid=True, gridcolor=FLEXOKI_BASE[100], gridwidth=0.5,
            zeroline=True, zerolinecolor=FLEXOKI_BASE[300], zerolinewidth=1.5,
            showline=False,
            tickfont=dict(color=FLEXOKI_BASE[600], size=9),
            title=dict(
                text='<-- Time %          Contribution $/MWh -->',
                font=dict(size=9, color=FLEXOKI_BASE[800]),
            ),
            row=row, col=col,
        )
        fig.update_yaxes(
            categoryorder='array',
            categoryarray=list(reversed(BAND_ORDER)),
            tickfont=dict(color=FLEXOKI_BLACK, size=10),
            showgrid=False, showline=False,
            row=row, col=col,
        )

    fig.update_layout(
        autosize=True,
        height=height,
        barmode='overlay',
        bargap=0.35,
        paper_bgcolor=FLEXOKI_PAPER,
        plot_bgcolor=FLEXOKI_PAPER,
        title=dict(
            text=(
                f'Price Band Contribution ({date_range_text})<br>'
                f'<sub>Left = % of time  |  Right = $/MWh contribution to flat load average</sub>'
            ),
            font=dict(size=14, color=FLEXOKI_BLACK),
            x=0.5,
        ),
        margin=dict(l=100, r=40, t=80, b=40),
        annotations=[
            a for a in fig.layout.annotations
        ] + [
            dict(
                text='Data: AEMO', xref='paper', yref='paper',
                x=1.0, y=-0.02, showarrow=False,
                font=dict(size=9, color=FLEXOKI_BASE[600], style='italic'),
                xanchor='right',
            ),
        ],
    )

    # Style subplot titles
    for ann in fig.layout.annotations:
        if hasattr(ann, 'font') and ann.font is not None:
            ann.font.size = 12
            ann.font.color = FLEXOKI_BLACK

    return fig


def build_band_detail_table(
    bands_df,
    original_price_data,
    selected_regions,
    region_avg_demand,
):
    """Build the price-band details DataFrame for the Tabulator widget.

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
