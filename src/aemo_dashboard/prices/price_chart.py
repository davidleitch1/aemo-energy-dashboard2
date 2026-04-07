"""Price time-series and time-of-day charts for the Prices tab (Plotly).

Extracted from gen_dash.py.
"""

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from ..shared.flexoki_theme import FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT

logger = logging.getLogger(__name__)

REGION_COLORS = {
    'NSW1': FLEXOKI_ACCENT['green'],
    'QLD1': FLEXOKI_ACCENT['orange'],
    'SA1': FLEXOKI_ACCENT['magenta'],
    'TAS1': FLEXOKI_ACCENT['cyan'],
    'VIC1': FLEXOKI_ACCENT['purple'],
}


def build_price_time_series(
    price_data,
    y_col,
    ylabel,
    use_log,
    date_range_text,
    start_date,
    end_date,
    attribution_hook=None,
    flexoki_bg_hook=None,
):
    """Build the price time-series Plotly figure.

    Returns
    -------
    fig : go.Figure
    """
    fig = go.Figure()

    for region in price_data['REGIONID'].unique():
        region_data = price_data[price_data['REGIONID'] == region]
        color = REGION_COLORS.get(region, '#6F6E69')
        fig.add_trace(go.Scatter(
            x=region_data['SETTLEMENTDATE'],
            y=region_data[y_col],
            name=region,
            mode='lines',
            line=dict(width=2, color=color),
            hovertemplate=f'{region}: $%{{y:.1f}}/MWh<extra></extra>',
        ))

    fig.update_layout(
        autosize=True,
        height=400,
        paper_bgcolor=FLEXOKI_PAPER,
        plot_bgcolor=FLEXOKI_PAPER,
        title=dict(
            text=f'Electricity Spot Prices by Region ({date_range_text})',
            font=dict(size=14, color=FLEXOKI_BLACK),
        ),
        legend=dict(bgcolor=FLEXOKI_PAPER, font=dict(size=10)),
        margin=dict(l=60, r=30, t=40, b=40),
        xaxis=dict(
            title='Time', showgrid=False,
            range=[pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(days=1)],
            tickfont=dict(color=FLEXOKI_BASE[800]),
        ),
        yaxis=dict(
            title=ylabel, showgrid=False,
            type='log' if use_log else 'linear',
            tickfont=dict(color=FLEXOKI_BASE[800]),
        ),
        annotations=[dict(
            text='<i>data: AEMO, design: ITK</i>',
            xref='paper', yref='paper', x=1.0, y=-0.12,
            showarrow=False, font=dict(size=9, color=FLEXOKI_BASE[500]), xanchor='right',
        )],
    )

    return fig


def build_tod_chart(original_price_data, date_range_text, attribution_hook=None, flexoki_bg_hook=None):
    """Build the time-of-day average-price line chart (Plotly).

    Returns
    -------
    fig : go.Figure
    """
    df = original_price_data.copy()
    df['Hour'] = pd.to_datetime(df['SETTLEMENTDATE']).dt.hour

    tod_data = df.groupby(['Hour', 'REGIONID'])['RRP'].mean().reset_index()
    tod_data.rename(columns={'RRP': 'Average Price'}, inplace=True)

    fig = go.Figure()

    for region in tod_data['REGIONID'].unique():
        region_df = tod_data[tod_data['REGIONID'] == region]
        color = REGION_COLORS.get(region, '#6F6E69')
        fig.add_trace(go.Scatter(
            x=region_df['Hour'],
            y=region_df['Average Price'],
            name=region,
            mode='lines',
            line=dict(width=2, color=color),
            hovertemplate=f'{region}: $%{{y:.1f}}/MWh<extra></extra>',
        ))

    fig.update_layout(
        autosize=True,
        height=400,
        paper_bgcolor=FLEXOKI_PAPER,
        plot_bgcolor=FLEXOKI_PAPER,
        title=dict(
            text=f'Average Price by Hour ({date_range_text})',
            font=dict(size=14, color=FLEXOKI_BLACK),
        ),
        legend=dict(bgcolor=FLEXOKI_PAPER, font=dict(size=10)),
        margin=dict(l=60, r=30, t=40, b=40),
        xaxis=dict(
            title='Hour of Day', showgrid=False,
            tickvals=list(range(0, 24, 3)),
            tickfont=dict(color=FLEXOKI_BASE[800]),
        ),
        yaxis=dict(
            title='Average Price ($/MWh)', showgrid=False,
            tickfont=dict(color=FLEXOKI_BASE[800]),
        ),
        annotations=[dict(
            text='<i>data: AEMO, design: ITK</i>',
            xref='paper', yref='paper', x=1.0, y=-0.12,
            showarrow=False, font=dict(size=9, color=FLEXOKI_BASE[500]), xanchor='right',
        )],
    )

    return fig
