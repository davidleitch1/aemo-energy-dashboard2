"""Price time-series and time-of-day charts for the Prices tab.

Extracted from gen_dash.py.
"""

import logging

import holoviews as hv
import hvplot.pandas
import numpy as np
import pandas as pd

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
    attribution_hook,
    flexoki_bg_hook,
):
    """Build the price time-series hvplot overlay.

    Parameters
    ----------
    price_data : DataFrame
        Smoothed price data with ``SETTLEMENTDATE``, ``REGIONID``, *y_col*.
    y_col : str
        Column name for the y-axis (``'RRP'`` or ``'RRP_adjusted'``).
    ylabel : str
    use_log : bool
    date_range_text : str
    start_date, end_date : date-like
    attribution_hook, flexoki_bg_hook : callables
        Bokeh hooks from the dashboard.

    Returns
    -------
    plot : hv.Overlay
    """
    xlim = (
        pd.Timestamp(start_date),
        pd.Timestamp(end_date) + pd.Timedelta(days=1),
    )

    color_list = [
        REGION_COLORS.get(r, '#6F6E69') for r in price_data['REGIONID'].unique()
    ]

    plot = price_data.hvplot.line(
        x='SETTLEMENTDATE', y=y_col, by='REGIONID',
        width=1200, height=400,
        xlabel='Time', ylabel=ylabel,
        title=f'Electricity Spot Prices by Region ({date_range_text})',
        logy=use_log, grid=True,
        color=color_list, line_width=2,
        hover=True, hover_cols=['REGIONID', 'RRP'],
        bgcolor=FLEXOKI_PAPER,
        fontsize={'title': 14, 'labels': 12, 'ticks': 10},
    ).opts(
        xlim=xlim, toolbar='above',
        active_tools=['pan', 'wheel_zoom'],
        tools=['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset', 'save'],
        show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        hooks=[attribution_hook, flexoki_bg_hook],
    )

    return plot


def build_tod_chart(original_price_data, date_range_text, attribution_hook, flexoki_bg_hook):
    """Build the time-of-day average-price line chart.

    Parameters
    ----------
    original_price_data : DataFrame
        Resampled (but unsmoothed) price data.
    date_range_text : str
    attribution_hook, flexoki_bg_hook : callables

    Returns
    -------
    tod_plot : hv.Overlay
    """
    df = original_price_data.copy()
    df['Hour'] = pd.to_datetime(df['SETTLEMENTDATE']).dt.hour

    tod_data = df.groupby(['Hour', 'REGIONID'])['RRP'].mean().reset_index()
    tod_data.rename(columns={'RRP': 'Average Price'}, inplace=True)

    tod_colors = [REGION_COLORS.get(r, '#6F6E69') for r in tod_data['REGIONID'].unique()]

    tod_plot = tod_data.hvplot.line(
        x='Hour', y='Average Price', by='REGIONID',
        width=400, height=400,
        xlabel='Hour of Day', ylabel='Average Price ($/MWh)',
        title=f'Average Price by Hour ({date_range_text})',
        color=tod_colors, bgcolor=FLEXOKI_PAPER,
        legend='top_right',
        xticks=list(range(0, 24, 3)),
        toolbar='above', grid=True,
    ).opts(
        show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        fontsize={'xlabel': 10, 'ylabel': 10, 'ticks': 9},
        hooks=[attribution_hook, flexoki_bg_hook],
    )

    return tod_plot
