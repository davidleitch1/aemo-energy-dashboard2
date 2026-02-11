import pandas as pd
import panel as pn
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time
import numpy as np
import os
import sys
import logging
from datetime import datetime, timedelta

pn.extension()

# Simple cache for parquet data
_data_cache = {'data': None, 'timestamp': None}
_CACHE_TTL_SECONDS = 60  # Cache for 60 seconds

# Add page background CSS for iframe embedding
pn.config.raw_css.append("""
body, html {
    background-color: #FFFCF0 !important;
    overflow: hidden !important;
}
.bk-root, .bk, .pn-loading {
    background-color: #FFFCF0 !important;
}
.markdown, .pn-wrapper, .card, .card-header {
    background-color: #FFFCF0 !important;
}
.mpl-container, .matplotlib {
    background-color: #FFFCF0 !important;
}
div[class*="pn-"] {
    background-color: #FFFCF0 !important;
}
""")

# Production path for 5-minute price data
file_path = "/Users/davidleitch/aemo_production/data/prices5.parquet"
font_size = 10

# Flexoki Light theme colors
FLEXOKI_PAPER = '#FFFCF0'
FLEXOKI_BLACK = '#100F0F'
FLEXOKI_BASE = {
    50: '#F2F0E5',
    100: '#E6E4D9',
    150: '#DAD8CE',
    200: '#CECDC3',
    300: '#B7B5AC',
    600: '#6F6E69',
}
FLEXOKI_ACCENT = {
    'cyan': '#24837B',
    'green': '#66800B',
    'orange': '#BC5215',
    'magenta': '#A02F6F',
    'purple': '#5E409D',
}
# Region colors for price chart lines
REGION_COLORS = {
    'NSW1': FLEXOKI_ACCENT['green'],
    'QLD1': FLEXOKI_ACCENT['orange'],
    'SA1': FLEXOKI_ACCENT['magenta'],
    'TAS1': FLEXOKI_ACCENT['cyan'],
    'VIC1': FLEXOKI_ACCENT['purple'],
}

# DuckDB path (preferred over parquet when set)
DUCKDB_PATH = os.getenv('AEMO_DUCKDB_PATH')

# Function to load price data with caching
def open_parquet_file(path):
    """Load recent price data from DuckDB (preferred) or parquet with caching"""
    global _data_cache

    # Check cache
    now = datetime.now()
    if (_data_cache['data'] is not None and
        _data_cache['timestamp'] is not None and
        (now - _data_cache['timestamp']).total_seconds() < _CACHE_TTL_SECONDS):
        return _data_cache['data']

    try:
        if DUCKDB_PATH:
            import duckdb
            conn = duckdb.connect(DUCKDB_PATH, read_only=True)
            cutoff = now - timedelta(hours=48)
            result = conn.execute(
                "SELECT settlementdate, regionid, rrp FROM prices5 "
                "WHERE settlementdate >= ? ORDER BY settlementdate",
                [cutoff]
            ).df()
            conn.close()

            # Set index to match expected format
            if 'settlementdate' in result.columns:
                result = result.set_index('settlementdate')
            result = result.sort_index()

            logging.info(f"Loaded {len(result)} price records from DuckDB")
        else:
            import pyarrow.parquet as pq

            parquet_file = pq.ParquetFile(path)
            schema = parquet_file.schema_arrow

            if 'settlementdate' in schema.names:
                date_col = 'settlementdate'
            elif 'SETTLEMENTDATE' in schema.names:
                date_col = 'SETTLEMENTDATE'
            else:
                date_col = None

            cutoff = now - timedelta(hours=48)
            if date_col:
                filters = [(date_col, '>=', pd.Timestamp(cutoff))]
                result = pd.read_parquet(path, filters=filters)
            else:
                result = pd.read_parquet(path)

            if not isinstance(result.index, pd.DatetimeIndex):
                if 'SETTLEMENTDATE' in result.columns:
                    result = result.set_index('SETTLEMENTDATE')
                elif 'settlementdate' in result.columns:
                    result = result.set_index('settlementdate')
                else:
                    try:
                        result.index = pd.to_datetime(result.index)
                    except Exception:
                        pass

            result = result.sort_index()

        # Update cache
        _data_cache['data'] = result
        _data_cache['timestamp'] = now

        return result

    except Exception as e:
        logging.error(f"Failed to load price data: {e}")
        raise ValueError(f"Failed to load price data: {e}")

# Flexoki Light style for dataframe
styles = [
    dict(selector="caption",
         props=[("text-align", "left"),
                ("font-size", "150%"),
                ("color", FLEXOKI_PAPER),
                ("background-color", FLEXOKI_ACCENT['cyan']),
                ("caption-side", "top"),
                ("padding", "8px")]),
    dict(selector="",
         props=[("color", FLEXOKI_BLACK),
                ("background-color", FLEXOKI_PAPER),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[150]}")]),
    dict(selector="th",
         props=[("background-color", FLEXOKI_BASE[50]),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[150]}"),
                ("font-size", "14px"),
                ("color", FLEXOKI_BLACK),
                ("padding", "8px")]),
    dict(selector="tr",
         props=[("background-color", FLEXOKI_PAPER),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[100]}"),
                ("color", FLEXOKI_BLACK)]),
    dict(selector="td",
         props=[("font-size", "14px"),
                ("padding", "6px 8px")]),
    dict(selector="th.col_heading",
         props=[("color", FLEXOKI_PAPER),
                ("font-size", "110%"),
                ("background-color", FLEXOKI_ACCENT['cyan'])]),
    dict(selector="tr:last-child",
         props=[("color", FLEXOKI_BLACK),
                ("border-bottom", f"3px solid {FLEXOKI_BASE[200]}")]),
    dict(selector=".row_heading",
         props=[("background-color", FLEXOKI_BASE[50]),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[150]}"),
                ("color", FLEXOKI_BLACK),
                ("font-size", "14px")]),
    dict(selector="thead th:first-child",
         props=[("background-color", FLEXOKI_ACCENT['cyan']),
                ("color", FLEXOKI_PAPER)]),
    dict(selector="tr:hover",
         props=[("background-color", FLEXOKI_BASE[50])]),
]

def display_table(prices):
    display = prices.copy()
    display.index = display.index.strftime('%H:%M')

    display.loc["Last hour average"] = display.tail(12).mean()
    display.loc["Last 24 hr average"] = display.tail(24*12).mean()
    display.rename_axis(None, inplace=True)
    display.rename_axis(None, axis=1, inplace=True)
    return (display.tail(7)
        .style
        .format('{:,.0f}')
        .set_caption("5 minute spot $/MWh " + prices.index[-1].strftime("%d %b %H:%M"))
        .set_table_styles(styles)
        .apply(lambda x: ['font-weight: bold' if x.name in display.tail(3).index else '' for _ in x], axis=1)
        .apply(lambda x: [f'color: {FLEXOKI_BASE[600]}' if x.name not in display.tail(3).index else '' for _ in x], axis=1))

# Flexoki Light style for matplotlib
flexoki_style = {
    "axes.facecolor": FLEXOKI_PAPER,
    "axes.edgecolor": FLEXOKI_BASE[150],
    "axes.labelcolor": FLEXOKI_BLACK,
    "figure.facecolor": FLEXOKI_PAPER,
    "grid.color": FLEXOKI_BASE[100],
    "text.color": FLEXOKI_BLACK,
    "xtick.color": FLEXOKI_BLACK,
    "ytick.color": FLEXOKI_BLACK,
    "axes.prop_cycle": plt.cycler("color", [
        REGION_COLORS['NSW1'],
        REGION_COLORS['QLD1'],
        REGION_COLORS['SA1'],
        REGION_COLORS['TAS1'],
        REGION_COLORS['VIC1'],
    ])
}

def get_data():
    data = open_parquet_file(file_path)

    # Handle both uppercase and lowercase column names
    if 'regionid' in data.columns:
        prices = data.pivot(columns='regionid', values='rrp')
    elif 'REGIONID' in data.columns:
        prices = data.pivot(columns='REGIONID', values='RRP')
    else:
        raise ValueError("Could not find region column in data")

    return prices

def pcht(prices):
    # Count consecutive valid points from the end for each column
    valid_counts = {}
    for col in prices.columns:
        series = prices[col]
        count = 0
        for i in range(len(series)-1, -1, -1):
            if pd.isna(series.iloc[i]):
                break
            count += 1
        valid_counts[col] = count

    min_valid_points = min(valid_counts.values())

    # Take last min(120, min_valid_points) rows
    rows_to_take = min(120, min_valid_points)

    # Calculate EWM only on the rows we'll use
    df1 = prices.tail(rows_to_take).ewm(alpha=0.22, adjust=False).mean()
    df = df1.copy()

    # Apply Flexoki style BEFORE creating figure
    plt.rcParams.update(flexoki_style)

    fig, ax = plt.subplots()
    fig.set_size_inches(4.5, 2.5)

    # Explicitly set figure and axes background colors
    fig.set_facecolor(FLEXOKI_PAPER)
    ax.set_facecolor(FLEXOKI_PAPER)

    when = prices.index[-1].strftime("%d %b %H:%M")

    for col in df.columns:
        line, = ax.plot(df[col])
        last_value = df.at[df.index[-1], col]
        if pd.isna(last_value):
            line.set_label(f"{col}: N/A")
        else:
            line.set_label(f"{col}: ${last_value:.0f}")

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.set_ylabel("$/MWh")

    plt.title(f"Smoothed 5 minute prices as at {when} ({rows_to_take} points)", fontsize=10)

    ax.tick_params(axis='both', which='major', labelsize=7)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.axhline(0, color=FLEXOKI_BASE[300], linewidth=0.3)
    plt.figtext(0.97, 0.0, "©ITK", fontsize=7, horizontalalignment="right")
    ax.legend(fontsize=7)
    legend = ax.legend(fontsize=7, frameon=False)
    plt.close()
    return fig

# Loading indicator HTML
_loading_html = f"""
<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
            height:300px;width:450px;color:{FLEXOKI_BASE[600]};background:{FLEXOKI_PAPER};">
    <div style="width:40px;height:40px;border:3px solid {FLEXOKI_BASE[200]};
                border-top-color:{FLEXOKI_ACCENT['cyan']};border-radius:50%;
                animation:spin 1s linear infinite;"></div>
    <p style="margin-top:15px;font-size:12px;">Loading prices...</p>
</div>
<style>@keyframes spin {{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}</style>
"""

# Create containers that hold loading indicator initially
# These will have their contents replaced after data loads
_chart_container = pn.Column(
    pn.pane.HTML(_loading_html, sizing_mode='fixed', width=450, height=300),
    sizing_mode='fixed', width=450, height=300, margin=(0, 0, 0, 0),
    styles={'background-color': FLEXOKI_PAPER}
)
_table_container = pn.Column(
    pn.pane.HTML(_loading_html, sizing_mode='fixed', width=450, height=310),
    sizing_mode='fixed', width=450, height=310, margin=(0, 0, 0, 0),
    styles={'background-color': FLEXOKI_PAPER}
)

# These will hold the actual panes after loading (for periodic updates)
_mpl_pane = None
_table_pane = None

def update_plot(event=None):
    """Update the plot - called periodically after initial load."""
    global _mpl_pane, _table_pane
    if _mpl_pane is None or _table_pane is None:
        return  # Not yet initialized
    prices = get_data()
    fig = pcht(prices)
    _mpl_pane.object = fig
    _table_pane.object = display_table(prices)

def load_initial_data():
    """Load initial data - called after page loads via onload callback."""
    global _mpl_pane, _table_pane
    logging.info("onload callback triggered - loading price data")
    try:
        prices = get_data()
        fig = pcht(prices)
        table = display_table(prices)

        # Create the actual panes
        _mpl_pane = pn.pane.Matplotlib(fig, sizing_mode='fixed', width=450, height=300,
                                        margin=(0, 0, 0, 0), styles={'background-color': FLEXOKI_PAPER})
        _table_pane = pn.pane.DataFrame(table, sizing_mode='fixed', width=450, height=310,
                                         margin=(0, 0, 0, 0), styles={'background-color': FLEXOKI_PAPER})

        # Replace loading indicators with actual content
        _chart_container[:] = [_mpl_pane]
        _table_container[:] = [_table_pane]

        logging.info("Price data loaded successfully")

        # Set up periodic callback to update the plot every 4.5 minutes
        pn.state.add_periodic_callback(update_plot, 270000)
    except Exception as e:
        logging.error(f"Error loading price data: {e}")
        _chart_container[:] = [pn.pane.HTML(
            f"<div style='padding:20px;color:red;background:{FLEXOKI_PAPER};'>Error: {e}</div>"
        )]

# Schedule data loading after page is served
pn.state.onload(load_initial_data)

# Layout for the Panel with Flexoki background
layout = pn.Column(
    _table_container,
    _chart_container,
    sizing_mode='fixed',
    width=450,
    styles={'background-color': FLEXOKI_PAPER}
)

# Serve the Panel
layout.servable()
