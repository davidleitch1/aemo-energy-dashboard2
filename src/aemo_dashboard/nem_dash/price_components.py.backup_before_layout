"""
Price components for Nem-dash tab
Adapted from aemo-spot-dashboard display_spot.py
"""

import pandas as pd
import panel as pn
import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend for matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from pathlib import Path
import time
import threading

from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

# Add debug logging for refresh issues
import logging
refresh_logger = logging.getLogger('refresh.price_components')
refresh_logger.setLevel(logging.DEBUG)

# ITK teal style for dataframe (from original)
PRICE_TABLE_STYLES = [
    dict(selector="caption",
         props=[("text-align", "left"),
                ("font-size", "150%"),
                ("color", 'white'),
                ("background-color", "teal"),
                ("caption-side", "top")]),
    dict(selector="",
         props=[("color", "#f8f8f2"),
                ("background-color", "#282a36"),
                ("border-bottom", "1px dotted #6272a4")]),
    dict(selector="th",
         props=[("background-color", "#44475a"),
                ("border-bottom", "1px dotted #6272a4"),
                ("font-size", "14px"),
                ("color", "#f8f8f2")]),
    dict(selector="tr",
         props=[("background-color", "#282a36"),
                ("border-bottom", "1px dotted #6272a4"),
                ("color", "#f8f8f2")]),
    dict(selector="td",
         props=[("font-size", "14px")]),
    dict(selector="th.col_heading",
         props=[("color", "black"),
                ("font-size", "110%"),
                ("background-color", "#00DCDC")]),
    dict(selector="tr:last-child",
         props=[("color", "#f8f8f2"),
                ("border-bottom", "5px solid #6272a4")]),
    dict(selector=".row_heading",
         props=[("background-color", "#282a36"),
                ("border-bottom", "1px dotted #6272a4"),
                ("color", "#f8f8f2"),
                ("font-size", "14px")]),
    dict(selector="thead th:first-child",
         props=[("background-color", "#00DCDC"),
                ("color", "black")])
]

# Dracula style for matplotlib (from original)
DRACULA_STYLE = {
    "axes.facecolor": "#282a36",
    "axes.edgecolor": "#44475a",
    "axes.labelcolor": "#f8f8f2",
    "figure.facecolor": "#282a36",
    "grid.color": "#6272a4",
    "text.color": "#f8f8f2",
    "xtick.color": "#f8f8f2",
    "ytick.color": "#f8f8f2",
    "axes.prop_cycle": plt.cycler("color", ["#8be9fd", "#ff79c6", "#50fa7b", "#ffb86c", "#bd93f9", "#ff5555", "#f1fa8c"])
}


def load_price_data(start_date=None, end_date=None):
    """
    Load price data using the price adapter
    
    Args:
        start_date: Start date for price data (defaults to 48 hours ago)
        end_date: End date for price data (defaults to now)
    """
    refresh_logger.debug(f"load_price_data called at {time.time()} with dates: {start_date} to {end_date}")
    
    # If no dates provided, default to last 48 hours to prevent loading all data
    if start_date is None or end_date is None:
        end_date = pd.Timestamp.now()
        start_date = end_date - pd.Timedelta(hours=48)
        refresh_logger.info(f"No dates provided, using default range: {start_date} to {end_date}")
    else:
        # Convert date objects to pandas Timestamps if needed
        refresh_logger.debug(f"Date types received: start_date={type(start_date)}, end_date={type(end_date)}")
        if hasattr(start_date, 'date'):  # It's already a datetime/Timestamp
            pass
        else:  # It's likely a date object, convert to Timestamp
            start_date = pd.Timestamp(start_date)
            end_date = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)  # End of day
        refresh_logger.info(f"Using provided dates (converted): {start_date} to {end_date}")
    
    try:
        from ..shared.adapter_selector import load_price_data as load_price_adapter
        
        logger.info(f"Loading price data for {start_date} to {end_date}")
        refresh_logger.debug("About to call load_price_adapter with date filtering...")
        
        # Load data using the adapter with date filtering
        data = load_price_adapter(start_date=start_date, end_date=end_date)
        
        if data.empty:
            logger.error("No price data returned from adapter")
            return pd.DataFrame()
        
        # Reset index to get SETTLEMENTDATE as a column
        if data.index.name == 'SETTLEMENTDATE':
            data = data.reset_index()
        
        # Convert to the expected format (pivot table)
        if 'REGIONID' in data.columns and 'RRP' in data.columns and 'SETTLEMENTDATE' in data.columns:
            data['SETTLEMENTDATE'] = pd.to_datetime(data['SETTLEMENTDATE'])
            data = data.set_index('SETTLEMENTDATE')
            prices = data.pivot(columns='REGIONID', values='RRP')
            logger.info(f"Loaded price data: {len(prices)} records, {len(prices.columns)} regions")
            return prices
        else:
            logger.error(f"Expected columns not found. Available: {list(data.columns)}")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error loading price data: {e}")
        return pd.DataFrame()


def create_price_table(prices):
    """
    Create styled price table showing recent prices and averages
    Adapted from display_spot.py display_table function
    """
    if prices.empty:
        return pn.pane.HTML("<div>No price data available</div>", width=550, height=300)
    
    try:
        display = prices.copy()
        display.index = display.index.strftime('%H:%M')

        # Calculate averages
        display.loc["Last hour average"] = display.tail(12).mean()
        display.loc["Last 24 hr average"] = display.tail(24*12).mean()
        display.rename_axis(None, inplace=True)
        display.rename_axis(None, axis=1, inplace=True)
        
        # Create styled table
        styled_table = (display.tail(7)
            .style
            .format('{:,.0f}')
            .set_caption("5 minute spot $/MWh " + prices.index[-1].strftime("%d %b %H:%M"))
            .set_table_styles(PRICE_TABLE_STYLES)
            .apply(lambda x: ['font-weight: bold' if x.name in display.tail(3).index else '' for _ in x], axis=1)
            .apply(lambda x: ['color: #e0e0e0' if x.name not in display.tail(3).index else '' for _ in x], axis=1))
        
        return pn.pane.DataFrame(styled_table, sizing_mode='fixed', width=550, height=300)
        
    except Exception as e:
        logger.error(f"Error creating price table: {e}")
        return pn.pane.HTML(f"<div>Error creating price table: {e}</div>", width=550, height=300)


def create_price_chart(prices):
    """
    Create smoothed price chart
    Adapted from display_spot.py pcht function
    """
    refresh_logger.debug(f"create_price_chart called with {len(prices) if not prices.empty else 0} price records")
    
    if prices.empty:
        return pn.pane.HTML("<div>No price data available</div>", width=550, height=250)
    
    try:
        refresh_logger.debug("Starting matplotlib figure creation...")
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
        
        min_valid_points = min(valid_counts.values()) if valid_counts else 0
        
        # Take last min(120, min_valid_points) rows
        rows_to_take = min(120, min_valid_points) if min_valid_points > 0 else 120
        rows_to_take = min(rows_to_take, len(prices))
        
        if rows_to_take == 0:
            return pn.pane.HTML("<div>No valid price data points</div>", width=550, height=250)
        
        # Calculate EWM only on the rows we'll use
        df = prices.tail(rows_to_take).ewm(alpha=0.22, adjust=False).mean()
        
        # Create matplotlib figure
        refresh_logger.debug("Creating matplotlib figure...")
        fig, ax = plt.subplots()
        fig.set_size_inches(5.5, 2.5)
        plt.rcParams.update(DRACULA_STYLE)
        refresh_logger.debug("Matplotlib figure created successfully")
        
        when = prices.index[-1].strftime("%d %b %H:%M")
        
        # Plot each region
        for col in df.columns:
            if col in df.columns:
                line, = ax.plot(df[col])
                last_value = df.at[df.index[-1], col]
                if pd.isna(last_value):
                    line.set_label(f"{col}: N/A")
                else:
                    line.set_label(f"{col}: ${last_value:.0f}")
        
        # Format axes
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.set_ylabel("$/MWh")
        
        plt.title(f"Smoothed 5 minute prices as at {when} ({rows_to_take} points)", fontsize=10)
        
        ax.tick_params(axis='both', which='major', labelsize=7)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.axhline(0, color='white', linewidth=0.3)
        plt.figtext(0.97, 0.0, "©ITK", fontsize=7, horizontalalignment="right")
        ax.legend(fontsize=7, frameon=False)
        
        plt.tight_layout()
        refresh_logger.debug("Matplotlib chart completed, creating Panel pane...")
        
        pane = pn.pane.Matplotlib(fig, sizing_mode='fixed', width=550, height=250)
        refresh_logger.debug(f"Panel Matplotlib pane created: {type(pane)}")
        
        return pane
        
    except Exception as e:
        logger.error(f"Error creating price chart: {e}")
        return pn.pane.HTML(f"<div>Error creating price chart: {e}</div>", width=550, height=250)


def create_price_section(start_date=None, end_date=None):
    """
    Create the complete price section with table and chart
    
    Args:
        start_date: Start date for price data
        end_date: End date for price data
    """
    refresh_logger.info("="*60)
    refresh_logger.info(f"create_price_section called at {time.time()}")
    refresh_logger.info(f"Date range: {start_date} to {end_date}")
    refresh_logger.info(f"Thread: {threading.current_thread().name}")
    refresh_logger.info(f"Panel state: {hasattr(pn.state, 'curdoc')}")
    refresh_logger.info("="*60)
    
    def update_price_components():
        refresh_logger.debug(f"update_price_components called with dates: {start_date} to {end_date}")
        start_time = time.time()
        
        try:
            refresh_logger.debug("Loading price data with date filtering...")
            prices = load_price_data(start_date, end_date)
            
            refresh_logger.debug(f"Creating price table with {len(prices) if not prices.empty else 0} records...")
            table = create_price_table(prices)
            
            refresh_logger.debug("Creating price chart...")
            chart = create_price_chart(prices)
            
            refresh_logger.debug(f"Components created in {time.time() - start_time:.2f}s")
            
            return pn.Column(
                table,
                chart,
                sizing_mode='fixed',
                width=550,
                margin=(5, 5)
            )
        except Exception as e:
            refresh_logger.error(f"Error in update_price_components: {e}", exc_info=True)
            raise
    
    refresh_logger.debug("Wrapping with pn.pane.panel...")
    result = pn.pane.panel(update_price_components)
    refresh_logger.debug(f"create_price_section returning: {type(result)}")
    
    return result


if __name__ == "__main__":
    # Test the components
    pn.extension()
    
    prices = load_price_data()
    if not prices.empty:
        table = create_price_table(prices)
        chart = create_price_chart(prices)
        
        layout = pn.Column(
            pn.pane.Markdown("## Price Components Test"),
            table,
            chart
        )
        layout.show()
    else:
        pn.pane.HTML("No price data available for testing").show()