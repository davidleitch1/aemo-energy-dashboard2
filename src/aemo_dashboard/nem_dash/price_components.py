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

from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

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


def load_price_data():
    """
    Load price data using the price adapter
    """
    try:
        from ..shared.adapter_selector import load_price_data as load_price_adapter
        
        logger.info("Loading price data using adapter for nem_dash...")
        
        # Load data using the adapter (handles format conversion)
        data = load_price_adapter()
        
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
    if prices.empty:
        return pn.pane.HTML("<div>No price data available</div>", width=550, height=250)
    
    try:
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
        fig, ax = plt.subplots()
        fig.set_size_inches(5.5, 2.5)
        plt.rcParams.update(DRACULA_STYLE)
        
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
        
        return pn.pane.Matplotlib(fig, sizing_mode='fixed', width=550, height=250)
        
    except Exception as e:
        logger.error(f"Error creating price chart: {e}")
        return pn.pane.HTML(f"<div>Error creating price chart: {e}</div>", width=550, height=250)


def create_price_section():
    """
    Create the complete price section with table and chart
    """
    def update_price_components():
        prices = load_price_data()
        table = create_price_table(prices)
        chart = create_price_chart(prices)
        
        return pn.Column(
            table,
            chart,
            sizing_mode='fixed',
            width=550,
            margin=(5, 5)
        )
    
    return pn.pane.panel(update_price_components)


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