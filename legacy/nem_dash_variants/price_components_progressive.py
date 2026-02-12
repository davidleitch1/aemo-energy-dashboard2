"""
Price components with progressive loading (no defer_load)
"""

import pandas as pd
import panel as pn
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import threading
from pathlib import Path

from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

# ITK teal style for dataframe
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
         props=[("color", "#f8f8f2")])
]


def load_price_data():
    """Load price data using the price adapter"""
    try:
        from ..shared.adapter_selector import load_price_data as load_price_adapter
        
        logger.info("Loading price data using adapter for nem_dash...")
        
        # Load data using the adapter
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
    """Create formatted price table"""
    try:
        if prices.empty:
            return pn.pane.HTML("<div>No price data available</div>", width=550)
        
        # Get latest prices
        current_prices = prices.iloc[-1].round(2)
        
        # Create previous period data (5 minutes ago)
        if len(prices) > 1:
            previous_prices = prices.iloc[-2].round(2)
            price_changes = (current_prices - previous_prices).round(2)
        else:
            previous_prices = current_prices
            price_changes = pd.Series(0, index=current_prices.index)
        
        # Create DataFrame for display
        price_df = pd.DataFrame({
            'Current Price ($/MWh)': current_prices,
            'Previous ($/MWh)': previous_prices,
            'Change ($/MWh)': price_changes
        })
        
        # Sort by current price descending
        price_df = price_df.sort_values('Current Price ($/MWh)', ascending=False)
        
        # Create styled table
        styled_table = price_df.style.set_table_styles(PRICE_TABLE_STYLES)
        styled_table = styled_table.set_caption(f"Current Spot Prices - {prices.index[-1].strftime('%H:%M')}")
        
        # Apply conditional formatting to change column
        def color_negative_red(val):
            if isinstance(val, (int, float)):
                color = 'red' if val < 0 else 'green' if val > 0 else 'white'
                return f'color: {color}'
            return ''
        
        styled_table = styled_table.map(color_negative_red, subset=['Change ($/MWh)'])
        
        # Convert to HTML
        html_table = styled_table.to_html()
        
        return pn.pane.HTML(html_table, width=550, sizing_mode='fixed')
        
    except Exception as e:
        logger.error(f"Error creating price table: {e}")
        return pn.pane.HTML(f"<div>Error creating price table: {e}</div>", width=550)


def create_price_chart(prices):
    """Create price chart"""
    try:
        if prices.empty:
            return pn.pane.HTML("<div>No price data for chart</div>", width=550, height=250)
        
        # Create matplotlib figure
        fig, ax = plt.subplots(figsize=(10, 5))
        
        # Plot last 48 hours of data
        last_48h = prices.last('48h')
        
        # Plot each region
        for region in last_48h.columns:
            ax.plot(last_48h.index, last_48h[region], label=region, linewidth=2)
        
        # Formatting
        ax.set_xlabel('Time')
        ax.set_ylabel('Price ($/MWh)')
        ax.set_title('Spot Prices - Last 48 Hours')
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        plt.xticks(rotation=45)
        
        # Set dark theme
        fig.patch.set_facecolor('#282a36')
        ax.set_facecolor('#282a36')
        ax.tick_params(colors='white')
        ax.xaxis.label.set_color('white')
        ax.yaxis.label.set_color('white')
        ax.title.set_color('white')
        
        # Adjust layout
        plt.tight_layout()
        
        # Convert to Panel
        return pn.pane.Matplotlib(fig, width=550, height=250, sizing_mode='fixed')
        
    except Exception as e:
        logger.error(f"Error creating price chart: {e}")
        return pn.pane.HTML(f"<div>Error creating price chart: {e}</div>", width=550, height=250)


def create_price_section():
    """
    Create the complete price section with progressive loading (no defer_load)
    """
    # Create container with loading indicator
    container = pn.Column(
        pn.Row(
            pn.indicators.LoadingSpinner(value=True, size=30),
            pn.pane.HTML("<h4>Loading price data...</h4>"),
            align='center'
        ),
        sizing_mode='fixed',
        width=550,
        height=400,
        margin=(5, 5)
    )
    
    def load_content():
        """Load content in background thread"""
        try:
            # Load data
            prices = load_price_data()
            
            # Create components
            table = create_price_table(prices)
            chart = create_price_chart(prices)
            
            # Update container in main thread
            def update_ui():
                container[:] = [table, chart]
            
            # Use pn.state.execute to update UI from thread
            if hasattr(pn.state, 'execute'):
                pn.state.execute(update_ui)
            else:
                # Fallback for when not in server context
                container[:] = [table, chart]
                
        except Exception as e:
            logger.error(f"Error loading price content: {e}")
            error_msg = pn.pane.HTML(
                f"<div style='color:red;'>Error loading prices: {e}</div>",
                width=550
            )
            if hasattr(pn.state, 'execute'):
                pn.state.execute(lambda: container.append(error_msg))
            else:
                container.append(error_msg)
    
    # Start loading in background
    thread = threading.Thread(target=load_content, daemon=True)
    thread.start()
    
    return container


if __name__ == "__main__":
    # Test the components
    pn.extension()
    
    section = create_price_section()
    section.servable()