"""
Price components using Plotly for fast chart rendering
"""

import pandas as pd
import panel as pn
import numpy as np
import plotly.graph_objects as go
from pathlib import Path

from ..shared.config import config
from ..shared.logging_config import get_logger
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
    FLEXOKI_TABLE_STYLES,
    REGION_COLORS,
)

logger = get_logger(__name__)

# Use Flexoki Light table styles
PRICE_TABLE_STYLES = FLEXOKI_TABLE_STYLES


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
            return pn.pane.HTML(
                "<div class='responsive-table'>No price data available</div>",
                sizing_mode='stretch_width',
            )

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

        # Convert to HTML with responsive wrapper
        html_table = f"<div class='responsive-table'>{styled_table.to_html()}</div>"

        return pn.pane.HTML(html_table, sizing_mode='stretch_width')

    except Exception as e:
        logger.error(f"Error creating price table: {e}")
        return pn.pane.HTML(f"<div>Error creating price table: {e}</div>", sizing_mode='stretch_width')


def create_price_chart(prices):
    """Create price chart using Plotly"""
    try:
        if prices.empty:
            return pn.pane.HTML("<div>No price data for chart</div>", sizing_mode='stretch_width')

        # Get last 48 hours of data
        last_48h = prices.last('48h')

        fig = go.Figure()

        for region in last_48h.columns:
            color = REGION_COLORS.get(region, FLEXOKI_BASE[600])
            fig.add_trace(go.Scatter(
                x=last_48h.index,
                y=last_48h[region],
                name=region,
                mode='lines',
                line=dict(width=2, color=color),
                hovertemplate=f'{region}: $%{{y:.1f}}/MWh<extra></extra>',
            ))

        fig.update_layout(
            autosize=True,
            height=250,
            paper_bgcolor=FLEXOKI_PAPER,
            plot_bgcolor=FLEXOKI_PAPER,
            title=dict(text='Spot Prices - Last 48 Hours', font=dict(size=14, color=FLEXOKI_BLACK)),
            legend=dict(bgcolor=FLEXOKI_PAPER, font=dict(size=10)),
            margin=dict(l=50, r=20, t=35, b=30),
            xaxis=dict(title='Time', showgrid=False, tickfont=dict(color=FLEXOKI_BASE[800])),
            yaxis=dict(title='Price ($/MWh)', showgrid=False, tickfont=dict(color=FLEXOKI_BASE[800])),
        )

        return pn.pane.Plotly(fig, sizing_mode='stretch_width')

    except Exception as e:
        logger.error(f"Error creating price chart: {e}")
        return pn.pane.HTML(f"<div>Error creating price chart: {e}</div>", sizing_mode='stretch_width')


# Cache for the price section to avoid re-computation
@pn.cache(ttl=60)  # Cache for 60 seconds
def create_cached_price_components():
    """Create price components with caching"""
    prices = load_price_data()
    table = create_price_table(prices)
    chart = create_price_chart(prices)

    return pn.Column(
        table,
        chart,
        sizing_mode='stretch_width',
        margin=(5, 5),
    )


def create_price_section():
    """
    Create the complete price section with table and chart
    """
    def update_price_components():
        return create_cached_price_components()

    return pn.pane.panel(update_price_components)


if __name__ == "__main__":
    # Test the components
    pn.extension('plotly')

    section = create_price_section()
    section.servable()
