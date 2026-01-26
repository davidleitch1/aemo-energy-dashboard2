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
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
    FLEXOKI_TABLE_STYLES,
    FLEXOKI_MATPLOTLIB_STYLE,
    REGION_COLORS,
)

logger = get_logger(__name__)

# Add debug logging for refresh issues
import logging
refresh_logger = logging.getLogger('refresh.price_components')
refresh_logger.setLevel(logging.DEBUG)

# Flexoki Light style for dataframe
PRICE_TABLE_STYLES = FLEXOKI_TABLE_STYLES

# Flexoki style for matplotlib
FLEXOKI_STYLE = FLEXOKI_MATPLOTLIB_STYLE.copy()
FLEXOKI_STYLE["axes.prop_cycle"] = plt.cycler("color", [
    REGION_COLORS['NSW1'],  # green
    REGION_COLORS['QLD1'],  # orange
    REGION_COLORS['SA1'],   # magenta
    REGION_COLORS['TAS1'],  # cyan
    REGION_COLORS['VIC1'],  # purple
])


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
        return pn.pane.HTML("<div>No price data available</div>", width=550, height=350)

    try:
        display = prices.copy()

        # Detect data resolution from timestamps (before converting to string format)
        if len(display) >= 2:
            time_diff = display.index[-1] - display.index[-2]
            periods_per_hour = pd.Timedelta(hours=1) / time_diff
            periods_per_day = int(periods_per_hour * 24)
        else:
            # Fallback for insufficient data
            periods_per_hour = 12  # Assume 5-min
            periods_per_day = 288

        # Calculate averages with dynamic periods
        # Handle edge cases: not enough data for full periods
        hour_periods = min(int(periods_per_hour), len(display))
        day_periods = min(periods_per_day, len(display))

        # Convert index to time strings for display
        display.index = display.index.strftime('%H:%M')

        # Calculate averages using detected resolution
        if hour_periods > 0:
            display.loc["Last hour average"] = display.tail(hour_periods).mean()
        if day_periods > 0:
            display.loc["Last 24 hr average"] = display.tail(day_periods).mean()

        display.rename_axis(None, inplace=True)
        display.rename_axis(None, axis=1, inplace=True)
        
        # Create styled table - show last 9 rows (7 prices + 2 averages) to better fill the height
        styled_table = (display.tail(9)
            .style
            .format('{:,.0f}')
            .set_caption("5 minute spot $/MWh " + prices.index[-1].strftime("%d %b %H:%M"))
            .set_table_styles(PRICE_TABLE_STYLES)
            .apply(lambda x: ['font-weight: bold' if x.name in display.tail(3).index else '' for _ in x], axis=1)
            .apply(lambda x: [f'color: {FLEXOKI_BASE[600]}' if x.name not in display.tail(3).index else '' for _ in x], axis=1))
        
        return pn.pane.DataFrame(styled_table, sizing_mode='fixed', width=550, height=350)
        
    except Exception as e:
        logger.error(f"Error creating price table: {e}")
        return pn.pane.HTML(f"<div>Error creating price table: {e}</div>", width=550, height=350)


def create_price_chart(prices):
    """
    Create smoothed price chart
    Adapted from display_spot.py pcht function
    """
    refresh_logger.debug(f"create_price_chart called with {len(prices) if not prices.empty else 0} price records")
    
    if prices.empty:
        return pn.pane.HTML("<div>No price data available</div>", width=550, height=400)
    
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
            return pn.pane.HTML("<div>No valid price data points</div>", width=550, height=400)
        
        # Calculate EWM only on the rows we'll use
        df = prices.tail(rows_to_take).ewm(alpha=0.22, adjust=False).mean()

        # Apply Flexoki style BEFORE creating figure
        refresh_logger.debug("Applying Flexoki style...")
        plt.rcParams.update(FLEXOKI_STYLE)

        # Create matplotlib figure
        refresh_logger.debug("Creating matplotlib figure...")
        fig, ax = plt.subplots()
        fig.set_size_inches(5.5, 4.0)  # Increased height to match generation chart

        # EXPLICIT FIX: Force background colors to Flexoki cream
        fig.patch.set_facecolor(FLEXOKI_PAPER)
        ax.set_facecolor(FLEXOKI_PAPER)

        refresh_logger.debug("Matplotlib figure created successfully")
        
        # Show the timestamp of the last data point
        # The real issue is data not updating after midnight, not the display
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
        ax.axhline(0, color=FLEXOKI_BASE[300], linewidth=0.3)
        plt.figtext(0.97, 0.0, "©ITK", fontsize=7, horizontalalignment="right")

        # Create legend with explicit Flexoki cream background
        legend = ax.legend(fontsize=7, frameon=True, facecolor=FLEXOKI_PAPER,
                          edgecolor=FLEXOKI_BASE[150], framealpha=1.0)
        
        plt.tight_layout()
        refresh_logger.debug("Matplotlib chart completed, creating Panel pane...")
        
        pane = pn.pane.Matplotlib(fig, sizing_mode='fixed', width=550, height=400)
        refresh_logger.debug(f"Panel Matplotlib pane created: {type(pane)}")
        
        return pane
        
    except Exception as e:
        logger.error(f"Error creating price chart: {e}")
        return pn.pane.HTML(f"<div>Error creating price chart: {e}</div>", width=550, height=400)


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
    
    # FIX for midnight rollover bug: Create and return components directly
    # without pn.pane.panel wrapper so they can be refreshed via object property
    start_time = time.time()
    
    try:
        refresh_logger.debug("Loading price data with date filtering...")
        prices = load_price_data(start_date, end_date)
        
        refresh_logger.debug(f"Creating price table with {len(prices) if not prices.empty else 0} records...")
        table = create_price_table(prices)
        
        refresh_logger.debug("Creating price chart...")
        chart = create_price_chart(prices)
        
        refresh_logger.debug(f"Components created in {time.time() - start_time:.2f}s")
        
        result = pn.Column(
            table,
            chart,
            sizing_mode='fixed',
            width=550,
            margin=(5, 5)
        )
        
        refresh_logger.debug(f"create_price_section returning: {type(result)}")
        return result
        
    except Exception as e:
        refresh_logger.error(f"Error in create_price_section: {e}", exc_info=True)
        raise


def create_price_chart_component(start_date=None, end_date=None):
    """
    Create just the price chart component (for separated layout)
    
    Args:
        start_date: Start date for price data
        end_date: End date for price data
    """
    # FIX for midnight rollover bug: Return pane directly without pn.pane.panel wrapper
    # This allows the component to be refreshed via the object property
    prices = load_price_data(start_date, end_date)
    return create_price_chart(prices)


def create_price_table_component(start_date=None, end_date=None):
    """
    Create just the price table component (for separated layout)
    
    Args:
        start_date: Start date for price data
        end_date: End date for price data
    """
    # FIX for midnight rollover bug: Return pane directly without pn.pane.panel wrapper
    # This allows the component to be refreshed via the object property
    prices = load_price_data(start_date, end_date)
    return create_price_table(prices)


class PriceDisplay:
    """
    Persistent price display components that update via object properties.
    This pattern fixes the midnight rollover bug by maintaining persistent panes
    and updating their content rather than replacing components.
    
    Based on the working pattern from display_spot.py
    """
    
    def __init__(self):
        """Initialize with persistent panes"""
        refresh_logger.info("PriceDisplay: Creating persistent panes")
        
        # Load initial data (last 48 hours by default)
        initial_prices = load_price_data()
        
        # Create panes ONCE - these will persist throughout the session
        if not initial_prices.empty:
            self.chart_pane = create_price_chart(initial_prices)
            self.table_pane = create_price_table(initial_prices)
            refresh_logger.info(f"PriceDisplay: Created panes with {len(initial_prices)} price records")
        else:
            # Fallback if no data available
            self.chart_pane = pn.pane.HTML("<div>No price data available</div>", width=550, height=400)
            self.table_pane = pn.pane.HTML("<div>No price data available</div>", width=550, height=350)
            refresh_logger.warning("PriceDisplay: No initial price data available")
        
        # Store last update info for debugging
        self.last_update_time = pd.Timestamp.now()
        self.last_date_range = (None, None)
    
    def update(self, start_date=None, end_date=None):
        """
        Update the price displays by replacing the object property.
        This is the key pattern that fixes the midnight rollover bug.
        
        Args:
            start_date: Start date for price data
            end_date: End date for price data
        """
        update_start = time.time()
        refresh_logger.info(f"PriceDisplay.update called with dates: {start_date} to {end_date}")
        
        # Load fresh data
        prices = load_price_data(start_date, end_date)
        
        if not prices.empty:
            # Create new chart and table with fresh data
            new_chart = create_price_chart(prices)
            new_table = create_price_table(prices)
            
            # UPDATE OBJECT PROPERTIES - This is the critical fix!
            # We update the content of existing panes rather than replacing them
            if hasattr(new_chart, 'object'):
                self.chart_pane.object = new_chart.object
                refresh_logger.debug("PriceDisplay: Updated chart_pane.object")
            else:
                # If new_chart is already a figure, assign directly
                self.chart_pane.object = new_chart
                refresh_logger.debug("PriceDisplay: Updated chart_pane.object (direct)")
            
            if hasattr(new_table, 'object'):
                self.table_pane.object = new_table.object
                refresh_logger.debug("PriceDisplay: Updated table_pane.object")
            else:
                # If new_table is already styled data, assign directly
                self.table_pane.object = new_table
                refresh_logger.debug("PriceDisplay: Updated table_pane.object (direct)")
            
            # Log successful update
            self.last_update_time = pd.Timestamp.now()
            self.last_date_range = (start_date, end_date)
            
            # Check for midnight rollover
            if start_date and end_date:
                refresh_logger.info(f"PriceDisplay: Updated with {len(prices)} records for {start_date} to {end_date}")
                if hasattr(self, '_last_end_date') and self._last_end_date:
                    if self._last_end_date.date() != end_date.date() if hasattr(end_date, 'date') else end_date != self._last_end_date:
                        refresh_logger.info(f"PriceDisplay: MIDNIGHT ROLLOVER DETECTED - Date changed from {self._last_end_date} to {end_date}")
                self._last_end_date = end_date
            
            refresh_logger.info(f"PriceDisplay: Update completed in {time.time() - update_start:.2f}s")
        else:
            refresh_logger.warning("PriceDisplay: No price data available for update")
            # Update with empty message
            self.chart_pane.object = pn.pane.HTML("<div>No price data available</div>", width=550, height=400).object
            self.table_pane.object = pn.pane.HTML("<div>No price data available</div>", width=550, height=350).object
    
    def get_chart(self):
        """Get the persistent chart pane"""
        return self.chart_pane
    
    def get_table(self):
        """Get the persistent table pane"""
        return self.table_pane
    
    def get_status(self):
        """Get update status for debugging"""
        return {
            'last_update': self.last_update_time.strftime('%Y-%m-%d %H:%M:%S'),
            'date_range': self.last_date_range,
            'chart_type': type(self.chart_pane).__name__,
            'table_type': type(self.table_pane).__name__
        }


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