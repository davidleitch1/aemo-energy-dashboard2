"""
24-hour Generation Overview Component for Nem-dash tab
Fixed 24-hour stacked area chart showing NEM generation by fuel type
"""

import pandas as pd
import numpy as np
import panel as pn
import holoviews as hv
import hvplot.pandas
from datetime import datetime, timedelta

from ..shared.config import config
from ..shared.logging_config import get_logger
from ..shared.resolution_utils import (
    detect_resolution_minutes,
    periods_for_hours,
    get_decay_rate_per_period
)
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
)


def set_flexoki_background(plot, element):
    """
    Hook to set Flexoki cream background for hvplot/Bokeh charts.
    Sets plot area background, outline, and legend background.
    """
    # Set plot area background to Flexoki cream
    plot.state.background_fill_color = FLEXOKI_PAPER
    plot.state.border_fill_color = FLEXOKI_PAPER
    plot.state.outline_line_color = FLEXOKI_BASE[150]

    # Set legend background if legend exists
    if plot.state.legend:
        for legend in plot.state.legend:
            legend.background_fill_color = FLEXOKI_PAPER
            legend.border_line_color = FLEXOKI_BASE[150]
            legend.background_fill_alpha = 1.0
from .nem_dash_query_manager import NEMDashQueryManager

logger = get_logger(__name__)

# Initialize query manager
query_manager = NEMDashQueryManager()

# Configure HoloViews (ensure it's set up)
hv.extension('bokeh')

# Fuel colors - Flexoki-compatible, visible on light backgrounds
FUEL_COLORS = {
    'Solar': '#D4A000',           # Darkened gold for visibility on light bg
    'Rooftop Solar': '#E8C547',   # Lighter yellow-gold, more yellow than Solar
    'Wind': FLEXOKI_ACCENT['green'],  # Flexoki green #66800B
    'Water': FLEXOKI_ACCENT['cyan'],  # Flexoki cyan #24837B
    'Battery Storage': FLEXOKI_ACCENT['purple'],  # Flexoki purple #5E409D
    'Battery': FLEXOKI_ACCENT['purple'],  # Same as Battery Storage
    'Coal': '#6B3A10',            # Darker brown for visibility
    'Gas other': FLEXOKI_ACCENT['orange'],  # Flexoki orange #BC5215
    'OCGT': '#E05830',            # Lighter reddish orange
    'CCGT': '#8A2E0D',            # Darker deep orange/brown
    'Biomass': '#4A7C23',         # Medium forest green
    'Other': FLEXOKI_BASE[600],   # Flexoki gray
    'Transmission Flow': FLEXOKI_ACCENT['magenta']  # Flexoki magenta #A02F6F
}

# HoloViews options for consistent styling
hv.opts.defaults(
    hv.opts.Area(
        width=1200,
        height=400,
        alpha=0.8,
        show_grid=False,
        toolbar='above'
    ),
    hv.opts.Overlay(
        show_grid=False,
        toolbar='above'
    )
)


def load_generation_data():
    """
    Load generation data for the last 24 hours using query manager
    """
    try:
        logger.info("Loading generation data using query manager...")
        
        # Get 24 hours of generation data
        gen_data = query_manager.get_generation_overview(hours=24)
        
        if gen_data.empty:
            logger.error("No generation data returned from query manager")
            return pd.DataFrame()
        
        logger.info(f"Loaded generation data shape: {gen_data.shape}")
        logger.info(f"Generation data columns: {list(gen_data.columns)}")
        logger.info(f"Date range: {gen_data.index.min()} to {gen_data.index.max()}")
        
        return gen_data
        
    except Exception as e:
        logger.error(f"Error loading generation data: {e}")
        return pd.DataFrame()


def load_transmission_data():
    """
    Load transmission data for the last 24 hours using query manager
    """
    try:
        logger.info("Loading transmission data using query manager...")
        
        # Get 24 hours of transmission data
        transmission_data = query_manager.get_transmission_flows(hours=24)
        
        if transmission_data.empty:
            logger.warning("No transmission data returned from query manager")
            return pd.DataFrame()
        
        # Ensure proper datetime index
        if 'SETTLEMENTDATE' in transmission_data.columns:
            transmission_data['SETTLEMENTDATE'] = pd.to_datetime(transmission_data['SETTLEMENTDATE'])
            transmission_data = transmission_data.set_index('SETTLEMENTDATE')
        elif not isinstance(transmission_data.index, pd.DatetimeIndex):
            transmission_data.index = pd.to_datetime(transmission_data.index)
        
        logger.info(f"Loaded {len(transmission_data)} transmission records for last 24 hours")
        return transmission_data
        
    except Exception as e:
        logger.error(f"Error loading transmission data: {e}")
        return pd.DataFrame()



def load_rooftop_solar_data():
    """
    Load rooftop solar data for the last 24 hours using the rooftop adapter
    """
    try:
        from ..shared.rooftop_adapter import load_rooftop_data as load_rooftop_adapter
        
        logger.info("Loading rooftop solar data using adapter...")
        
        # Load data using the adapter (handles conversion automatically)
        rooftop_data = load_rooftop_adapter()
        
        if rooftop_data.empty:
            logger.warning("No rooftop solar data available")
            return pd.DataFrame()
        
        # Convert to datetime index for filtering
        rooftop_data['settlementdate'] = pd.to_datetime(rooftop_data['settlementdate'])
        rooftop_data = rooftop_data.set_index('settlementdate')
        
        # Filter for last 24 hours
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        filtered_rooftop = rooftop_data[
            (rooftop_data.index >= start_time) & (rooftop_data.index <= end_time)
        ]
        
        logger.info(f"Loaded {len(filtered_rooftop)} rooftop solar records for last 24 hours")
        return filtered_rooftop
        
    except Exception as e:
        logger.error(f"Error loading rooftop solar data: {e}")
        return pd.DataFrame()


def prepare_generation_for_stacking(gen_data, transmission_data=None, rooftop_data=None):
    """
    Prepare generation data for stacked area chart
    Mirrors the logic from the main dashboard
    """
    try:
        if gen_data.empty:
            logger.warning("No generation data to prepare")
            return pd.DataFrame()
        
        # Check available columns and adapt
        logger.info(f"Generation data columns: {list(gen_data.columns)}")
        
        # Check if this is already processed data (from dashboard) or raw data
        fuel_columns = ['Solar', 'Wind', 'Water', 'Coal', 'Gas other', 'CCGT', 'OCGT', 'Battery Storage', 'Biomass']
        has_fuel_columns = any(col in gen_data.columns for col in fuel_columns)
        
        if has_fuel_columns:
            # This is already processed/pivoted data from dashboard - just rename index
            logger.info("Data is already processed by fuel type - using directly")
            pivot_df = gen_data.copy()
            
            # Ensure we have the time column as index
            if 'settlementdate' in pivot_df.columns:
                pivot_df = pivot_df.set_index('settlementdate')
            elif 'SETTLEMENTDATE' in pivot_df.columns:
                pivot_df = pivot_df.set_index('SETTLEMENTDATE')
            
            # Rename index for hvplot
            pivot_df.index.name = 'settlementdate'
            
            # Keep only fuel columns (remove any extra columns)
            # Include all possible fuel columns including Transmission Flow
            all_fuel_columns = fuel_columns + ['Rooftop Solar', 'Other', 'Transmission Flow', 'Transmission Exports']
            fuel_cols_present = [col for col in pivot_df.columns if col in all_fuel_columns]
            pivot_df = pivot_df[fuel_cols_present]
            
        elif 'FUEL_CAT' in gen_data.columns and 'MW' in gen_data.columns:
            # Raw format - use FUEL_CAT and MW
            logger.info("Raw data format - grouping by FUEL_CAT")
            pivot_df = gen_data.groupby(['SETTLEMENTDATE', 'FUEL_CAT'])['MW'].sum().unstack(fill_value=0)
        elif 'fuel' in gen_data.columns and 'scadavalue' in gen_data.columns:
            # Alternative raw format - use fuel and scadavalue
            logger.info("Raw data format - grouping by fuel")
            pivot_df = gen_data.groupby(['SETTLEMENTDATE', 'fuel'])['scadavalue'].sum().unstack(fill_value=0)
        else:
            logger.error(f"Cannot find suitable columns for grouping. Available: {list(gen_data.columns)}")
            return pd.DataFrame()
        
        # Ensure index is datetime
        if not isinstance(pivot_df.index, pd.DatetimeIndex):
            pivot_df.index = pd.to_datetime(pivot_df.index)
        
        # Rename index for hvplot
        pivot_df.index.name = 'settlementdate'

        # Data is already filtered to the correct time range by the dashboard
        # Do not filter again to avoid data truncation
        if len(pivot_df) > 0:
            logger.info(f"Data range: {len(pivot_df)} records from {pivot_df.index.min()} to {pivot_df.index.max()}")
        
        # Add transmission flows if available
        if transmission_data is not None and not transmission_data.empty:
            try:
                # Calculate net transmission flows (simplified version)
                # Group by settlement date and calculate net flow for NEM
                transmission_grouped = transmission_data.groupby('SETTLEMENTDATE')['METEREDMWFLOW'].sum()
                
                # Align with generation data index
                net_flows = pd.DataFrame({
                    'net_transmission_mw': transmission_grouped
                }, index=transmission_grouped.index)
                
                # Reindex to match generation data
                if len(pivot_df) > 0:
                    transmission_series = net_flows.reindex(pivot_df.index, fill_value=0)['net_transmission_mw']
                    transmission_values = pd.to_numeric(transmission_series, errors='coerce').fillna(0)
                    
                    # Add transmission imports (positive values only)
                    pivot_df['Transmission Flow'] = pd.Series(
                        np.where(transmission_values.values > 0, transmission_values.values, 0),
                        index=transmission_values.index
                    )
                    
                    logger.info(f"Added transmission flows: max {pivot_df['Transmission Flow'].max():.1f}MW")
                
            except Exception as e:
                logger.error(f"Error adding transmission flows: {e}")
        
        # Add rooftop solar if available
        if rooftop_data is not None and not rooftop_data.empty:
            try:
                # Assume rooftop data has NEM total or sum regions
                if 'NEM' in rooftop_data.columns:
                    rooftop_series = rooftop_data['NEM']
                else:
                    # Sum all regions
                    rooftop_series = rooftop_data.sum(axis=1)
                
                # Align with generation data
                if len(pivot_df) > 0:
                    rooftop_aligned = rooftop_series.reindex(pivot_df.index)

                    # Detect resolution dynamically
                    resolution_minutes = detect_resolution_minutes(pivot_df.index)

                    # Forward-fill missing values at the end (up to 2 hours)
                    # This handles the case where rooftop data is less recent than generation data
                    ffill_limit = periods_for_hours(2, resolution_minutes)  # Dynamic: 2 hours worth of periods
                    rooftop_aligned = rooftop_aligned.fillna(method='ffill', limit=ffill_limit)

                    # Apply gentle decay for extended forward-fill periods
                    last_valid_idx = rooftop_aligned.last_valid_index()
                    if last_valid_idx is not None and last_valid_idx < rooftop_aligned.index[-1]:
                        # Calculate how many periods we're forward-filling
                        fill_start_pos = rooftop_aligned.index.get_loc(last_valid_idx) + 1
                        fill_periods = len(rooftop_aligned) - fill_start_pos

                        if fill_periods > 0:
                            # Apply exponential decay for realism (solar decreases over time)
                            # Use 2-hour half-life (resolution-aware)
                            last_value = rooftop_aligned.iloc[fill_start_pos - 1]
                            decay_rate = get_decay_rate_per_period(2.0, resolution_minutes)
                            for i in range(fill_periods):
                                rooftop_aligned.iloc[fill_start_pos + i] = last_value * (decay_rate ** (i + 1))

                    # Fill any remaining NaN with 0
                    rooftop_aligned = rooftop_aligned.fillna(0)
                    pivot_df['Rooftop Solar'] = rooftop_aligned

                    logger.info(f"Added rooftop solar: max {pivot_df['Rooftop Solar'].max():.1f}MW "
                               f"(resolution: {resolution_minutes}min, ffill_limit: {ffill_limit} periods)")
                
            except Exception as e:
                logger.error(f"Error adding rooftop solar: {e}")
        
        # Ensure all values are positive for stacking EXCEPT Battery Storage
        # Battery Storage can be negative (charging) or positive (discharging)
        for col in pivot_df.columns:
            if col not in ['settlementdate', 'Battery Storage', 'Battery']:
                pivot_df[col] = pivot_df[col].clip(lower=0)
        
        logger.info(f"Prepared data shape: {pivot_df.shape}")
        logger.info(f"Fuel types: {list(pivot_df.columns)}")
        
        # Define preferred fuel order with battery near zero line
        preferred_order = [
            'Transmission Flow',     # At top of stack (positive values)
            'Solar', 
            'Rooftop Solar',
            'Wind', 
            'Other', 
            'Coal', 
            'CCGT', 
            'Gas other', 
            'OCGT', 
            'Water',
            'Battery Storage',       # Near zero line (can be negative for charging)
            'Battery',              # Alternative name for Battery Storage
            'Biomass'
        ]
        
        # Reorder columns based on preferred order, only including columns that exist
        available_fuels = [fuel for fuel in preferred_order if fuel in pivot_df.columns]
        
        # Add any remaining fuels not in the preferred order
        remaining_fuels = [col for col in pivot_df.columns if col not in available_fuels]
        final_order = available_fuels + remaining_fuels
        
        # Reorder the dataframe
        pivot_df = pivot_df[final_order]
        
        return pivot_df
        
    except Exception as e:
        logger.error(f"Error preparing generation data: {e}")
        return pd.DataFrame()


def create_24hour_generation_chart(pivot_df):
    """
    Create 24-hour stacked area chart with proper battery handling
    """
    try:
        if pivot_df.empty:
            return pn.pane.HTML(
                "<div style='width:1000px;height:400px;display:flex;align-items:center;justify-content:center;'>"
                "<h3>No generation data available for last 24 hours</h3></div>",
                width=1000, height=400
            )
        
        # Rename Battery Storage to Battery for consistency
        if 'Battery Storage' in pivot_df.columns:
            pivot_df = pivot_df.rename(columns={'Battery Storage': 'Battery'})
        
        # Get fuel types (exclude any index columns)
        fuel_types = [col for col in pivot_df.columns if col not in ['settlementdate']]
        
        if not fuel_types:
            return pn.pane.HTML(
                "<div style='width:1000px;height:400px;display:flex;align-items:center;justify-content:center;'>"
                "<h3>No fuel type data available</h3></div>",
                width=1000, height=400
            )
        
        # Separate positive and negative values for battery
        plot_data_positive = pivot_df.copy()
        battery_col = 'Battery'
        has_battery = battery_col in plot_data_positive.columns
        
        # Handle battery - only keep positive values in main plot
        if has_battery:
            plot_data_positive[battery_col] = plot_data_positive[battery_col].clip(lower=0)
        
        # Get fuel types for positive stacking (all fuels)
        positive_fuel_types = fuel_types
        
        # Create main stacked area plot with positive values
        main_plot = plot_data_positive.hvplot.area(
            x='settlementdate',
            y=positive_fuel_types,
            stacked=True,
            width=1000,
            height=400,
            ylabel='Generation (MW)',
            xlabel='Time',
            color=[FUEL_COLORS.get(fuel, '#888888') for fuel in positive_fuel_types],
            alpha=0.8,
            hover_cols=['settlementdate'] + positive_fuel_types,
            legend='right'
        )
        
        # Check if we have negative battery values
        if has_battery and (pivot_df[battery_col].values < 0).any():
            # Create negative battery data
            plot_data_negative = pd.DataFrame(index=pivot_df.index)
            plot_data_negative['settlementdate'] = plot_data_negative.index
            plot_data_negative[battery_col] = pd.Series(
                np.where(pivot_df[battery_col].values < 0, pivot_df[battery_col].values, 0),
                index=pivot_df.index
            )
            
            # Create battery negative area plot
            battery_negative_plot = plot_data_negative.hvplot.area(
                x='settlementdate',
                y=battery_col,
                stacked=False,
                width=1000,
                height=400,
                color=FUEL_COLORS.get('Battery', '#9370DB'),
                alpha=0.8,
                hover=True,
                legend=False  # Legend already shown in main plot
            )
            
            # Combine positive and negative plots
            area_plot = main_plot * battery_negative_plot
        else:
            area_plot = main_plot
        
        # Apply styling options with Flexoki Light theme
        # Use hooks to set legend and plot area backgrounds explicitly
        area_plot = area_plot.opts(
            title="NEM Generation - Last 24 Hours",
            show_grid=False,
            toolbar='above',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            bgcolor=FLEXOKI_PAPER,  # Flexoki Light background
            hooks=[set_flexoki_background]  # Apply hook to set legend/border backgrounds
        )
        
        return pn.pane.HoloViews(area_plot, sizing_mode='fixed', width=1000, height=400, 
                                css_classes=['chart-no-border'],
                                linked_axes=False)  # Disable axis linking to prevent UFuncTypeError
        
    except Exception as e:
        logger.error(f"Error creating generation chart: {e}")
        return pn.pane.HTML(
            f"<div style='width:1000px;height:400px;display:flex;align-items:center;justify-content:center;'>"
            f"<h3>Error creating chart: {e}</h3></div>",
            width=1000, height=400
        )


def create_generation_overview_component(dashboard_instance=None):
    """
    Create the complete 24-hour generation overview component
    """
    def update_generation_overview():
        try:
            # Try to use dashboard's processed data first if available
            if dashboard_instance and hasattr(dashboard_instance, 'process_data_for_region'):
                logger.info("Using dashboard's process_data_for_region() method")
                try:
                    # Use the dashboard's own method to get processed data
                    gen_data = dashboard_instance.process_data_for_region()
                    logger.info(f"Dashboard processed data shape: {gen_data.shape if not gen_data.empty else 'empty'}")
                    logger.info(f"Dashboard processed data columns: {list(gen_data.columns) if not gen_data.empty else 'none'}")

                    if not gen_data.empty:
                        # This data is already processed by fuel type and filtered by the dashboard's time range
                        # DO NOT filter again - the dashboard has already applied the correct time range
                        # Just ensure proper datetime format
                        if gen_data.index.name == 'settlementdate':
                            gen_data = gen_data.reset_index()
                            gen_data['SETTLEMENTDATE'] = gen_data['settlementdate']
                        elif 'settlementdate' not in gen_data.columns:
                            gen_data = gen_data.reset_index()
                            gen_data['SETTLEMENTDATE'] = gen_data.index

                        logger.info(f"Using dashboard processed data: {len(gen_data)} records (already filtered by dashboard)")

                        # Dashboard data already includes rooftop solar and transmission
                        # Do NOT load them separately or we'll have conflicts
                        transmission_data = None
                        rooftop_data = None
                except Exception as e:
                    logger.error(f"Error using dashboard processed data: {e}")
                    gen_data = pd.DataFrame()
                    transmission_data = load_transmission_data()
                    rooftop_data = load_rooftop_solar_data()
            else:
                # Fallback to loading data directly
                logger.info("Loading generation data directly")
                gen_data = load_generation_data()
                transmission_data = load_transmission_data()
                rooftop_data = load_rooftop_solar_data()

            # Prepare data for stacking
            pivot_df = prepare_generation_for_stacking(gen_data, transmission_data, rooftop_data)
            
            # Create chart
            chart = create_24hour_generation_chart(pivot_df)
            
            return chart
            
        except Exception as e:
            logger.error(f"Error updating generation overview: {e}")
            return pn.pane.HTML(
                f"<div style='width:1000px;height:400px;display:flex;align-items:center;justify-content:center;'>"
                f"<h3>Error loading generation overview: {e}</h3></div>",
                width=1000, height=400
            )
    
    return pn.pane.panel(update_generation_overview)


if __name__ == "__main__":
    # Test the generation overview component
    pn.extension('bokeh')
    hv.extension('bokeh')
    
    overview = create_generation_overview_component()
    
    layout = pn.Column(
        pn.pane.Markdown("## 24-Hour Generation Overview Test"),
        overview
    )
    layout.show()