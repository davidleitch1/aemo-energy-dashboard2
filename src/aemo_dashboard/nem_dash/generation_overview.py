"""
24-hour Generation Overview Component for Nem-dash tab
Fixed 24-hour stacked area chart showing NEM generation by fuel type
"""

import pandas as pd
import numpy as np
import panel as pn
import plotly.graph_objects as go
from datetime import datetime, timedelta

from ..shared.config import config
from ..shared.logging_config import get_logger
from .nem_dash_query_manager import NEMDashQueryManager

logger = get_logger(__name__)

# Initialize query manager
query_manager = NEMDashQueryManager()

# Fuel colors consistent with main dashboard
FUEL_COLORS = {
    'Solar': '#FFD700',           # Gold
    'Rooftop Solar': '#FFF59D',   # Light yellow
    'Wind': '#00FF7F',            # Spring green - matches Generation tab
    'Water': '#00BFFF',           # Sky blue - matches Generation tab
    'Battery Storage': '#9370DB',  # Medium purple
    'Battery': '#9370DB',         # Medium purple (same as Battery Storage)
    'Coal': '#8B4513',            # Saddle brown
    'Gas other': '#FF7F50',       # Coral
    'OCGT': '#FF6347',            # Tomato
    'CCGT': '#FF4500',            # Orange red
    'Biomass': '#228B22',         # Forest green
    'Other': '#A9A9A9',           # Dark gray
    'Transmission Flow': '#FFB6C1' # Light pink
}

# Plotly template for Dracula theme
PLOTLY_TEMPLATE = {
    'layout': {
        'paper_bgcolor': '#282a36',  # Dracula background
        'plot_bgcolor': '#282a36',   # Plot area background
        'font': {'color': '#f8f8f2', 'size': 12},  # Foreground text
        'xaxis': {
            'gridcolor': '#44475a',
            'zerolinecolor': '#44475a',
            'linecolor': '#6272a4'
        },
        'yaxis': {
            'gridcolor': '#44475a',
            'zerolinecolor': '#44475a',
            'linecolor': '#6272a4'
        },
        'legend': {
            'bgcolor': 'rgba(40, 42, 54, 0.8)',
            'bordercolor': '#6272a4',
            'borderwidth': 1
        }
    }
}


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
        
        # Ensure we only have last 24 hours of data
        if len(pivot_df) > 0:
            end_time = pivot_df.index.max()
            start_time = end_time - pd.Timedelta(hours=24)
            pivot_df = pivot_df[pivot_df.index >= start_time]
            logger.info(f"Filtered to last 24 hours: {len(pivot_df)} records from {start_time} to {end_time}")
        
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
                    
                    # Forward-fill missing values at the end (up to 2 hours)
                    # This handles the case where rooftop data is less recent than generation data
                    rooftop_aligned = rooftop_aligned.fillna(method='ffill', limit=24)  # 24 * 5min = 2 hours
                    
                    # Apply gentle decay for extended forward-fill periods
                    last_valid_idx = rooftop_aligned.last_valid_index()
                    if last_valid_idx is not None and last_valid_idx < rooftop_aligned.index[-1]:
                        # Calculate how many periods we're forward-filling
                        fill_start_pos = rooftop_aligned.index.get_loc(last_valid_idx) + 1
                        fill_periods = len(rooftop_aligned) - fill_start_pos
                        
                        if fill_periods > 0:
                            # Apply exponential decay for realism (solar decreases over time)
                            last_value = rooftop_aligned.iloc[fill_start_pos - 1]
                            decay_rate = 0.98  # 2% decay per 5-minute period
                            for i in range(fill_periods):
                                rooftop_aligned.iloc[fill_start_pos + i] = last_value * (decay_rate ** (i + 1))
                    
                    # Fill any remaining NaN with 0
                    rooftop_aligned = rooftop_aligned.fillna(0)
                    pivot_df['Rooftop Solar'] = rooftop_aligned
                    
                    logger.info(f"Added rooftop solar: max {pivot_df['Rooftop Solar'].max():.1f}MW")
                
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
    Create 24-hour stacked area chart with proper battery handling using Plotly
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

        # Create Plotly figure
        fig = go.Figure()

        # Separate positive and negative battery values
        battery_col = 'Battery'
        has_battery = battery_col in pivot_df.columns

        # Prepare data with battery handling
        plot_data = pivot_df.copy()
        battery_negative = None

        if has_battery:
            # Separate negative battery values
            battery_negative = pd.Series(
                np.where(plot_data[battery_col].values < 0, plot_data[battery_col].values, 0),
                index=plot_data.index
            )
            # Clip battery to positive for main stack
            plot_data[battery_col] = plot_data[battery_col].clip(lower=0)

        # Add stacked area traces for each fuel type
        for fuel in fuel_types:
            color = FUEL_COLORS.get(fuel, '#888888')
            fig.add_trace(go.Scatter(
                x=plot_data.index,
                y=plot_data[fuel],
                name=fuel,
                mode='lines',
                stackgroup='one',  # This creates the stacked area effect
                fillcolor=color,
                line=dict(color=color, width=0.5),
                opacity=0.8,
                hovertemplate='<b>%{fullData.name}</b><br>%{y:.0f} MW<extra></extra>'
            ))

        # Add negative battery area if exists
        if has_battery and (battery_negative < 0).any():
            color = FUEL_COLORS.get('Battery', '#9370DB')
            fig.add_trace(go.Scatter(
                x=plot_data.index,
                y=battery_negative,
                name='Battery (Charging)',
                mode='lines',
                fill='tozeroy',
                fillcolor=color,
                line=dict(color=color, width=0.5),
                opacity=0.8,
                showlegend=False,  # Already shown in main legend
                hovertemplate='<b>Battery Charging</b><br>%{y:.0f} MW<extra></extra>'
            ))

        # Apply layout styling with Dracula theme
        fig.update_layout(
            title=dict(
                text="NEM Generation - Last 24 Hours<br><sub>Design: ITK, Data: AEMO</sub>",
                font=dict(size=14)
            ),
            xaxis_title="",
            yaxis_title="Generation (MW)",
            width=1000,
            height=400,
            paper_bgcolor=PLOTLY_TEMPLATE['layout']['paper_bgcolor'],
            plot_bgcolor=PLOTLY_TEMPLATE['layout']['plot_bgcolor'],
            font=PLOTLY_TEMPLATE['layout']['font'],
            xaxis=dict(
                gridcolor=PLOTLY_TEMPLATE['layout']['xaxis']['gridcolor'],
                showgrid=False,
                linecolor=PLOTLY_TEMPLATE['layout']['xaxis']['linecolor']
            ),
            yaxis=dict(
                gridcolor=PLOTLY_TEMPLATE['layout']['yaxis']['gridcolor'],
                showgrid=False,
                linecolor=PLOTLY_TEMPLATE['layout']['yaxis']['linecolor']
            ),
            legend=dict(
                bgcolor=PLOTLY_TEMPLATE['layout']['legend']['bgcolor'],
                bordercolor=PLOTLY_TEMPLATE['layout']['legend']['bordercolor'],
                borderwidth=PLOTLY_TEMPLATE['layout']['legend']['borderwidth'],
                orientation='v',
                yanchor='top',
                y=1,
                xanchor='left',
                x=1.02
            ),
            hovermode='x unified',
            margin=dict(l=60, r=150, t=60, b=40)
        )

        return pn.pane.Plotly(fig, sizing_mode='fixed', width=1000, height=400)

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
                        # Convert to format needed for stacking (add SETTLEMENTDATE column)
                        if gen_data.index.name == 'settlementdate':
                            gen_data = gen_data.reset_index()
                            gen_data['SETTLEMENTDATE'] = gen_data['settlementdate']
                        elif 'settlementdate' not in gen_data.columns:
                            gen_data = gen_data.reset_index()
                            gen_data['SETTLEMENTDATE'] = gen_data.index
                        
                        # Filter to last 24 hours based on timestamp
                        if 'SETTLEMENTDATE' in gen_data.columns:
                            gen_data['SETTLEMENTDATE'] = pd.to_datetime(gen_data['SETTLEMENTDATE'])
                            end_time = gen_data['SETTLEMENTDATE'].max()
                            start_time = end_time - pd.Timedelta(hours=24)
                            gen_data = gen_data[gen_data['SETTLEMENTDATE'] >= start_time]
                        elif 'settlementdate' in gen_data.columns:
                            gen_data['settlementdate'] = pd.to_datetime(gen_data['settlementdate'])
                            end_time = gen_data['settlementdate'].max()
                            start_time = end_time - pd.Timedelta(hours=24)
                            gen_data = gen_data[gen_data['settlementdate'] >= start_time]
                        else:
                            # Fallback to tail if no date column
                            gen_data = gen_data.tail(288)
                        logger.info(f"Using dashboard processed data: {len(gen_data)} records for last 24h")
                except Exception as e:
                    logger.error(f"Error using dashboard processed data: {e}")
                    gen_data = pd.DataFrame()
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
    pn.extension('plotly')

    overview = create_generation_overview_component()

    layout = pn.Column(
        pn.pane.Markdown("## 24-Hour Generation Overview Test"),
        overview
    )
    layout.show()