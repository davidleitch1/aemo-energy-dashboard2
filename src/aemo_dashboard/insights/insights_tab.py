"""
Insights tab for AEMO Energy Dashboard
Based on Prices tab structure but with custom content
"""
import pandas as pd
import numpy as np
import panel as pn
import holoviews as hv
import hvplot.pandas
from datetime import datetime, time, timedelta
from typing import Optional, List

from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.price_adapter import load_price_data
from aemo_dashboard.shared.generation_adapter import load_generation_data
from aemo_dashboard.shared.config import Config
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

logger = get_logger(__name__)

# Optional LOESS import
try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOESS = True
except ImportError:
    HAS_LOESS = False
    logger.warning("statsmodels not available, LOESS smoothing disabled")

class InsightsTab:
    """Insights analysis tab with dynamic content and price controls"""
    
    def __init__(self):
        """Initialize the batteries tab"""
        # Initialize config
        self.config = Config()
        
        # Initialize generation query manager for efficient data loading
        self.query_manager = GenerationQueryManager()
        
        # Load battery information
        self._load_battery_info()
        
        # Initialize components
        self._setup_controls()
        self._setup_content_area()
        self._setup_one_bess_controls()
    
    def _load_battery_info(self):
        """Load battery storage information from gen_info"""
        try:
            import pickle
            with open(self.config.gen_info_file, 'rb') as f:
                gen_info = pickle.load(f)
            
            # Filter for battery storage
            self.battery_info = gen_info[gen_info['Fuel'] == 'Battery Storage'].copy()
            
            # Group batteries by region for easy access
            self.batteries_by_region = {}
            for region in self.battery_info['Region'].unique():
                region_batteries = self.battery_info[self.battery_info['Region'] == region]
                # Create list of tuples (display_name, duid)
                battery_list = []
                for _, battery in region_batteries.iterrows():
                    capacity_mw = battery.get('Capacity(MW)', battery.get('Reg Cap (MW)', 0))
                    storage_mwh = battery.get('Storage(MWh)', 0)
                    # Calculate duration in hours
                    duration = storage_mwh / capacity_mw if capacity_mw > 0 else 0
                    display_name = f"{battery['Site Name']} ({battery['DUID']}) - {capacity_mw:.0f} MW / {storage_mwh:.0f} MWh ({duration:.1f}h)"
                    battery_list.append((display_name, battery['DUID']))
                # Sort by MW capacity descending
                battery_list.sort(key=lambda x: -self.battery_info[self.battery_info['DUID'] == x[1]]['Capacity(MW)'].values[0])
                self.batteries_by_region[region] = battery_list
            
            logger.info(f"Loaded {len(self.battery_info)} battery storage units")
            
        except Exception as e:
            logger.error(f"Error loading battery info: {e}")
            self.battery_info = pd.DataFrame()
            self.batteries_by_region = {}
        
    def _setup_controls(self):
        """Set up all control widgets (same as Prices tab)"""
        # Date preset radio buttons (vertical like frequency)
        self.date_presets = pn.widgets.RadioBoxGroup(
            name='',
            options=['1 day', '7 days', '30 days', '90 days', '1 year', 'All data'],
            value='30 days',
            inline=False,
            width=100
        )
        
        # Date pickers
        default_end = pd.Timestamp.now().date()
        default_start = default_end - pd.Timedelta(days=30)
        
        self.start_date_picker = pn.widgets.DatePicker(
            name='Start Date',
            value=default_start,
            width=120
        )
        
        self.end_date_picker = pn.widgets.DatePicker(
            name='End Date',
            value=default_end,
            width=120
        )
        
        # Show selected dates clearly
        self.date_display = pn.pane.Markdown(
            f"**Selected Period:** {self.start_date_picker.value.strftime('%Y-%m-%d')} to {self.end_date_picker.value.strftime('%Y-%m-%d')}",
            width=300
        )
        
        # Region radio button group
        # Define region colors
        self.region_colors = {
            'NSW1': '#50fa7b',  # Green
            'QLD1': '#ffb86c',  # Orange
            'SA1': '#ff79c6',   # Pink
            'TAS1': '#8be9fd',  # Cyan
            'VIC1': '#bd93f9',  # Purple
            'NEM': '#f8f8f2'    # White for NEM (all regions)
        }
        
        # Create standard RadioBoxGroup (no color styling)
        self.region_selector = pn.widgets.RadioBoxGroup(
            name='',
            value='NEM',  # Default to NEM
            options=['NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
            inline=False,
            align='start',
            margin=(0, 0, 0, 0)
        )
        
        
        # Aggregate level radio buttons
        self.aggregate_selector = pn.widgets.RadioBoxGroup(
            name='',
            value='1 hour',
            options=['5 min', '1 hour', 'Daily', 'Monthly', 'Quarterly', 'Yearly'],
            inline=False,
            width=120
        )
        
        # Battery metric selector for lollipop chart
        self.metric_selector = pn.widgets.Select(
            name='Battery Metric',
            value='Discharge Revenue',
            options=[
                'Discharge Revenue',
                'Charge Cost', 
                'Discharge Price',
                'Charge Price',
                'Discharge Energy',
                'Charge Energy',
                'Price Spread'
            ],
            width=150
        )
        
        # Update button for loading battery analysis
        self.update_button = pn.widgets.Button(
            name='Update Battery Analysis',
            button_type='primary',
            width=150
        )
        
        # Set up callbacks
        self._setup_callbacks()
        
    def _setup_content_area(self):
        """Set up the content area for battery analysis"""
        # Battery analysis placeholder - now replaced with lollipop chart
        # Use a generic pane that can hold either HoloViews or Matplotlib objects
        self.battery_content_pane = pn.Column(
            sizing_mode='stretch_width',
            height=500
        )
        
        # Info pane for showing summary statistics
        self.battery_info_pane = pn.pane.HTML(
            """
            <div style="
                background-color: #1a1a1a;
                border: 2px solid #666;
                border-radius: 10px;
                padding: 15px;
                margin: 10px;
                color: #e0e0e0;
            ">
                <p>Click 'Update Battery Analysis' to generate the battery performance chart.</p>
            </div>
            """,
            sizing_mode='stretch_width'
        )
        
        # Placeholder for future dynamic content
        self.dynamic_content_pane = pn.pane.Markdown(
            "",
            width=600
        )
    
    def _setup_one_bess_controls(self):
        """Set up controls for One BESS subtab"""
        # Date preset radio buttons for BESS
        self.bess_date_presets = pn.widgets.RadioBoxGroup(
            name='',
            options=['1 day', '7 days', '30 days', '90 days', '1 year', 'All data'],
            value='7 days',
            inline=False,
            width=100
        )
        
        # Date pickers for BESS
        default_end = pd.Timestamp.now().date()
        default_start = default_end - pd.Timedelta(days=7)
        
        self.bess_start_date = pn.widgets.DatePicker(
            name='Start Date',
            value=default_start,
            width=120
        )
        
        self.bess_end_date = pn.widgets.DatePicker(
            name='End Date',
            value=default_end,
            width=120
        )
        
        # Frequency selector for BESS
        self.bess_frequency = pn.widgets.RadioBoxGroup(
            name='',
            value='1 hour',
            options=['5 min', '30 min', '1 hour', 'Daily'],
            inline=False,
            width=120
        )
        
        # Region selector for BESS
        self.bess_region_selector = pn.widgets.Select(
            name='Select Region',
            value='NSW1',
            options=['NSW1', 'QLD1', 'SA1', 'VIC1'],
            width=150
        )
        
        # Battery selector - will be populated based on region
        initial_batteries = self.batteries_by_region.get('NSW1', [])
        self.bess_selector = pn.widgets.Select(
            name='Select Battery',
            value=initial_batteries[0][1] if initial_batteries else None,
            options=dict(initial_batteries),
            width=400
        )
        
        # Analysis button
        self.bess_analyze_button = pn.widgets.Button(
            name='Analyze Battery',
            button_type='primary',
            width=150
        )
        
        # Log scale selector for price plot
        self.bess_log_scale = pn.widgets.Checkbox(
            name='Log Scale (Price)',
            value=False,
            width=120
        )
        
        # Results panes
        self.bess_info_pane = pn.pane.HTML(
            """
            <div style="background-color: #1a1a1a; border: 1px solid #666; padding: 10px; border-radius: 5px;">
                <p style="color: #e0e0e0;">Select a battery and click Analyze to view details.</p>
            </div>
            """,
            sizing_mode='stretch_width'
        )
        
        self.bess_chart_pane = pn.pane.HoloViews(
            object=None,
            sizing_mode='stretch_width',
            height=400
        )
        
        # Set up callbacks
        def update_bess_date_range(event):
            """Update date range based on preset selection for BESS"""
            preset = event.new
            current_end = self.bess_end_date.value
            
            if preset == '1 day':
                new_start = current_end - pd.Timedelta(days=1)
            elif preset == '7 days':
                new_start = current_end - pd.Timedelta(days=7)
            elif preset == '30 days':
                new_start = current_end - pd.Timedelta(days=30)
            elif preset == '90 days':
                new_start = current_end - pd.Timedelta(days=90)
            elif preset == '1 year':
                new_start = current_end - pd.Timedelta(days=365)
            else:  # All data
                new_start = pd.Timestamp('2020-01-01').date()
            
            self.bess_start_date.value = new_start
        
        def update_battery_list(event):
            """Update battery list when region changes"""
            selected_region = event.new
            batteries = self.batteries_by_region.get(selected_region, [])
            self.bess_selector.options = dict(batteries)
            if batteries:
                self.bess_selector.value = batteries[0][1]
            logger.info(f"Updated battery list for region {selected_region}: {len(batteries)} batteries")
        
        def analyze_battery(event):
            """Analyze selected battery with date range and frequency"""
            selected_duid = self.bess_selector.value
            selected_region = self.bess_region_selector.value
            start_date = self.bess_start_date.value
            end_date = self.bess_end_date.value
            frequency = self.bess_frequency.value
            
            if not selected_duid:
                self.bess_info_pane.object = """
                <div style="background-color: #1a1a1a; border: 1px solid #666; padding: 10px; border-radius: 5px;">
                    <p style="color: #ff5555;">No battery selected.</p>
                </div>
                """
                return
            
            # Get battery info
            battery_data = self.battery_info[self.battery_info['DUID'] == selected_duid]
            if not battery_data.empty:
                battery = battery_data.iloc[0].to_dict()  # Convert to dict for safer access
                
                # Debug: Print available keys
                logger.info(f"Battery dict keys: {list(battery.keys())}")
                
                capacity_mw = battery.get('Capacity(MW)', battery.get('Reg Cap (MW)', 0))
                storage_mwh = battery.get('Storage(MWh)', 0)
                duration = storage_mwh / capacity_mw if capacity_mw > 0 else 0
                
                # Load generation and price data for analysis
                try:
                    # Convert dates to datetime
                    start_dt = pd.Timestamp(start_date)
                    end_dt = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    
                    # Load generation data - use 5min if requested, otherwise 30min
                    from aemo_dashboard.shared.generation_adapter import load_generation_data
                    base_resolution = '5min' if frequency == '5 min' else '30min'
                    gen_data = load_generation_data(
                        start_date=start_dt,
                        end_date=end_dt,
                        resolution=base_resolution
                    )
                    
                    if not gen_data.empty:
                        # Standardize column names to uppercase for consistency
                        gen_data.columns = gen_data.columns.str.upper()
                        
                        # Filter for selected DUID
                        battery_gen = gen_data[gen_data['DUID'] == selected_duid].copy()
                        
                        if not battery_gen.empty:
                            # Load price data - same resolution as generation
                            from aemo_dashboard.shared.price_adapter import load_price_data
                            price_data = load_price_data(
                                start_date=start_dt,
                                end_date=end_dt,
                                resolution=base_resolution,
                                regions=[selected_region]
                            )
                            
                            if not price_data.empty:
                                # Merge generation and price data
                                battery_gen['SETTLEMENTDATE'] = pd.to_datetime(battery_gen['SETTLEMENTDATE'])
                                price_data.index.name = 'SETTLEMENTDATE'
                                price_data = price_data.reset_index()
                                price_data['SETTLEMENTDATE'] = pd.to_datetime(price_data['SETTLEMENTDATE'])
                                
                                # Debug: Check data before merge
                                logger.info(f"Battery gen records: {len(battery_gen)}, unique timestamps: {battery_gen['SETTLEMENTDATE'].nunique()}")
                                logger.info(f"Price data records: {len(price_data)}, unique timestamps: {price_data['SETTLEMENTDATE'].nunique()}")
                                
                                # Use outer merge to keep all timestamps from both datasets
                                analysis_data = battery_gen.merge(
                                    price_data[['SETTLEMENTDATE', 'RRP']],
                                    on='SETTLEMENTDATE',
                                    how='outer'  # Changed from 'left' to 'outer' to keep all price data
                                )
                                
                                # Fill missing SCADAVALUE with 0 (battery idle) and DUID with the selected DUID
                                analysis_data['SCADAVALUE'] = analysis_data['SCADAVALUE'].fillna(0)
                                analysis_data['DUID'] = analysis_data['DUID'].fillna(selected_duid)
                                
                                logger.info(f"After merge: {len(analysis_data)} records, RRP nulls: {analysis_data['RRP'].isnull().sum()}")
                                
                                # Determine base time multiplier for the source data
                                if base_resolution == '5min':
                                    base_time_multiplier = 1/12  # 5 minutes = 1/12 hours
                                else:  # 30min
                                    base_time_multiplier = 0.5  # 30 minutes = 0.5 hours
                                
                                # First, calculate MWh and revenue for each base period
                                analysis_data['MWH'] = analysis_data['SCADAVALUE'] * base_time_multiplier
                                analysis_data['REVENUE'] = analysis_data['MWH'] * analysis_data['RRP']
                                
                                # Aggregate data based on frequency selection
                                if frequency == '5 min':
                                    # Using 5-minute data as-is
                                    aggregated_data = analysis_data.copy()
                                elif frequency == '30 min':
                                    # Use 30-minute data as-is
                                    aggregated_data = analysis_data.copy()
                                elif frequency == '1 hour':
                                    # Aggregate to hourly
                                    analysis_data['SETTLEMENTDATE'] = pd.to_datetime(analysis_data['SETTLEMENTDATE'])
                                    
                                    # Group by hour and aggregate
                                    aggregated_data = analysis_data.set_index('SETTLEMENTDATE').resample('1H').agg({
                                        'SCADAVALUE': 'mean',  # Average MW for display
                                        'MWH': 'sum',  # Total MWh for the hour
                                        'REVENUE': 'sum',  # Total revenue for the hour
                                        'RRP': 'mean',  # Simple average price for display
                                        'DUID': 'first'
                                    }).reset_index()
                                    
                                elif frequency == 'Daily':
                                    # Aggregate to daily
                                    analysis_data['SETTLEMENTDATE'] = pd.to_datetime(analysis_data['SETTLEMENTDATE'])
                                    
                                    # Group by day and aggregate
                                    aggregated_data = analysis_data.set_index('SETTLEMENTDATE').resample('1D').agg({
                                        'SCADAVALUE': 'mean',  # Average MW for display
                                        'MWH': 'sum',  # Total MWh for the day
                                        'REVENUE': 'sum',  # Total revenue for the day
                                        'RRP': 'mean',  # Simple average price for display
                                        'DUID': 'first'
                                    }).reset_index()
                                    
                                else:
                                    # Default to base resolution
                                    aggregated_data = analysis_data.copy()
                                
                                # Calculate metrics using aggregated data
                                # Split by charge/discharge based on MWH (which preserves the sign from SCADAVALUE)
                                discharge_data = aggregated_data[aggregated_data['MWH'] > 0]
                                charge_data = aggregated_data[aggregated_data['MWH'] < 0]
                                
                                # Calculate totals from pre-aggregated data
                                # Energy totals
                                total_discharge_mwh = discharge_data['MWH'].sum() if not discharge_data.empty else 0
                                total_charge_mwh = abs(charge_data['MWH'].sum()) if not charge_data.empty else 0
                                
                                # Revenue totals (already calculated in REVENUE column)
                                total_discharge_revenue = discharge_data['REVENUE'].sum() if not discharge_data.empty else 0
                                total_charge_cost = abs(charge_data['REVENUE'].sum()) if not charge_data.empty else 0
                                
                                # Calculate weighted average prices from totals
                                avg_discharge_price = total_discharge_revenue / total_discharge_mwh if total_discharge_mwh > 0 else 0
                                avg_charge_price = total_charge_cost / total_charge_mwh if total_charge_mwh > 0 else 0
                                
                                # Calculate average spread
                                avg_spread = avg_discharge_price - avg_charge_price
                                
                                # Debug logging to verify calculations
                                logger.info(f"Frequency: {frequency}, Base resolution: {base_resolution}")
                                logger.info(f"Total discharge MWh: {total_discharge_mwh:.2f}")
                                logger.info(f"Total charge MWh: {total_charge_mwh:.2f}")
                                logger.info(f"Total discharge revenue: ${total_discharge_revenue:.2f}")
                                logger.info(f"Total charge cost: ${total_charge_cost:.2f}")
                                logger.info(f"Avg discharge price: ${avg_discharge_price:.2f}/MWh")
                                logger.info(f"Avg charge price: ${avg_charge_price:.2f}/MWh")
                                logger.info(f"Verification - Revenue/MWh: ${total_discharge_revenue/total_discharge_mwh:.2f}" if total_discharge_mwh > 0 else "No discharge")
                                
                                # Total spread is the net profit (revenue minus cost)
                                total_spread = total_discharge_revenue - total_charge_cost
                                
                                # Days calculation
                                total_days = (end_dt - start_dt).days + 1
                                aggregated_data['SETTLEMENTDATE'] = pd.to_datetime(aggregated_data['SETTLEMENTDATE'])
                                aggregated_data['date'] = aggregated_data['SETTLEMENTDATE'].dt.date
                                days_discharged = aggregated_data[aggregated_data['SCADAVALUE'] > 0]['date'].nunique()
                                days_charged = aggregated_data[aggregated_data['SCADAVALUE'] < 0]['date'].nunique()
                                pct_days_discharged = (days_discharged / total_days * 100) if total_days > 0 else 0
                                pct_days_charged = (days_charged / total_days * 100) if total_days > 0 else 0
                                
                                # Capacity utilization (based on one full discharge cycle per day)
                                capacity_utilization = (total_discharge_mwh / (storage_mwh * total_days) * 100) if storage_mwh > 0 and total_days > 0 else 0
                                
                                # Format revenue/cost
                                if abs(total_discharge_revenue) >= 1000000:
                                    revenue_str = f"${total_discharge_revenue/1000000:.2f}m"
                                else:
                                    revenue_str = f"${total_discharge_revenue/1000:.1f}k"
                                
                                if abs(total_charge_cost) >= 1000000:
                                    cost_str = f"${total_charge_cost/1000000:.2f}m"
                                else:
                                    cost_str = f"${total_charge_cost/1000:.1f}k"
                                
                                # Format total spread
                                if abs(total_spread) >= 1000000:
                                    total_spread_str = f"${total_spread/1000000:.2f}m"
                                else:
                                    total_spread_str = f"${total_spread/1000:.1f}k"
                                
                                # Create hvplot chart
                                import hvplot.pandas
                                
                                # Prepare data for plotting - use aggregated data
                                plot_data = aggregated_data.copy()
                                # Set SETTLEMENTDATE as index for hvplot
                                plot_data = plot_data.set_index('SETTLEMENTDATE')
                                plot_data.index = pd.to_datetime(plot_data.index)
                                plot_data = plot_data.sort_index()
                                
                                # Debug logging
                                logger.info(f"Plot data shape: {plot_data.shape}")
                                logger.info(f"Date range in plot: {plot_data.index.min()} to {plot_data.index.max()}")
                                logger.info(f"SCADAVALUE range: {plot_data['SCADAVALUE'].min():.2f} to {plot_data['SCADAVALUE'].max():.2f}")
                                logger.info(f"RRP nulls after processing: {plot_data['RRP'].isnull().sum()} out of {len(plot_data)}")
                                
                                # Use bar plot which works perfectly with colors
                                # In AEMO data: positive SCADAVALUE = discharge (generation), negative = charge (consumption)
                                
                                # Create a DataFrame with both columns
                                power_df = pd.DataFrame(index=plot_data.index)
                                power_df['Discharge'] = plot_data['SCADAVALUE'].where(plot_data['SCADAVALUE'] > 0, 0)
                                power_df['Charge'] = plot_data['SCADAVALUE'].where(plot_data['SCADAVALUE'] < 0, 0)
                                
                                # Create bar plots (using step as bars)
                                discharge_plot = power_df['Discharge'].hvplot.step(
                                    where='mid',
                                    color='#50fa7b',
                                    line_width=1,
                                    label='Discharge'
                                )
                                
                                charge_plot = power_df['Charge'].hvplot.step(
                                    where='mid',
                                    color='#ff5555',
                                    line_width=1,
                                    label='Charge'
                                )
                                
                                # Combine the charge/discharge plots
                                power_plot = (discharge_plot * charge_plot).opts(
                                    ylabel='Power (MW)',
                                    xlabel='',  # Remove x-label from top plot
                                    height=300,
                                    width=800,
                                    title=f"{battery['Site Name']} - Charge/Discharge Profile",
                                    legend_position='top_right',
                                    bgcolor='#282a36',
                                    show_grid=True,
                                    gridstyle={'grid_line_alpha': 0.2, 'grid_line_color': '#44475a'},
                                    xaxis=None  # Hide x-axis completely on top plot
                                )
                                
                                # Create a horizontal line at y=0 for the price plot
                                import holoviews as hv
                                zero_line = hv.HLine(0).opts(
                                    color='white',
                                    line_width=0.5,
                                    alpha=0.8
                                )
                                
                                # Handle log scale for price plot
                                use_log_scale = self.bess_log_scale.value
                                
                                if use_log_scale:
                                    ylabel_text = 'Price ($/MWh, symlog scale)'
                                    # Apply symlog transformation to the data for plotting
                                    # This creates a symmetric log scale effect that handles negative values
                                    import numpy as np
                                    
                                    symlog_threshold = 300
                                    price_data_transformed = plot_data['RRP'].copy()
                                    
                                    # Apply symlog transformation manually
                                    # For values between -300 and 300, keep them linear
                                    # For values above 300 or below -300, apply log transformation
                                    positive_mask = price_data_transformed > symlog_threshold
                                    negative_mask = price_data_transformed < -symlog_threshold
                                    
                                    # Transform positive values above threshold
                                    if positive_mask.any():
                                        price_data_transformed.loc[positive_mask] = (
                                            symlog_threshold * (1 + np.log10(price_data_transformed.loc[positive_mask] / symlog_threshold))
                                        )
                                    
                                    # Transform negative values below threshold
                                    if negative_mask.any():
                                        price_data_transformed.loc[negative_mask] = (
                                            -symlog_threshold * (1 + np.log10(-price_data_transformed.loc[negative_mask] / symlog_threshold))
                                        )
                                    
                                    # Create the plot with transformed data
                                    price_line = price_data_transformed.hvplot.line(
                                        color='#f1fa8c',  # Yellow for price
                                        line_width=2,
                                        ylabel=ylabel_text,
                                        xlabel='Date',
                                        label='Price (symlog)',
                                        height=280,  # Increased height for price plot
                                        width=800
                                    ).opts(
                                        bgcolor='#282a36',
                                        show_grid=True,
                                        gridstyle={'grid_line_alpha': 0.2, 'grid_line_color': '#44475a'},
                                        line_join='round',
                                        line_cap='round'
                                    )
                                    
                                else:
                                    ylabel_text = 'Price ($/MWh)'
                                    # Create normal linear scale price plot
                                    price_line = plot_data['RRP'].hvplot.line(
                                        color='#f1fa8c',  # Yellow for price
                                        line_width=2,
                                        ylabel=ylabel_text,
                                        xlabel='Date',
                                        label='Price',
                                        height=280,  # Increased height for price plot
                                        width=800
                                    ).opts(
                                        bgcolor='#282a36',
                                        show_grid=True,
                                        gridstyle={'grid_line_alpha': 0.2, 'grid_line_color': '#44475a'},
                                        line_join='round',
                                        line_cap='round'
                                    )
                                
                                # Always overlay the zero line (visible in both linear and symlog)
                                price_plot = price_line * zero_line
                                
                                # Stack the plots vertically with linked x-axes
                                combined_plot = (power_plot + price_plot).cols(1).opts(
                                    hv.opts.Layout(shared_axes='x')  # Link only the x-axes, not y-axes
                                )
                                
                                # Update the chart pane with the stacked plots
                                self.bess_chart_pane.object = combined_plot
                                
                                # Create performance metrics table
                                metrics_html = f"""
                                <hr style="border-color: #666; margin: 15px 0;">
                                <table style="color: #e0e0e0; width: 100%; border-collapse: collapse;">
                                    <tr><td colspan="2" style="color: #bd93f9; padding-bottom: 10px;"><strong>Performance Metrics:</strong></td></tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Average Discharge Price:</strong></td>
                                        <td style="padding: 5px; text-align: right;">${avg_discharge_price:.0f}/MWh</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Average Charge Price:</strong></td>
                                        <td style="padding: 5px; text-align: right;">${avg_charge_price:.0f}/MWh</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Average Spread:</strong></td>
                                        <td style="padding: 5px; text-align: right; color: #50fa7b; font-weight: bold;">${avg_spread:.0f}/MWh</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Total Discharge Revenue:</strong></td>
                                        <td style="padding: 5px; text-align: right; color: #50fa7b;">{revenue_str}</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Total Charge Cost:</strong></td>
                                        <td style="padding: 5px; text-align: right; color: #ff5555;">{cost_str}</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Total Spread (Gross Profit):</strong></td>
                                        <td style="padding: 5px; text-align: right; color: #ffb86c; font-weight: bold;">{total_spread_str}</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Total Discharge Energy:</strong></td>
                                        <td style="padding: 5px; text-align: right;">{total_discharge_mwh:,.1f} MWh</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Total Charge Energy:</strong></td>
                                        <td style="padding: 5px; text-align: right;">{total_charge_mwh:,.1f} MWh</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Days Discharged:</strong></td>
                                        <td style="padding: 5px; text-align: right;">{pct_days_discharged:.1f}% ({days_discharged}/{total_days} days)</td>
                                    </tr>
                                    <tr style="border-bottom: 1px solid #444;">
                                        <td style="padding: 5px;"><strong>Days Charged:</strong></td>
                                        <td style="padding: 5px; text-align: right;">{pct_days_charged:.1f}% ({days_charged}/{total_days} days)</td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 5px;"><strong>Capacity Utilization:</strong></td>
                                        <td style="padding: 5px; text-align: right; color: #ffb86c;">{capacity_utilization:.1f}%</td>
                                    </tr>
                                </table>
                                """
                            else:
                                metrics_html = '<p style="color: #ff5555;">No price data available for analysis period.</p>'
                                self.bess_chart_pane.object = None  # Clear chart
                        else:
                            metrics_html = '<p style="color: #ff5555;">No generation data found for this battery in the selected period.</p>'
                            self.bess_chart_pane.object = None  # Clear chart
                    else:
                        metrics_html = '<p style="color: #ff5555;">No generation data available for analysis period.</p>'
                        self.bess_chart_pane.object = None  # Clear chart
                        
                except Exception as e:
                    logger.error(f"Error analyzing battery: {e}")
                    metrics_html = f'<p style="color: #ff5555;">Error analyzing battery: {str(e)}</p>'
                    self.bess_chart_pane.object = None  # Clear chart on error
                
                # Create info display with analysis parameters and metrics
                info_html = f"""
                <div style="background-color: #1a1a1a; border: 2px solid #bd93f9; padding: 15px; border-radius: 5px;">
                    <h3 style="color: #bd93f9; margin-top: 0;">{battery['Site Name']}</h3>
                    <table style="color: #e0e0e0; width: 100%;">
                        <tr><td><strong>DUID:</strong></td><td>{battery['DUID']}</td></tr>
                        <tr><td><strong>Region:</strong></td><td>{battery['Region']}</td></tr>
                        <tr><td><strong>Power Capacity:</strong></td><td>{capacity_mw:.0f} MW</td></tr>
                        <tr><td><strong>Energy Storage:</strong></td><td>{storage_mwh:.0f} MWh</td></tr>
                        <tr><td><strong>Duration:</strong></td><td>{duration:.1f} hours</td></tr>
                        <tr><td><strong>Technology:</strong></td><td>Battery Storage</td></tr>
                    </table>
                    <hr style="border-color: #666; margin: 10px 0;">
                    <table style="color: #e0e0e0; width: 100%;">
                        <tr><td colspan="2" style="color: #bd93f9;"><strong>Analysis Parameters:</strong></td></tr>
                        <tr><td><strong>Date Range:</strong></td><td>{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}</td></tr>
                        <tr><td><strong>Frequency:</strong></td><td>{frequency}</td></tr>
                    </table>
                    {metrics_html}
                </div>
                """
                self.bess_info_pane.object = info_html
                
                logger.info(f"Analyzing battery: {battery['Site Name']} ({selected_duid}) from {start_date} to {end_date}")
            
        # Connect callbacks
        self.bess_date_presets.param.watch(update_bess_date_range, 'value')
        self.bess_region_selector.param.watch(update_battery_list, 'value')
        self.bess_analyze_button.on_click(analyze_battery)
        # Re-analyze when log scale changes (only if already analyzed)
        self.bess_log_scale.param.watch(lambda event: analyze_battery(event) if self.bess_chart_pane.object is not None else None, 'value')
        
    def _setup_callbacks(self):
        """Set up widget callbacks"""
        # Date preset callback
        def update_date_range(event):
            """Update date range based on preset selection"""
            preset = event.new
            current_end = self.end_date_picker.value
            
            if preset == '1 day':
                new_start = current_end - pd.Timedelta(days=1)
            elif preset == '7 days':
                new_start = current_end - pd.Timedelta(days=7)
            elif preset == '30 days':
                new_start = current_end - pd.Timedelta(days=30)
            elif preset == '90 days':
                new_start = current_end - pd.Timedelta(days=90)
            elif preset == '1 year':
                new_start = current_end - pd.Timedelta(days=365)
            else:  # All data
                new_start = pd.Timestamp('2020-01-01').date()
            
            self.start_date_picker.value = new_start
        
        # Date picker callback
        def update_date_display(event):
            """Update the date display when date pickers change"""
            self.date_display.object = f"**Selected Period:** {self.start_date_picker.value.strftime('%Y-%m-%d')} to {self.end_date_picker.value.strftime('%Y-%m-%d')}"
        
        # Update button callback
        def update_insights(event):
            """Update battery analysis based on current selections"""
            logger.info(f"Updating battery analysis for regions: {self.region_selector.value}")
            logger.info(f"Date range: {self.start_date_picker.value} to {self.end_date_picker.value}")
            
            # Clear the dynamic content pane
            self.dynamic_content_pane.object = ""
            
            # Battery analysis will be implemented here
            self.battery_content_pane.clear()
            self.battery_content_pane.append(pn.pane.HTML("""
            <div style="
                background-color: #1a1a1a;
                border: 2px solid #666;
                border-radius: 10px;
                padding: 20px;
                margin: 10px;
                color: #ffffff;
            ">
                <h2 style="color: #bd93f9;">Battery Analysis Updated</h2>
                <p style="color: #e0e0e0;">Analysis for selected regions and date range will appear here.</p>
            </div>
            """))
        
        # Connect callbacks
        self.date_presets.param.watch(update_date_range, 'value')
        self.start_date_picker.param.watch(update_date_display, 'value')
        self.end_date_picker.param.watch(update_date_display, 'value')
        self.update_button.on_click(self._create_battery_lollipop_chart)
    
    def _calculate_battery_metrics(self, event=None):
        """Calculate metrics for all batteries in selected regions"""
        try:
            # Get selected region (now single selection)
            selected_region = self.region_selector.value
            if not selected_region:
                logger.warning("No region selected")
                return pd.DataFrame()
            
            # Handle NEM selection - aggregate all regions
            if selected_region == 'NEM':
                actual_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
            else:
                actual_regions = [selected_region]
            
            # Get date range
            start_date = pd.Timestamp(self.start_date_picker.value)
            end_date = pd.Timestamp(self.end_date_picker.value) + pd.Timedelta(hours=23, minutes=59)
            
            # Load generation and price data
            logger.info(f"Loading data for regions {actual_regions} from {start_date} to {end_date}")
            
            gen_data = load_generation_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min'
            )
            
            price_data = load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min'
            )
            
            if gen_data.empty or price_data.empty:
                logger.warning("No data available for analysis")
                return pd.DataFrame()
            
            # Standardize column names
            gen_data.columns = gen_data.columns.str.upper()
            if price_data.index.name == 'SETTLEMENTDATE':
                price_data = price_data.reset_index()
            price_data.columns = price_data.columns.str.upper()
            
            # Filter for batteries in selected regions
            batteries_to_analyze = self.battery_info[
                self.battery_info['Region'].isin(actual_regions)
            ].copy()
            
            if batteries_to_analyze.empty:
                logger.warning("No batteries found in selected regions")
                return pd.DataFrame()
            
            # Calculate metrics for each battery
            metrics_list = []
            
            for _, battery in batteries_to_analyze.iterrows():
                duid = battery['DUID']
                
                # Filter generation data for this battery
                battery_gen = gen_data[gen_data['DUID'] == duid].copy()
                
                if battery_gen.empty:
                    continue
                
                # Merge with price data
                battery_gen = battery_gen.merge(
                    price_data[['SETTLEMENTDATE', 'REGIONID', 'RRP']],
                    on='SETTLEMENTDATE',
                    how='left'
                )
                
                # Filter for matching region
                battery_gen = battery_gen[battery_gen['REGIONID'] == battery['Region']]
                
                if battery_gen.empty:
                    continue
                
                # Separate charge and discharge
                discharge_data = battery_gen[battery_gen['SCADAVALUE'] > 0].copy()
                charge_data = battery_gen[battery_gen['SCADAVALUE'] < 0].copy()
                
                # Calculate metrics based on selected metric type
                metrics = {
                    'DUID': duid,
                    'Site Name': battery['Site Name'],
                    'Region': battery['Region'],
                    'Capacity_MW': battery.get('Capacity(MW)', 0),
                    'Storage_MWh': battery.get('Storage(MWh)', 0)
                }
                
                # Discharge metrics
                if not discharge_data.empty:
                    metrics['Discharge Price'] = discharge_data['RRP'].mean()
                    metrics['Discharge Energy'] = discharge_data['SCADAVALUE'].sum() / 2  # MWh for 30-min periods
                    metrics['Discharge Revenue'] = (discharge_data['SCADAVALUE'] * discharge_data['RRP'] / 2).sum()
                else:
                    metrics['Discharge Price'] = 0
                    metrics['Discharge Energy'] = 0
                    metrics['Discharge Revenue'] = 0
                
                # Charge metrics
                if not charge_data.empty:
                    metrics['Charge Price'] = charge_data['RRP'].mean()
                    metrics['Charge Energy'] = abs(charge_data['SCADAVALUE'].sum()) / 2  # MWh
                    metrics['Charge Cost'] = abs((charge_data['SCADAVALUE'] * charge_data['RRP'] / 2).sum())
                else:
                    metrics['Charge Price'] = 0
                    metrics['Charge Energy'] = 0
                    metrics['Charge Cost'] = 0
                
                # Calculate spread
                metrics['Price Spread'] = metrics['Discharge Price'] - metrics['Charge Price']
                
                metrics_list.append(metrics)
            
            if not metrics_list:
                logger.warning("No battery data found for analysis")
                return pd.DataFrame()
            
            return pd.DataFrame(metrics_list)
            
        except Exception as e:
            logger.error(f"Error calculating battery metrics: {e}")
            return pd.DataFrame()
    
    def _create_battery_lollipop_chart(self, event=None):
        """Create lollipop chart for battery comparison using matplotlib"""
        try:
            logger.info("Creating battery lollipop chart")
            
            # Calculate metrics for all batteries
            metrics_df = self._calculate_battery_metrics()
            
            if metrics_df.empty:
                self.battery_info_pane.object = """
                <div style="background-color: #1a1a1a; border: 2px solid #ff5555; padding: 15px; border-radius: 10px;">
                    <p style="color: #ff5555;">No battery data available for the selected regions and date range.</p>
                </div>
                """
                self.battery_content_pane.clear()
                return
            
            # Get selected metric
            selected_metric = self.metric_selector.value
            
            # Sort by selected metric and get top 20
            metrics_df = metrics_df.sort_values(selected_metric, ascending=False).head(20)
            
            # Create display names (truncate if too long)
            metrics_df['Display Name'] = metrics_df.apply(
                lambda x: f"{x['Site Name'][:20]}..." if len(x['Site Name']) > 20 else x['Site Name'],
                axis=1
            )
            
            # Determine colors based on region when NEM is selected
            selected_region = self.region_selector.value
            if selected_region == 'NEM':
                # Use different colors for different regions (defined in __init__)
                colors = [self.region_colors.get(region, '#f8f8f2') for region in metrics_df['Region']]
            else:
                # Use metric-based colors for single region
                if selected_metric in ['Discharge Revenue', 'Discharge Energy', 'Price Spread']:
                    base_color = '#50fa7b' if selected_metric != 'Price Spread' else None
                    if selected_metric == 'Price Spread':
                        # Use green for positive, red for negative
                        colors = ['#50fa7b' if x > 0 else '#ff5555' for x in metrics_df[selected_metric]]
                    else:
                        colors = [base_color] * len(metrics_df)
                elif selected_metric in ['Charge Cost', 'Charge Energy']:
                    colors = ['#ff5555'] * len(metrics_df)
                else:
                    colors = ['#8be9fd'] * len(metrics_df)
            
            # Format values for display with zero decimal places
            if selected_metric in ['Discharge Revenue', 'Charge Cost']:
                # Format as currency with no decimals
                metrics_df['Formatted Value'] = metrics_df[selected_metric].apply(
                    lambda x: f"${x/1e6:.0f}M" if abs(x) >= 1e6 else f"${x/1e3:.0f}K"
                )
            elif selected_metric in ['Discharge Energy', 'Charge Energy']:
                # Format as MWh with no decimals
                metrics_df['Formatted Value'] = metrics_df[selected_metric].apply(
                    lambda x: f"{x:,.0f} MWh"
                )
            elif selected_metric in ['Discharge Price', 'Charge Price', 'Price Spread']:
                # Format as $/MWh with no decimals
                metrics_df['Formatted Value'] = metrics_df[selected_metric].apply(
                    lambda x: f"${x:.0f}/MWh"
                )
            else:
                metrics_df['Formatted Value'] = metrics_df[selected_metric].round(0).astype(int).astype(str)
            
            # Create lollipop chart using matplotlib
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
            
            fig, ax = plt.subplots(figsize=(12, 6), facecolor='#282a36')
            ax.set_facecolor('#282a36')
            
            # X positions
            x_pos = range(len(metrics_df))
            y_values = metrics_df[selected_metric].values
            
            # Draw stems (vertical lines)
            for i, y in enumerate(y_values):
                ax.plot([i, i], [0, y], color='#666666', linewidth=2, alpha=0.7)
            
            # Draw dots
            ax.scatter(x_pos, y_values, s=150, c=colors, zorder=5, edgecolors='white', linewidth=0.5)
            
            # Set x-axis labels
            ax.set_xticks(x_pos)
            ax.set_xticklabels(metrics_df['Display Name'].values, rotation=45, ha='right', color='#f8f8f2')
            
            # Set y-axis label and format
            ax.set_ylabel(selected_metric, color='#f8f8f2', fontsize=12)
            
            # Format y-axis values based on metric type
            if selected_metric in ['Discharge Revenue', 'Charge Cost']:
                # Format as currency
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'${x/1e6:.1f}M' if abs(x) >= 1e6 else f'${x/1e3:.0f}K'
                ))
            elif selected_metric in ['Discharge Energy', 'Charge Energy']:
                # Format as MWh
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'{x:,.0f}'
                ))
            elif selected_metric in ['Price Spread']:
                # Format as $/MWh, handle negative values
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'${x:.0f}'
                ))
            else:
                # Format as $/MWh
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'${x:.0f}'
                ))
            
            # Title
            ax.set_title(f'Top 20 Batteries by {selected_metric}', 
                        color='#f8f8f2', fontsize=14, pad=20)
            
            # Grid
            ax.grid(True, axis='y', alpha=0.2, color='#44475a')
            ax.set_axisbelow(True)
            
            # Add horizontal line at y=0
            ax.axhline(y=0, color='#44475a', linewidth=1, alpha=0.5)
            
            # Tick colors
            ax.tick_params(colors='#f8f8f2')
            
            # Spine colors
            for spine in ax.spines.values():
                spine.set_edgecolor('#44475a')
                spine.set_linewidth(1)
            
            # Add value labels with alternating heights to avoid overlap
            for i, (val, fmt_val) in enumerate(zip(y_values, metrics_df['Formatted Value'].values)):
                # Alternate between two different vertical offsets for positive values
                if val >= 0:
                    # Even indices get normal position, odd indices get higher position
                    if i % 2 == 0:
                        offset = val * 0.02 if val > 0 else 5  # Small offset above the dot
                    else:
                        offset = val * 0.08 if val > 0 else 15  # Larger offset for odd indices
                    ax.text(i, val + offset, fmt_val, ha='center', va='bottom', color='#f8f8f2', 
                           fontsize=8, fontweight='bold')
                else:
                    # For negative values, place below
                    offset = val * 0.05 if val < 0 else -5
                    ax.text(i, val + offset, fmt_val, ha='center', va='top', color='#f8f8f2', 
                           fontsize=8, fontweight='bold')
            
            # Add legend if NEM is selected to show region colors
            if selected_region == 'NEM':
                # Create custom legend entries for regions (excluding NEM itself)
                from matplotlib.patches import Patch
                legend_elements = []
                for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
                    if region in self.region_colors:
                        legend_elements.append(
                            Patch(facecolor=self.region_colors[region], 
                                  edgecolor='none', label=region)
                        )
                
                # Add legend to the plot
                if legend_elements:
                    ax.legend(handles=legend_elements, 
                             loc='upper right',
                             frameon=True,
                             fancybox=True,
                             shadow=False,
                             facecolor='#282a36',
                             edgecolor='#44475a',
                             labelcolor='#f8f8f2',
                             fontsize=9,
                             title='Regions',
                             title_fontsize=10)
                    # Style the legend title
                    legend = ax.get_legend()
                    if legend:
                        legend.get_title().set_color('#f8f8f2')
            
            # Add attribution at bottom right
            ax.text(0.99, 0.01, 'Data: AEMO, Plot: ITK', 
                   transform=ax.transAxes, ha='right', va='bottom',
                   fontsize=8, color='#6272a4', alpha=0.8)
            
            # Tight layout
            plt.tight_layout()
            
            # Convert matplotlib figure to Panel pane and update the container
            self.battery_content_pane.clear()
            self.battery_content_pane.append(pn.pane.Matplotlib(fig, tight=True, dpi=100))
            
            # Update info pane with summary statistics
            selected_regions_text = self.region_selector.value
            total_batteries = len(metrics_df)
            
            if selected_metric in ['Discharge Revenue', 'Charge Cost']:
                total_value = metrics_df[selected_metric].sum()
                total_text = f"${total_value/1e6:.2f}M" if abs(total_value) >= 1e6 else f"${total_value/1e3:.1f}K"
                avg_value = metrics_df[selected_metric].mean()
                avg_text = f"${avg_value/1e6:.2f}M" if abs(avg_value) >= 1e6 else f"${avg_value/1e3:.1f}K"
            else:
                total_value = metrics_df[selected_metric].sum()
                avg_value = metrics_df[selected_metric].mean()
                if selected_metric in ['Discharge Energy', 'Charge Energy']:
                    total_text = f"{total_value:,.0f} MWh"
                    avg_text = f"{avg_value:,.0f} MWh"
                else:
                    total_text = f"{total_value:.2f}"
                    avg_text = f"{avg_value:.2f}"
            
            info_html = f"""
            <div style="background-color: #1a1a1a; border: 2px solid #bd93f9; padding: 15px; border-radius: 10px;">
                <h3 style="color: #bd93f9; margin-top: 0;">Battery Analysis Summary</h3>
                <table style="color: #e0e0e0; width: 100%;">
                    <tr><td><strong>Regions:</strong></td><td>{selected_regions_text}</td></tr>
                    <tr><td><strong>Date Range:</strong></td><td>{self.start_date_picker.value} to {self.end_date_picker.value}</td></tr>
                    <tr><td><strong>Metric:</strong></td><td>{selected_metric}</td></tr>
                    <tr><td><strong>Batteries Shown:</strong></td><td>{total_batteries} (top performers)</td></tr>
                    <tr><td><strong>Total {selected_metric}:</strong></td><td>{total_text}</td></tr>
                    <tr><td><strong>Average {selected_metric}:</strong></td><td>{avg_text}</td></tr>
                </table>
            </div>
            """
            self.battery_info_pane.object = info_html
            
            logger.info(f"Lollipop chart created for {total_batteries} batteries")
            
        except Exception as e:
            logger.error(f"Error creating lollipop chart: {e}")
            self.battery_info_pane.object = f"""
            <div style="background-color: #1a1a1a; border: 2px solid #ff5555; padding: 15px; border-radius: 10px;">
                <p style="color: #ff5555;">Error creating chart: {str(e)}</p>
            </div>
            """
            self.battery_content_pane.clear()
    
    def _setup_volatility_controls(self):
        """Set up volatility chart specific controls"""
        # Volatility chart pane
        self.volatility_chart_pane = pn.pane.HoloViews(
            object=None,
            sizing_mode='stretch_width',
            height=500
        )
        
        # Don't connect callbacks here - chart should only update on button click
        # Removed automatic watches on region_selector, volatility_window_selector, and log_scale_checkbox
        
        # Delay initial chart creation to avoid hanging on tab load
        # Chart will be created when user clicks update button
        self._show_placeholder_message()
    
    def _show_placeholder_message(self):
        """Show placeholder message for volatility chart"""
        # Create a simple plot with message
        import pandas as pd
        empty_df = pd.DataFrame({'x': [0], 'y': [0]})
        placeholder = empty_df.hvplot.scatter(
            x='x', y='y', 
            size=0,  # Make points invisible
            label=''
        ).opts(
            width=1000,
            height=500,
            bgcolor='#2B2B3B',
            title='Select a region and smoothing window to generate the volatility chart',
            fontsize={'title': 14},
            toolbar=None,
            xaxis=None,
            yaxis=None
        )
        self.volatility_chart_pane.object = placeholder
    
    def _calculate_rolling_statistics(self, df: pd.DataFrame, window_days: int) -> pd.DataFrame:
        """Calculate rolling mean and standard deviation for volatility bands"""
        # Convert to numeric window (30-minute periods per day = 48)
        window = window_days * 48
        
        # Calculate rolling statistics
        df['rolling_mean'] = df['rrp'].rolling(window=window, center=True).mean()
        df['rolling_std'] = df['rrp'].rolling(window=window, center=True).std()
        
        # Calculate bands
        df['upper_1std'] = df['rolling_mean'] + df['rolling_std']
        df['lower_1std'] = df['rolling_mean'] - df['rolling_std']
        df['upper_2std'] = df['rolling_mean'] + 2 * df['rolling_std']
        df['lower_2std'] = df['rolling_mean'] - 2 * df['rolling_std']
        
        return df
    
    def _apply_loess_with_bands(self, df: pd.DataFrame, window_days: int) -> pd.DataFrame:
        """Apply LOESS smoothing and calculate volatility bands"""
        if not HAS_LOESS:
            logger.warning("LOESS not available, falling back to rolling statistics")
            return self._calculate_rolling_statistics(df, window_days)
        
        # Since the data is already 30-minute, we can work with it directly
        # For better performance with 5 years of data, downsample to daily for LOESS
        logger.info(f"Applying LOESS to {len(df)} 30-minute data points")
        
        # Downsample to daily for LOESS calculation (48x reduction)
        df_daily = df.set_index('settlementdate').resample('1D').agg({
            'rrp': 'mean',
            'regionid': 'first'
        }).reset_index()
        
        logger.info(f"Downsampled to {len(df_daily)} daily points for LOESS")
        
        # Convert datetime to numeric for LOESS
        date_numeric = pd.to_datetime(df_daily['settlementdate']).astype(np.int64) / 1e9
        
        # Calculate fraction based on window size
        # For better trend capture, use a larger fraction
        total_days = (df_daily['settlementdate'].max() - df_daily['settlementdate'].min()).days
        # Make fraction larger to capture trends better - at least 0.05
        frac = min(0.5, max(0.05, window_days * 2 / total_days))
        
        logger.info(f"Applying LOESS with frac={frac:.4f} on {len(df_daily)} daily points for {window_days}-day smoothing")
        
        # Apply LOESS to price
        smoothed_price = lowess(
            df_daily['rrp'].values,
            date_numeric,
            frac=frac,
            it=0,
            return_sorted=False
        )
        
        df_daily['smoothed_price'] = smoothed_price
        
        # Calculate residuals on original 30-minute data for accurate volatility
        # Interpolate smoothed daily prices back to 30-minute resolution
        df = df.sort_values('settlementdate')
        df['smoothed_price'] = np.interp(
            pd.to_datetime(df['settlementdate']).astype(np.int64),
            pd.to_datetime(df_daily['settlementdate']).astype(np.int64),
            df_daily['smoothed_price'].values
        )
        
        # Calculate residuals
        df['residual'] = df['rrp'] - df['smoothed_price']
        
        # Calculate rolling standard deviation of residuals on 30-minute data
        window = window_days * 48  # 48 half-hour periods per day
        df['rolling_std'] = df['residual'].rolling(window=window, center=True).std()
        
        # Smooth the standard deviation using daily aggregation
        df_daily_std = df.set_index('settlementdate').resample('1D').agg({
            'rolling_std': 'mean'
        }).reset_index()
        
        # Apply LOESS to the rolling std
        smoothed_std = lowess(
            df_daily_std['rolling_std'].ffill().bfill().values,
            pd.to_datetime(df_daily_std['settlementdate']).astype(np.int64) / 1e9,
            frac=frac,
            it=0,
            return_sorted=False
        )
        
        # Interpolate smoothed std back to 30-minute resolution
        df['smoothed_std'] = np.interp(
            pd.to_datetime(df['settlementdate']).astype(np.int64),
            pd.to_datetime(df_daily_std['settlementdate']).astype(np.int64),
            smoothed_std
        )
        
        # Calculate bands
        df['upper_1std'] = df['smoothed_price'] + df['smoothed_std']
        df['lower_1std'] = df['smoothed_price'] - df['smoothed_std']
        df['upper_2std'] = df['smoothed_price'] + 2 * df['smoothed_std']
        df['lower_2std'] = df['smoothed_price'] - 2 * df['smoothed_std']
        
        logger.info("LOESS calculation complete")
        return df
    
    def _update_volatility_chart(self, event=None):
        """Update the volatility analysis chart"""
        try:
            # Get all selected regions
            if not self.region_selector.value:
                logger.warning("No region selected")
                self._show_placeholder_message()
                return
            
            selected_regions = self.region_selector.value  # Use all selected regions
            logger.info(f"Starting volatility chart creation for regions: {selected_regions}, window: {self.volatility_window_selector.value}")
            
            # Load all available price data (5 years)
            start_date = datetime(2020, 1, 1)
            end_date = datetime.now()
            logger.info(f"Loading price data from {start_date} to {end_date}")
            
            # Load price data using the function
            price_data = load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min'
            )
            
            if price_data.empty:
                logger.warning("No price data available")
                self.volatility_chart_pane.object = None
                return
            
            # Reset index if SETTLEMENTDATE is the index
            if price_data.index.name == 'SETTLEMENTDATE':
                price_data = price_data.reset_index()
            
            # Check column names
            if 'REGIONID' in price_data.columns:
                region_col = 'REGIONID'
                price_col = 'RRP'
                date_col = 'SETTLEMENTDATE'
            else:
                region_col = 'regionid'
                price_col = 'rrp'
                date_col = 'settlementdate'
            
            # Extract window days from selector
            window_map = {
                '7 days': 7,
                '30 days': 30,
                '90 days': 90,
                '180 days': 180
            }
            window_days = window_map[self.volatility_window_selector.value]
            
            # Process each selected region and collect plots
            all_plots = []
            region_colors = {
                'NSW1': 'white',
                'QLD1': 'cyan',
                'SA1': 'yellow',
                'TAS1': 'green',
                'VIC1': 'orange'
            }
            
            mean_prices = {}  # Store mean prices for each region
            
            for idx, region in enumerate(selected_regions):
                # Filter data for this region
                region_data = price_data[price_data[region_col] == region].copy()
                logger.info(f"Filtered to {len(region_data)} records for region {region}")
                
                # Standardize column names
                region_data = region_data.rename(columns={
                    date_col: 'settlementdate',
                    region_col: 'regionid',
                    price_col: 'rrp'
                })
                
                if region_data.empty:
                    logger.warning(f"No data for region {region}")
                    continue
                
                # Sort by date
                region_data = region_data.sort_values('settlementdate')
                
                # Apply LOESS smoothing with bands
                logger.info(f"Applying smoothing for {region} with window={window_days} days")
                if HAS_LOESS:
                    region_data = self._apply_loess_with_bands(region_data, window_days)
                    price_col_to_use = 'smoothed_price'
                else:
                    region_data = self._calculate_rolling_statistics(region_data, window_days)
                    price_col_to_use = 'rolling_mean'
                
                # Drop rows with NaN values for clean plotting
                plot_data = region_data.dropna(subset=[price_col_to_use, 'upper_2std', 'lower_2std'])
                
                if plot_data.empty:
                    logger.warning(f"No data after processing for {region}")
                    continue
                
                # Calculate and store mean price
                mean_prices[region] = region_data['rrp'].mean()
                
                # Get color for this region
                color = region_colors.get(region, 'white')
                
                # Create bands for each region with matching colors
                # 2 std band (lighter shade)
                area_2std = plot_data.hvplot.area(
                    x='settlementdate',
                    y='lower_2std',
                    y2='upper_2std',
                    label=f'{region} ±2σ',
                    color=color,
                    alpha=0.1,  # Very light for outer band
                    hover=False
                )
                all_plots.append(area_2std)
                
                # 1 std band (darker shade)
                area_1std = plot_data.hvplot.area(
                    x='settlementdate',
                    y='lower_1std',
                    y2='upper_1std',
                    label=f'{region} ±1σ',
                    color=color,
                    alpha=0.2,  # Slightly darker for inner band
                    hover=False
                )
                all_plots.append(area_1std)
                
                # Create the price line for this region
                price_line = plot_data.hvplot.line(
                    x='settlementdate',
                    y=price_col_to_use,
                    label=f'{region}',
                    color=color,
                    line_width=2,
                    hover=True
                )
                all_plots.append(price_line)
            
            if not all_plots:
                logger.warning("No plots created")
                self.volatility_chart_pane.object = None
                return
            
            # Combine all plots
            combined_plot = all_plots[0]
            for plot in all_plots[1:]:
                combined_plot = combined_plot * plot
            
            # Create title with mean prices
            title_parts = [f'Price Volatility Analysis ({self.volatility_window_selector.value} smoothing)']
            if mean_prices:
                avg_text = ' - '.join([f'{r}: ${p:.0f}' for r, p in mean_prices.items()])
                title_parts.append(f'Avg: {avg_text}')
            title = ' - '.join(title_parts)
            
            # Apply log scale if selected
            if self.log_scale_checkbox.value:
                # Symlog transformation not implemented for multi-region yet
                # For now, just add symlog to title
                title = title + ' - Symlog Scale'
                
                plot_opts = dict(
                    width=1000,
                    height=500,
                    bgcolor='#2B2B3B',
                    title=title,
                    xlabel='Date',
                    ylabel='Price ($/MWh) - Symlog Scale',
                    fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
                    show_grid=True,
                    gridstyle={'grid_line_alpha': 0.3},
                    toolbar='above',
                    legend_position='top_right',
                    framewise=True,
                    yformatter='%.0f',
                    logy=True  # Use built-in log scale
                )
            else:
                plot_opts = dict(
                    width=1000,
                    height=500,
                    bgcolor='#2B2B3B',
                    title=title,
                    xlabel='Date',
                    ylabel='Price ($/MWh)',
                    fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
                    show_grid=True,
                    gridstyle={'grid_line_alpha': 0.3},
                    toolbar='above',
                    legend_position='top_right',
                    framewise=True,
                    yformatter='%.0f',
                    ylim=(None, None)  # Allow negative values
                )
            
            # Style the plot
            final_plot = combined_plot.opts(**plot_opts)
            
            self.volatility_chart_pane.object = final_plot
            logger.info(f"Volatility chart created successfully for regions: {selected_regions}")
            
        except Exception as e:
            logger.error(f"Error creating volatility chart: {e}")
            import traceback
            traceback.print_exc()
            self.volatility_chart_pane.object = None
    
    def _generate_initial_table(self):
        """Generate comparison table for all regions on initial load"""
        all_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        self._create_comparison_table(all_regions)
    
    def _update_comparison_table(self):
        """Create comparison table for selected regions"""
        try:
            if not self.region_selector.value:
                self.comparison_table_pane.object = "<p>No regions selected</p>"
                return
            
            selected_regions = self.region_selector.value
            self._create_comparison_table(selected_regions)
        except Exception as e:
            logger.error(f"Error updating comparison table: {e}")
            import traceback
            traceback.print_exc()
    
    def _create_comparison_table(self, regions):
        """Create comparison table for specified regions"""
        try:
            logger.info(f"Creating comparison table for regions: {regions}")
            
            # Define time periods
            first_year_start = datetime(2020, 1, 1)
            first_year_end = datetime(2020, 12, 31, 23, 59, 59)
            
            yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            last_year_start = yesterday - timedelta(days=365)
            last_year_end = yesterday
            
            # Create table data
            table_data = []
            
            for region in regions:
                # Get price data for 2020
                price_2020 = load_price_data(
                    start_date=first_year_start,
                    end_date=first_year_end,
                    regions=[region],
                    resolution='30min'
                )
                
                # Get price data for last 12 months
                price_last12 = load_price_data(
                    start_date=last_year_start,
                    end_date=last_year_end,
                    regions=[region],
                    resolution='30min'
                )
                
                # Calculate price statistics
                if not price_2020.empty:
                    if price_2020.index.name == 'SETTLEMENTDATE':
                        price_2020 = price_2020.reset_index()
                    price_col = 'RRP' if 'RRP' in price_2020.columns else 'rrp'
                    avg_2020 = price_2020[price_col].mean()
                    std_2020 = price_2020[price_col].std()
                else:
                    avg_2020 = 0
                    std_2020 = 0
                
                if not price_last12.empty:
                    if price_last12.index.name == 'SETTLEMENTDATE':
                        price_last12 = price_last12.reset_index()
                    price_col = 'RRP' if 'RRP' in price_last12.columns else 'rrp'
                    avg_last12 = price_last12[price_col].mean()
                    std_last12 = price_last12[price_col].std()
                else:
                    avg_last12 = 0
                    std_last12 = 0
                
                # Get generation data for VRE calculation using query manager
                logger.info(f"Loading generation data for {region} - 2020")
                gen_2020 = self.query_manager.query_generation_by_fuel(
                    start_date=first_year_start,
                    end_date=first_year_end,
                    region=region,
                    resolution='30min'
                )
                
                logger.info(f"Loading generation data for {region} - last 12 months")
                gen_last12 = self.query_manager.query_generation_by_fuel(
                    start_date=last_year_start,
                    end_date=last_year_end,
                    region=region,
                    resolution='30min'
                )
                
                # Also load rooftop solar data
                from aemo_dashboard.shared.rooftop_adapter import load_rooftop_data
                
                rooftop_2020 = load_rooftop_data(
                    start_date=first_year_start,
                    end_date=first_year_end
                )
                
                rooftop_last12 = load_rooftop_data(
                    start_date=last_year_start,
                    end_date=last_year_end
                )
                
                # Calculate VRE share (including rooftop)
                vre_share_2020 = self._calculate_vre_share(gen_2020, region, rooftop_2020)
                vre_share_last12 = self._calculate_vre_share(gen_last12, region, rooftop_last12)
                
                # Calculate coefficient of variation (CV)
                cv_2020 = (std_2020 / avg_2020 * 100) if avg_2020 > 0 else 0
                cv_last12 = (std_last12 / avg_last12 * 100) if avg_last12 > 0 else 0
                
                # Add row data with no decimal places
                table_data.append({
                    'Region': region,
                    'Period': '2020',
                    'Avg Price ($/MWh)': f"${avg_2020:.0f}",
                    'Variability* (%)': f"{cv_2020:.0f}%",
                    'VRE Share (%)': f"{vre_share_2020:.0f}%"
                })
                
                table_data.append({
                    'Region': region,
                    'Period': f'Last 12mo',
                    'Avg Price ($/MWh)': f"${avg_last12:.0f}",
                    'Variability* (%)': f"{cv_last12:.0f}%",
                    'VRE Share (%)': f"{vre_share_last12:.0f}%"
                })
            
            # Create HTML table
            if table_data:
                df_table = pd.DataFrame(table_data)
                
                # Calculate table width based on number of regions
                table_width = 250 + (len(regions) * 140)  # Base width + width per region
                
                # Pivot to get regions as column groups
                html = '<div style="margin: 20px 0;">'
                html += '<h3 style="color: #008B8B;">Regional Comparison: 2020 vs Last 12 Months</h3>'
                html += f'<table style="border-collapse: collapse; width: {table_width}px; font-size: 13px;">'
                
                # Header row
                html += '<thead><tr style="background-color: #2B2B3B;">'
                html += '<th style="border: 1px solid #555; padding: 5px 8px; text-align: left;">Metric</th>'
                for region in regions:
                    html += f'<th colspan="2" style="border: 1px solid #555; padding: 5px 8px; text-align: center;">{region}</th>'
                html += '</tr>'
                
                # Sub-header row
                html += '<tr style="background-color: #3B3B4B;">'
                html += '<th style="border: 1px solid #555; padding: 5px 8px;"></th>'
                for region in regions:
                    html += '<th style="border: 1px solid #555; padding: 5px 8px; text-align: center; font-size: 12px;">2020</th>'
                    html += '<th style="border: 1px solid #555; padding: 5px 8px; text-align: center; font-size: 12px;">Last 12mo</th>'
                html += '</tr></thead>'
                
                # Data rows
                html += '<tbody>'
                metrics = ['Avg Price ($/MWh)', 'Variability* (%)', 'VRE Share (%)']
                
                for metric in metrics:
                    html += '<tr>'
                    html += f'<td style="border: 1px solid #555; padding: 5px 8px; font-weight: bold; font-size: 12px;">{metric}</td>'
                    
                    for region in regions:
                        # Get 2020 value
                        val_2020 = df_table[(df_table['Region'] == region) & (df_table['Period'] == '2020')][metric].values[0]
                        val_last12 = df_table[(df_table['Region'] == region) & (df_table['Period'] == 'Last 12mo')][metric].values[0]
                        
                        html += f'<td style="border: 1px solid #555; padding: 5px 8px; text-align: right; font-size: 12px;">{val_2020}</td>'
                        html += f'<td style="border: 1px solid #555; padding: 5px 8px; text-align: right; font-size: 12px;">{val_last12}</td>'
                    
                    html += '</tr>'
                
                html += '</tbody></table>'
                html += '<p style="font-size: 11px; color: #999; margin-top: 5px; font-style: italic;">* Variability = Coefficient of Variation (CV) = Standard Deviation / Mean × 100%</p>'
                html += '</div>'
                
                self.comparison_table_pane.object = html
                logger.info("Comparison table created successfully")
            else:
                self.comparison_table_pane.object = "<p>No data available for comparison</p>"
                
        except Exception as e:
            logger.error(f"Error creating comparison table: {e}")
            import traceback
            traceback.print_exc()
            self.comparison_table_pane.object = f"<p>Error creating table: {str(e)}</p>"
    
    def _calculate_vre_share(self, gen_data: pd.DataFrame, region: str, rooftop_data: pd.DataFrame = None) -> float:
        """Calculate VRE share for a specific region including rooftop solar"""
        try:
            # If gen_data already has fuel_type aggregations (from query_generation_by_fuel)
            if not gen_data.empty and 'fuel_type' in gen_data.columns:
                # Data is already aggregated by fuel type
                # Group by fuel type and calculate average generation
                fuel_averages = gen_data.groupby('fuel_type')['total_generation_mw'].mean()
                
                # Calculate total generation average
                total_gen_mw = fuel_averages.sum()
                
                # Calculate VRE average (Wind + Solar)
                vre_fuels = ['Wind', 'Solar']
                vre_mw = fuel_averages[fuel_averages.index.isin(vre_fuels)].sum()
                
                logger.info(f"Utility-scale generation for {region}: Total avg: {total_gen_mw:.1f} MW, VRE avg: {vre_mw:.1f} MW")
                
                # Add rooftop if available
                if rooftop_data is not None and not rooftop_data.empty:
                    if region == 'NEM':
                        # For NEM, sum all regions
                        region_cols = [col for col in rooftop_data.columns if col != 'settlementdate']
                        rooftop_avg = rooftop_data[region_cols].sum(axis=1).mean()
                    elif region in rooftop_data.columns:
                        rooftop_avg = rooftop_data[region].mean()
                    else:
                        rooftop_avg = 0.0
                    
                    # Add rooftop to totals
                    vre_mw += rooftop_avg
                    total_gen_mw += rooftop_avg
                    logger.info(f"Added rooftop solar: {rooftop_avg:.1f} MW average")
                
                # Calculate VRE share
                if total_gen_mw > 0:
                    vre_share = (vre_mw / total_gen_mw) * 100
                    logger.info(f"VRE share for {region}: {vre_share:.1f}% (VRE: {vre_mw:.0f} MW, Total: {total_gen_mw:.0f} MW)")
                    return vre_share
                else:
                    return 0.0
            
            # Fallback to old method if data is not aggregated by fuel type
            # Use a simpler approach - calculate average MW for the period
            total_vre_mw = 0.0
            total_gen_mw = 0.0
            
            # First handle utility-scale generation
            if not gen_data.empty:
                # Load DUID mapping to get region and fuel type information
                import pickle
                gen_info_path = self.config.gen_info_file
                
                if not gen_info_path.exists():
                    logger.warning(f"Gen info file not found: {gen_info_path}")
                    # Try without region filtering - just use fuel types
                    if 'fuel_type' in gen_data.columns or 'FUEL_TYPE' in gen_data.columns:
                        fuel_col = 'fuel_type' if 'fuel_type' in gen_data.columns else 'FUEL_TYPE'
                        value_col = 'scadavalue' if 'scadavalue' in gen_data.columns else 'SCADAVALUE'
                        
                        # Calculate averages for all generation
                        total_gen_mw = gen_data[value_col].mean()
                        
                        # Calculate VRE average
                        vre_fuels = ['Wind', 'Solar', 'Solar Utility', 'Wind Onshore']
                        vre_data = gen_data[gen_data[fuel_col].isin(vre_fuels)]
                        if not vre_data.empty:
                            total_vre_mw = vre_data[value_col].mean()
                        
                        logger.info(f"Using fuel type data without region filter - VRE: {total_vre_mw:.0f} MW, Total: {total_gen_mw:.0f} MW")
                else:
                    with open(gen_info_path, 'rb') as f:
                        gen_info = pickle.load(f)
                    
                    # Create DUID to region and fuel type mapping
                    duid_info = {}
                    for _, row in gen_info.iterrows():
                        duid = row.get('DUID', row.get('duid', ''))
                        if duid:
                            duid_info[duid] = {
                                'region': row.get('REGIONID', row.get('regionid', row.get('Region ID', ''))),
                                'fuel': row.get('Fuel Source - Descriptor', row.get('fuel_type', row.get('Fuel Source', '')))
                            }
                    
                    # Add region and fuel type to generation data
                    if 'duid' in gen_data.columns:
                        duid_col = 'duid'
                    elif 'DUID' in gen_data.columns:
                        duid_col = 'DUID'
                    else:
                        logger.warning("No DUID column found in generation data")
                        return 0.0
                    
                    gen_data['region'] = gen_data[duid_col].map(lambda x: duid_info.get(x, {}).get('region', ''))
                    gen_data['fuel_type'] = gen_data[duid_col].map(lambda x: duid_info.get(x, {}).get('fuel', ''))
                    
                    # Filter for region
                    region_data = gen_data[gen_data['region'] == region].copy()
                    
                    if not region_data.empty:
                        # Get value column
                        value_col = 'scadavalue' if 'scadavalue' in region_data.columns else 'SCADAVALUE'
                        
                        # Calculate average MW for ALL fuel types
                        total_gen_mw = region_data[value_col].mean()
                        
                        # Calculate average MW for VRE only
                        vre_fuels = ['Wind', 'Solar', 'Solar Utility', 'Wind Onshore']
                        vre_data = region_data[region_data['fuel_type'].isin(vre_fuels)]
                        if not vre_data.empty:
                            total_vre_mw = vre_data[value_col].mean()
                        
                        logger.info(f"Utility generation for {region} - VRE avg: {total_vre_mw:.0f} MW, Total avg: {total_gen_mw:.0f} MW")
            
            # Add rooftop solar if available
            if rooftop_data is not None and not rooftop_data.empty:
                # Rooftop data is in wide format with regions as columns
                if region in rooftop_data.columns:
                    # Calculate average rooftop generation for this region
                    rooftop_avg_mw = rooftop_data[region].mean()
                    
                    if not pd.isna(rooftop_avg_mw) and rooftop_avg_mw > 0:
                        # Add rooftop to both VRE and total
                        total_vre_mw += rooftop_avg_mw
                        total_gen_mw += rooftop_avg_mw
                        logger.info(f"Rooftop solar for {region}: avg {rooftop_avg_mw:.0f} MW")
                    else:
                        logger.info(f"No rooftop solar data for {region}")
                else:
                    logger.warning(f"Region {region} not found in rooftop data columns: {list(rooftop_data.columns)}")
            
            # Calculate final VRE share
            if total_gen_mw > 0:
                vre_share = (total_vre_mw / total_gen_mw) * 100
                logger.info(f"Total VRE share for {region}: {vre_share:.1f}% (VRE avg: {total_vre_mw:.0f} MW, Total avg: {total_gen_mw:.0f} MW)")
                return vre_share
            else:
                logger.warning(f"No generation found for {region}")
                return 0.0
                
        except Exception as e:
            logger.error(f"Error calculating VRE share: {e}")
            import traceback
            traceback.print_exc()
            return 0.0
        
    def create_layout(self) -> pn.Column:
        """Create the complete batteries tab layout with subtabs"""
        
        # Create One BESS subtab content with controls on left, analysis on right
        # Left column - date and frequency controls
        bess_date_controls = pn.Column(
            "### Date Range",
            pn.Row(
                pn.Column(
                    "Start Date",
                    self.bess_start_date,
                    width=120
                ),
                pn.Column(
                    "End Date",
                    self.bess_end_date,
                    width=120
                ),
                align='start'
            ),
            pn.Spacer(height=10),
            "Quick Select",
            self.bess_date_presets,
            width=250
        )
        
        bess_freq_control = pn.Column(
            "### Frequency",
            self.bess_frequency,
            width=120
        )
        
        bess_options_control = pn.Column(
            "### Options",
            self.bess_log_scale,
            width=120
        )
        
        bess_left_controls = pn.Column(
            bess_date_controls,
            pn.Spacer(height=20),
            bess_freq_control,
            pn.Spacer(height=20),
            bess_options_control,
            width=300,
            margin=(0, 20, 0, 0)
        )
        
        # Right side - battery selection and analysis
        bess_selection_controls = pn.Row(
            self.bess_region_selector,
            pn.Spacer(width=20),
            self.bess_selector,
            pn.Spacer(width=20),
            self.bess_analyze_button,
            align='center'
        )
        
        # Create a row for chart and info table side by side
        bess_analysis_row = pn.Row(
            self.bess_chart_pane,  # Chart on the left
            pn.Spacer(width=20),
            pn.Column(self.bess_info_pane, width=400),  # Table on the right with fixed width
            sizing_mode='stretch_width'
        )
        
        bess_analysis_content = pn.Column(
            "### Battery Selection",
            bess_selection_controls,
            pn.Spacer(height=20),
            bess_analysis_row,  # Chart and table side by side
            sizing_mode='stretch_width'
        )
        
        one_bess_tab = pn.Row(
            bess_left_controls,
            bess_analysis_content,
            sizing_mode='stretch_both'
        )
        
        # Create Overview subtab content (original content)
        # Left column - all controls
        region_group = pn.Column(
            "### Region",
            self.region_selector,  # Use the simple selector without color styling
            align='start',
            width=120  # Standard width for radio buttons
        )
        
        frequency_group = pn.Column(
            "### Frequency",
            self.aggregate_selector,
            width=120
        )
        
        top_controls = pn.Row(
            region_group,
            pn.Spacer(width=10),
            frequency_group,
            align='start'
        )
        
        # Date controls in one row
        date_controls = pn.Row(
            pn.Column(
                "Start Date",
                self.start_date_picker,
                width=100
            ),
            pn.Column(
                "End Date", 
                self.end_date_picker,
                width=100
            ),
            pn.Column(
                "Quick Select",
                self.date_presets,
                width=100
            ),
            align='start'
        )
        
        # Combine all controls
        controls_column = pn.Column(
            "## Battery Analysis Controls",
            pn.Spacer(height=10),
            top_controls,
            pn.Spacer(height=15),
            "### Date Range",
            date_controls,
            self.date_display,
            pn.Spacer(height=15),
            "### Analysis Options",
            self.metric_selector,
            pn.Spacer(height=20),
            self.update_button,
            width=350,
            margin=(0, 20, 0, 0),
            align='start'
        )
        
        # Main content area - battery analysis with lollipop chart
        main_content = pn.Column(
            self.battery_info_pane,
            pn.Spacer(height=10),
            self.battery_content_pane,
            pn.Spacer(height=20),
            self.dynamic_content_pane,
            sizing_mode='stretch_width'
        )
        
        # Right side - content area for overview
        overview_content = pn.Column(
            main_content,
            sizing_mode='stretch_both'
        )
        
        # Complete overview tab layout - controls on left, content on right
        overview_tab = pn.Row(
            controls_column,
            overview_content,
            sizing_mode='stretch_both'
        )
        
        # Create subtabs
        battery_subtabs = pn.Tabs(
            ('Overview', overview_tab),
            ('One BESS', one_bess_tab),
            sizing_mode='stretch_both'
        )
        
        return battery_subtabs