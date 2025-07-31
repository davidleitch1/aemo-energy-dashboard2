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
        """Initialize the insights tab"""
        # Initialize config
        self.config = Config()
        
        # Initialize generation query manager for efficient data loading
        self.query_manager = GenerationQueryManager()
        
        # Initialize components
        self._setup_controls()
        self._setup_content_area()
        self._setup_volatility_controls()
        
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
        
        # Region checkbox group for multi-selection
        regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        self.region_selector = pn.widgets.CheckBoxGroup(
            name='',
            value=['NSW1'],
            options=regions,
            inline=False,
            align='start',
            margin=(0, 0, 0, 0)
        )
        
        # Smoothing window selector (moved from volatility controls)
        self.volatility_window_selector = pn.widgets.Select(
            name='Smoothing Window',
            value='30 days',
            options=['7 days', '30 days', '90 days', '180 days'],
            width=150
        )
        
        # Log scale checkbox
        self.log_scale_checkbox = pn.widgets.Checkbox(
            name='Log Scale',
            value=False,
            width=100
        )
        
        # Aggregate level radio buttons
        self.aggregate_selector = pn.widgets.RadioBoxGroup(
            name='',
            value='1 hour',
            options=['5 min', '1 hour', 'Daily', 'Monthly', 'Quarterly', 'Yearly'],
            inline=False,
            width=120
        )
        
        # Update button for loading new insights
        self.update_button = pn.widgets.Button(
            name='Update Insights',
            button_type='primary',
            width=150
        )
        
        # Set up callbacks
        self._setup_callbacks()
        
    def _setup_content_area(self):
        """Set up the content area with this week's value add"""
        # This week's value add HTML box
        self.value_add_pane = pn.pane.HTML(
            """
            <div style="
                background-color: #1a1a1a;
                border: 2px solid #666;
                border-radius: 10px;
                padding: 20px;
                margin: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
                color: #ffffff;
            ">
                <h2 style="color: #00BFFF; margin-top: 0;">This Week's Value Add</h2>
                <p style="font-size: 16px; line-height: 1.6; color: #e0e0e0;">
                    <strong>Key Insight:</strong> Renewable penetration reached a new record of 72% 
                    during the midday period on July 21, 2025, driven by exceptional solar generation 
                    and mild weather conditions.
                </p>
                <ul style="font-size: 15px; line-height: 1.8; color: #e0e0e0;">
                    <li>🌞 Solar generation peaked at 14.2 GW at 12:30 PM</li>
                    <li>💨 Wind contributed a steady 4.5 GW throughout the day</li>
                    <li>⚡ Spot prices went negative in SA and VIC for 3 hours</li>
                    <li>🔋 Battery storage provided 1.8 GW of evening peak support</li>
                </ul>
                <p style="font-style: italic; color: #999; margin-top: 15px;">
                    Last updated: July 23, 2025
                </p>
            </div>
            """,
            width=400,
            height=250
        )
        
        # Placeholder for future dynamic content
        self.dynamic_content_pane = pn.pane.Markdown(
            "",
            width=600
        )
        
        # Comparison table pane
        self.comparison_table_pane = pn.pane.HTML(
            "<p>Loading comparison table for all regions...</p>",
            sizing_mode='stretch_width',
            height=300
        )
        
        # Generate initial comparison table for all regions after a short delay
        # Only add periodic callback if we're in a server context
        try:
            pn.state.add_periodic_callback(self._generate_initial_table, period=500, count=1)
        except RuntimeError:
            # Not in a server context (e.g., testing), call directly
            self._generate_initial_table()
        
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
            """Update insights based on current selections"""
            logger.info(f"Updating insights for regions: {self.region_selector.value}")
            logger.info(f"Date range: {self.start_date_picker.value} to {self.end_date_picker.value}")
            
            # Clear the dynamic content pane
            self.dynamic_content_pane.object = ""
            
            # Update the volatility chart
            self._update_volatility_chart()
            
            # Update the comparison table
            self._update_comparison_table()
        
        # Connect callbacks
        self.date_presets.param.watch(update_date_range, 'value')
        self.start_date_picker.param.watch(update_date_display, 'value')
        self.end_date_picker.param.watch(update_date_display, 'value')
        self.update_button.on_click(update_insights)
    
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
        """Create the complete insights tab layout"""
        # Left column - all controls
        region_group = pn.Column(
            "### Region",
            self.region_selector,
            align='start',
            width=120
        )
        
        frequency_group = pn.Column(
            "### Frequency",
            self.aggregate_selector,
            width=120
        )
        
        volatility_group = pn.Column(
            "### Volatility Analysis",
            self.volatility_window_selector,
            self.log_scale_checkbox,
            width=150
        )
        
        top_controls = pn.Row(
            region_group,
            pn.Spacer(width=10),
            frequency_group,
            pn.Spacer(width=10),
            volatility_group,
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
            "## Insights Analysis Controls",
            pn.Spacer(height=10),
            top_controls,
            pn.Spacer(height=15),
            "### Date Range",
            date_controls,
            self.date_display,
            pn.Spacer(height=20),
            self.update_button,
            width=350,
            margin=(0, 20, 0, 0),
            align='start'
        )
        
        # Volatility section - no controls here since they're in the left panel
        volatility_section = pn.Column(
            pn.pane.Markdown("## Price Volatility Analysis", styles={'color': '#008B8B'}),
            pn.Spacer(height=10),
            self.volatility_chart_pane,
            sizing_mode='stretch_width'
        )
        
        # Main content area - table first, then plot
        main_content = pn.Column(
            self.comparison_table_pane,
            pn.Spacer(height=20),
            volatility_section,
            sizing_mode='stretch_width'
        )
        
        # Right side - content area
        content_area = pn.Column(
            main_content,
            sizing_mode='stretch_both'
        )
        
        # Complete tab layout - controls on left, content on right
        insights_tab = pn.Row(
            controls_column,
            content_area,
            sizing_mode='stretch_both'
        )
        
        return insights_tab