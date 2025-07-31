#!/usr/bin/env python3
"""Test the Prices tab with hvplot functionality"""

import sys
import panel as pn
import pandas as pd
import holoviews as hv
from datetime import datetime, timedelta
import numpy as np

# Initialize Panel and HoloViews
pn.extension()
hv.extension('bokeh')

# Mock the logger
class MockLogger:
    def info(self, msg): print(f"INFO: {msg}")
    def error(self, msg): print(f"ERROR: {msg}")

logger = MockLogger()

# Test date range - 5 years of data
end_date = datetime.now().date()
start_date = end_date - timedelta(days=365*5)  # 5 years of data

# Create mock price data for testing
def create_mock_price_data(start_date, end_date, regions):
    """Create realistic mock price data with some negative values"""
    date_range = pd.date_range(start=start_date, end=end_date, freq='5min')
    data = []
    
    for region in regions:
        # Base price varies by region
        base_prices = {
            'NSW1': 80,
            'QLD1': 75,
            'SA1': 90,
            'TAS1': 70,
            'VIC1': 85
        }
        base = base_prices.get(region, 80)
        
        # Create realistic price patterns
        prices = base + 20 * np.sin(np.arange(len(date_range)) * 2 * np.pi / (288 * 7))  # Weekly pattern
        prices += 10 * np.sin(np.arange(len(date_range)) * 2 * np.pi / 288)  # Daily pattern
        prices += np.random.normal(0, 15, len(date_range))  # Random variation
        
        # Add some spikes and negative prices
        spike_indices = np.random.choice(len(date_range), size=int(len(date_range) * 0.001), replace=False)
        prices[spike_indices] = np.random.uniform(300, 1000, len(spike_indices))
        
        negative_indices = np.random.choice(len(date_range), size=int(len(date_range) * 0.0005), replace=False)
        prices[negative_indices] = np.random.uniform(-50, -10, len(negative_indices))
        
        for i, (date, price) in enumerate(zip(date_range, prices)):
            data.append({
                'SETTLEMENTDATE': date,
                'REGIONID': region,
                'RRP': price
            })
    
    return pd.DataFrame(data)

def create_prices_tab():
    """Create prices analysis tab with selectors and visualizations"""
    try:
        logger.info("Creating prices tab...")
        
        # Date preset radio buttons (vertical like frequency)
        date_presets = pn.widgets.RadioBoxGroup(
            name='',  # Empty name, we'll add label separately
            options=['1 day', '7 days', '30 days', '90 days', '1 year', 'All data'],
            value='30 days',
            inline=False,  # Vertical layout
            width=100
        )
        
        # Date pickers instead of slider (like generation tab)
        start_date_picker = pn.widgets.DatePicker(
            name='Start Date',
            value=end_date - pd.Timedelta(days=30),  # Default to 30 days ago
            start=start_date,
            end=end_date,
            width=150
        )
        
        end_date_picker = pn.widgets.DatePicker(
            name='End Date',
            value=end_date,
            start=start_date,
            end=end_date,
            width=150
        )
        
        # Show selected dates clearly
        date_display = pn.pane.Markdown(
            f"**Selected Period:** {start_date_picker.value.strftime('%Y-%m-%d')} to {end_date_picker.value.strftime('%Y-%m-%d')}",
            width=300
        )
        
        # Region checkbox group for multi-selection
        regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        region_selector = pn.widgets.CheckBoxGroup(
            name='Select Regions to Compare',
            value=['NSW1', 'VIC1'],  # Default selection
            options=regions,
            inline=False,  # Vertical layout
            width=250
        )
        
        # Aggregate level radio buttons (compact)
        aggregate_selector = pn.widgets.RadioBoxGroup(
            name='',
            value='1 hour',
            options=['5 min', '1 hour', 'Daily', 'Monthly', 'Quarterly', 'Yearly'],
            inline=False,  # Vertical for frequency options
            width=120
        )
        
        # Smoothing options
        smoothing_selector = pn.widgets.Select(
            name='Smoothing',
            value='None',
            options=['None', '7-period MA', '30-period MA', 'Exponential (α=0.3)'],
            width=200
        )
        
        # Add log scale checkbox
        log_scale_checkbox = pn.widgets.Checkbox(
            name='Log Scale Y-axis',
            value=False,
            width=150
        )
        
        # Create price plot pane
        price_plot_pane = pn.pane.HoloViews(
            height=400,
            sizing_mode='stretch_width'
        )
        
        # Initialize with loading message
        price_plot_pane.object = hv.Text(0.5, 0.5, 'Loading price data...').opts(
            xlim=(0, 1), ylim=(0, 1), 
            bgcolor='#282a36',  # Dracula background
            color='#f8f8f2',    # Dracula foreground
            fontsize=14
        )
        
        # Function to load and update price data
        def load_and_plot_prices(event=None):
            """Load price data and create hvplot"""
            try:
                # Get current selections
                selected_regions = region_selector.value
                if not selected_regions:
                    price_plot_pane.object = hv.Text(0.5, 0.5, 'Please select at least one region').opts(
                        xlim=(0, 1), ylim=(0, 1), 
                        bgcolor='#282a36', color='#f8f8f2', fontsize=14
                    )
                    return
                
                # Create mock data
                logger.info(f"Loading price data for regions: {selected_regions}")
                price_data = create_mock_price_data(
                    start_date_picker.value,
                    end_date_picker.value,
                    selected_regions
                )
                
                if price_data.empty:
                    price_plot_pane.object = hv.Text(0.5, 0.5, 'No data available for selected period').opts(
                        xlim=(0, 1), ylim=(0, 1), 
                        bgcolor='#282a36', color='#f8f8f2', fontsize=14
                    )
                    return
                
                # Handle negative prices for log scale
                use_log = log_scale_checkbox.value
                if use_log and (price_data['RRP'] <= 0).any():
                    # Option 1: Shift all values to make them positive
                    min_price = price_data['RRP'].min()
                    if min_price <= 0:
                        shift_value = abs(min_price) + 1
                        price_data['RRP_adjusted'] = price_data['RRP'] + shift_value
                        ylabel = f'Price ($/MWh) + {shift_value:.0f} [Log Scale]'
                        y_col = 'RRP_adjusted'
                    else:
                        y_col = 'RRP'
                        ylabel = 'Price ($/MWh) [Log Scale]'
                else:
                    y_col = 'RRP'
                    ylabel = 'Price ($/MWh)'
                
                # Resample based on frequency selection
                freq_map = {
                    '5 min': '5min',
                    '1 hour': 'h',
                    'Daily': 'D',
                    'Monthly': 'M',
                    'Quarterly': 'Q',
                    'Yearly': 'Y'
                }
                freq = freq_map.get(aggregate_selector.value, 'h')
                
                # Set SETTLEMENTDATE as index for resampling
                price_data = price_data.set_index('SETTLEMENTDATE')
                
                # Resample data
                if freq != '5min':  # Only resample if not 5 minute
                    price_data = price_data.groupby('REGIONID').resample(freq).agg({
                        y_col: 'mean',
                        'RRP': 'mean'  # Keep original for reference
                    }).reset_index()
                else:
                    price_data = price_data.reset_index()
                
                # Apply smoothing if selected
                if smoothing_selector.value != 'None':
                    for region in selected_regions:
                        region_mask = price_data['REGIONID'] == region
                        if smoothing_selector.value == '7-period MA':
                            price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].rolling(7, center=True).mean()
                        elif smoothing_selector.value == '30-period MA':
                            price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].rolling(30, center=True).mean()
                        elif smoothing_selector.value == 'Exponential (α=0.3)':
                            price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].ewm(alpha=0.3).mean()
                
                # Define Dracula theme colors for regions
                region_colors = {
                    'NSW1': '#8be9fd',  # Cyan
                    'QLD1': '#50fa7b',  # Green  
                    'SA1': '#ffb86c',   # Orange
                    'TAS1': '#ff79c6',  # Pink
                    'VIC1': '#bd93f9'   # Purple
                }
                
                # Create hvplot
                plot = price_data.hvplot.line(
                    x='SETTLEMENTDATE',
                    y=y_col,
                    by='REGIONID',
                    width=1200,
                    height=400,
                    xlabel='Time',
                    ylabel=ylabel,
                    title='Electricity Spot Prices by Region',
                    logy=use_log,
                    grid=True,
                    color=[region_colors.get(r, '#6272a4') for r in selected_regions],
                    line_width=2,
                    hover=True,
                    hover_cols=['REGIONID', 'RRP'],  # Show original price in hover
                    fontsize={'title': 14, 'labels': 12, 'ticks': 10}
                ).opts(
                    bgcolor='#282a36',  # Dracula background
                    toolbar='above',
                    active_tools=['pan', 'wheel_zoom'],
                    tools=['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset', 'save']
                )
                
                price_plot_pane.object = plot
                
            except Exception as e:
                logger.error(f"Error loading price data: {e}")
                price_plot_pane.object = hv.Text(0.5, 0.5, f'Error: {str(e)}').opts(
                    xlim=(0, 1), ylim=(0, 1), 
                    bgcolor='#282a36', color='#ff5555', fontsize=14
                )
        
        # Create placeholder for statistics table
        stats_table_placeholder = pn.pane.Markdown(
            "Statistics table will appear here...",
            height=150,
            sizing_mode='stretch_width'
        )
        
        # Create placeholder for price bands plot
        bands_plot_placeholder = pn.pane.Markdown(
            "Price bands plot will appear here...",
            height=300,
            width=400
        )
        
        # Layout the controls in columns
        region_column = pn.Column(
            "### Region",
            region_selector,
            width=150
        )
        
        # Split dates into two sub-columns for better layout
        date_presets_column = pn.Column(
            "### Quick Select",
            date_presets,
            width=120
        )
        
        date_pickers_column = pn.Column(
            "### Date Range",
            start_date_picker,
            end_date_picker,
            date_display,
            width=180
        )
        
        frequency_column = pn.Column(
            "### Frequency",
            aggregate_selector,
            width=150
        )
        
        smoothing_column = pn.Column(
            "### Smoothing",
            smoothing_selector,
            log_scale_checkbox,
            width=200
        )
        
        # Horizontal layout of all controls
        controls = pn.Column(
            "## Price Analysis Controls",
            pn.Row(
                region_column,
                pn.Spacer(width=20),
                frequency_column,
                pn.Spacer(width=20),
                date_presets_column,
                pn.Spacer(width=20),
                date_pickers_column,
                pn.Spacer(width=20),
                smoothing_column
            ),
            sizing_mode='stretch_width'
        )
        
        # Main content area
        main_content = pn.Column(
            pn.Row(
                pn.Column(
                    "## Price Time Series",
                    price_plot_pane,
                    sizing_mode='stretch_width'
                )
            ),
            pn.Spacer(height=20),
            pn.Row(
                pn.Column(
                    "## Statistics",
                    stats_table_placeholder,
                    width=500
                ),
                pn.Spacer(width=20),
                pn.Column(
                    "## Price Band Contribution",
                    bands_plot_placeholder,
                    width=400
                )
            ),
            sizing_mode='stretch_width'
        )
        
        # Complete tab layout - vertical with controls on top
        prices_tab = pn.Column(
            controls,
            pn.Spacer(height=20),
            main_content,
            sizing_mode='stretch_width'
        )
        
        # Set up callbacks for date presets
        def update_date_range(event):
            """Update date range based on preset selection"""
            preset = event.new
            if preset == '1 day':
                new_start = end_date - pd.Timedelta(days=1)
            elif preset == '7 days':
                new_start = end_date - pd.Timedelta(days=7)
            elif preset == '30 days':
                new_start = end_date - pd.Timedelta(days=30)
            elif preset == '90 days':
                new_start = end_date - pd.Timedelta(days=90)
            elif preset == '1 year':
                new_start = end_date - pd.Timedelta(days=365)
            else:  # All data
                new_start = start_date
            
            start_date_picker.value = new_start
            end_date_picker.value = end_date
        
        # Set up callback for date picker changes
        def update_date_display(event):
            """Update the date display when date pickers change"""
            date_display.object = f"**Selected Period:** {start_date_picker.value.strftime('%Y-%m-%d')} to {end_date_picker.value.strftime('%Y-%m-%d')}"
        
        date_presets.param.watch(update_date_range, 'value')
        start_date_picker.param.watch(update_date_display, 'value')
        end_date_picker.param.watch(update_date_display, 'value')
        
        # Add callbacks for data updates
        region_selector.param.watch(load_and_plot_prices, 'value')
        aggregate_selector.param.watch(load_and_plot_prices, 'value')
        smoothing_selector.param.watch(load_and_plot_prices, 'value')
        log_scale_checkbox.param.watch(load_and_plot_prices, 'value')
        start_date_picker.param.watch(load_and_plot_prices, 'value')
        end_date_picker.param.watch(load_and_plot_prices, 'value')
        
        # Load initial data
        load_and_plot_prices()
        
        logger.info("Prices tab created successfully")
        return prices_tab
        
    except Exception as e:
        logger.error(f"Error creating prices tab: {e}")
        return pn.pane.Markdown(f"**Error loading Prices tab:** {e}")

if __name__ == "__main__":
    # Create the tab
    tab = create_prices_tab()
    
    # Create a simple template to view it
    template = pn.template.MaterialTemplate(
        title="Prices Tab with HvPlot Test",
        main=[tab],
        header_background='#2c3e50'
    )
    
    # Save to HTML file for inspection
    template.save('test_prices_hvplot.html')
    print("Saved to test_prices_hvplot.html")
    
    # Also try to show if possible
    try:
        template.show(port=5558)
    except Exception as e:
        print(f"Could not start server: {e}")