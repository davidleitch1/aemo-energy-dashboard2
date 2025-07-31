#!/usr/bin/env python3
"""Test the new Prices tab layout"""

import sys
import panel as pn
import pandas as pd
from datetime import datetime, timedelta

# Initialize Panel
pn.extension()

# Mock the logger
class MockLogger:
    def info(self, msg): print(f"INFO: {msg}")
    def error(self, msg): print(f"ERROR: {msg}")

logger = MockLogger()

# Test date range - 5 years of data
end_date = datetime.now().date()
start_date = end_date - timedelta(days=365*5)  # 5 years of data

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
        
        # Create placeholder for plots
        price_plot_placeholder = pn.pane.Markdown(
            "Price plot will appear here after loading data...",
            height=400,
            sizing_mode='stretch_width'
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
                    price_plot_placeholder,
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
        title="Prices Tab Layout Test",
        main=[tab],
        header_background='#2c3e50'
    )
    
    # Save to HTML file for inspection
    template.save('test_prices_tab_layout.html')
    print("Saved to test_prices_tab_layout.html")
    
    # Also try to show if possible
    try:
        template.show(port=5557)
    except Exception as e:
        print(f"Could not start server: {e}")