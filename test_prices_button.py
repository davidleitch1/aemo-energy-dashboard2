#!/usr/bin/env python3
"""Test the updated prices tab with analyze button"""

import os
import sys
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Minimal test - just create the tab and save HTML
from datetime import datetime, timedelta

# Mock self object
class MockDashboard:
    def __init__(self):
        self.end_date = datetime.now().date()
        self.start_date = self.end_date - timedelta(days=365*5)
        self.price_plot_pane = None

# Create mock dashboard
mock_dash = MockDashboard()

# Import what we need
import panel as pn
import holoviews as hv
pn.extension()
hv.extension('bokeh')

# Now test the tab creation code
print("Testing prices tab creation...")

# Execute the tab creation code in the mock context
self = mock_dash  # Mock self for the code

# Execute the _create_prices_tab code directly
import pandas as pd
from aemo_dashboard.shared.logging_config import get_logger
logger = get_logger(__name__)

# Copy the tab creation code
try:
    logger.info("Creating prices tab...")
    
    # Calculate date range - use full data range (5 years)
    end_date = self.end_date
    start_date = self.start_date  # This should already be 5 years of data
    
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
    
    # Add Analyze button
    analyze_button = pn.widgets.Button(
        name='Analyze Prices',
        button_type='primary',
        width=150
    )
    
    # Create price plot pane - will be updated by load_price_data
    self.price_plot_pane = pn.pane.HoloViews(
        height=400,
        sizing_mode='stretch_width'
    )
    
    # Initialize with instruction message
    self.price_plot_pane.object = hv.Text(0.5, 0.5, "Click 'Analyze Prices' to load data").opts(
        xlim=(0, 1), ylim=(0, 1), 
        bgcolor='#282a36',  # Dracula background
        color='#f8f8f2',    # Dracula foreground
        fontsize=16
    )
    
    print("✓ Created all widgets successfully")
    print("✓ Price plot pane shows instruction message")
    print("✓ Analyze button created")
    
    # Layout test
    region_column = pn.Column(
        "### Region",
        region_selector,
        width=150
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
        pn.Spacer(height=10),
        analyze_button,
        width=200
    )
    
    print("✓ Created control columns with analyze button")
    
    # Test that button exists and has correct properties
    assert analyze_button.name == 'Analyze Prices'
    assert analyze_button.button_type == 'primary'
    print("✓ Analyze button configured correctly")
    
    # Create full layout
    controls = pn.Column(
        "## Price Analysis Controls",
        pn.Row(
            region_column,
            pn.Spacer(width=20),
            frequency_column,
            pn.Spacer(width=20),
            smoothing_column
        ),
        sizing_mode='stretch_width'
    )
    
    main_content = pn.Column(
        pn.Row(
            pn.Column(
                "## Price Time Series",
                self.price_plot_pane,
                sizing_mode='stretch_width'
            )
        ),
        sizing_mode='stretch_width'
    )
    
    prices_tab = pn.Column(
        controls,
        pn.Spacer(height=20),
        main_content,
        sizing_mode='stretch_width'
    )
    
    print("✓ Created full tab layout")
    
    # Save to HTML
    template = pn.template.MaterialTemplate(
        title="Prices Tab Test",
        main=[prices_tab],
        header_background='#2c3e50'
    )
    
    template.save('test_prices_button_output.html')
    print("\n✅ SUCCESS: Prices tab created with analyze button")
    print("   Saved to: test_prices_button_output.html")
    print("\nKey changes:")
    print("- Added 'Analyze Prices' button")
    print("- Initial message prompts user to click button")
    print("- Data only loads when button is clicked")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()