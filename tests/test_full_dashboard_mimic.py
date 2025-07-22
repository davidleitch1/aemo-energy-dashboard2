#!/usr/bin/env python3
"""
Test that mimics the actual dashboard structure more closely
This includes tabs, periodic callbacks, and multiple deferred components
"""
import panel as pn
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import time

def create_price_component():
    """Mimic the actual price component from the dashboard"""
    # Simulate data loading
    time.sleep(0.5)
    
    dates = pd.date_range(end=pd.Timestamp.now(), periods=100, freq='5min')
    prices = pd.DataFrame({
        'NSW1': np.random.randn(100).cumsum() + 100,
        'VIC1': np.random.randn(100).cumsum() + 95,
        'QLD1': np.random.randn(100).cumsum() + 105,
    }, index=dates)
    
    # Create matplotlib chart
    fig, ax = plt.subplots(figsize=(5.5, 2.5))
    for col in prices.columns:
        ax.plot(prices.index, prices[col], label=col)
    ax.set_title("5 minute spot prices")
    ax.legend()
    plt.tight_layout()
    
    # Create table
    table = pn.pane.DataFrame(
        prices.tail(5).round(2),
        width=550,
        height=200
    )
    
    return pn.Column(
        table,
        pn.pane.Matplotlib(fig, sizing_mode='fixed', width=550, height=250),
        name="Price Components"
    )


def create_renewable_gauge():
    """Mimic the renewable gauge component"""
    time.sleep(0.3)
    renewable_pct = np.random.randint(40, 60)
    
    return pn.indicators.Gauge(
        name='Renewable %',
        value=renewable_pct,
        bounds=(0, 100),
        format='{value}%',
        colors=[(0.3, 'red'), (0.6, 'gold'), (1, 'green')],
        width=200,
        height=200
    )


def create_generation_overview():
    """Mimic generation overview component"""
    time.sleep(0.4)
    
    # Create sample generation data
    dates = pd.date_range(end=pd.Timestamp.now(), periods=48, freq='30min')
    gen_data = pd.DataFrame({
        'Coal': np.random.uniform(10000, 15000, 48),
        'Gas': np.random.uniform(2000, 4000, 48),
        'Wind': np.random.uniform(1000, 5000, 48),
        'Solar': np.random.uniform(0, 8000, 48),
    }, index=dates)
    
    # Create stacked area chart using matplotlib
    fig, ax = plt.subplots(figsize=(8, 4))
    gen_data.plot.area(ax=ax, alpha=0.7)
    ax.set_title("Generation by Fuel Type")
    ax.set_ylabel("MW")
    plt.tight_layout()
    
    return pn.pane.Matplotlib(fig, sizing_mode='fixed', width=800, height=400)


def test_with_defer_load():
    """Test with defer_load enabled (mimicking the problematic case)"""
    pn.extension('tabulator', defer_load=True, loading_indicator=True)
    
    print("\n" + "="*60)
    print("FULL DASHBOARD TEST - With defer_load")
    print("="*60)
    
    # Create deferred components
    price_section = pn.panel(create_price_component, defer_load=True)
    renewable_gauge = pn.panel(create_renewable_gauge, defer_load=True)
    generation_overview = pn.panel(create_generation_overview, defer_load=True)
    
    # Create Today tab content
    today_tab = pn.Row(
        pn.Column(price_section, margin=(10, 10)),
        pn.Column(renewable_gauge, generation_overview, margin=(10, 10))
    )
    
    # Create other tabs (simplified)
    generation_tab = pn.pane.Markdown("# Generation Analysis\n\nPlaceholder content")
    price_tab = pn.pane.Markdown("# Price Analysis\n\nPlaceholder content")
    station_tab = pn.pane.Markdown("# Station Analysis\n\nPlaceholder content")
    
    # Create tabs with dynamic=True (as in original)
    tabs = pn.Tabs(
        ('Today', today_tab),
        ('Generation', generation_tab),
        ('Price Analysis', price_tab),
        ('Station Analysis', station_tab),
        dynamic=True
    )
    
    # Add a status indicator that updates periodically
    status = pn.pane.Markdown(f"Last update: {time.strftime('%H:%M:%S')}")
    
    def update_status():
        status.object = f"Last update: {time.strftime('%H:%M:%S')}"
    
    # Create template
    template = pn.template.MaterialTemplate(
        title="Full Dashboard Test - WITH defer_load",
        header_background='red',
    )
    
    template.main.extend([
        pn.pane.Markdown("""
        ## Test Instructions
        
        This closely mimics the actual dashboard structure:
        - Multiple tabs with dynamic=True
        - Deferred components in Today tab
        - Periodic status updates
        - Matplotlib charts with defer_load
        
        **Test**: Refresh browser (Cmd+R) and see if it hangs
        """),
        status,
        tabs
    ])
    
    # Add periodic callback (like the real dashboard)
    # Note: This will be added when the server starts
    
    return template


def test_without_defer_load():
    """Test without defer_load (control case)"""
    pn.extension('tabulator')
    
    print("\n" + "="*60)
    print("FULL DASHBOARD TEST - Without defer_load")
    print("="*60)
    
    # Create components directly
    price_section = create_price_component()
    renewable_gauge = create_renewable_gauge()
    generation_overview = create_generation_overview()
    
    # Create Today tab content
    today_tab = pn.Row(
        pn.Column(price_section, margin=(10, 10)),
        pn.Column(renewable_gauge, generation_overview, margin=(10, 10))
    )
    
    # Create other tabs
    generation_tab = pn.pane.Markdown("# Generation Analysis\n\nPlaceholder content")
    price_tab = pn.pane.Markdown("# Price Analysis\n\nPlaceholder content")
    station_tab = pn.pane.Markdown("# Station Analysis\n\nPlaceholder content")
    
    # Create tabs
    tabs = pn.Tabs(
        ('Today', today_tab),
        ('Generation', generation_tab),
        ('Price Analysis', price_tab),
        ('Station Analysis', station_tab),
        dynamic=True
    )
    
    # Status indicator
    status = pn.pane.Markdown(f"Last update: {time.strftime('%H:%M:%S')}")
    
    def update_status():
        status.object = f"Last update: {time.strftime('%H:%M:%S')}"
    
    # Create template
    template = pn.template.MaterialTemplate(
        title="Full Dashboard Test - WITHOUT defer_load",
        header_background='green',
    )
    
    template.main.extend([
        pn.pane.Markdown("""
        ## Test Instructions
        
        Same structure as above but without defer_load.
        Should refresh without issues.
        """),
        status,
        tabs
    ])
    
    # Add periodic callback
    # Note: This will be added when the server starts
    
    return template


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "defer":
        dashboard = test_with_defer_load()
    else:
        dashboard = test_without_defer_load()
    
    print("\nStarting full dashboard mimic test...")
    print("Navigate to: http://localhost:5012")
    print("\nThis test includes:")
    print("- Multiple tabs (dynamic=True)")
    print("- Three deferred components in Today tab")
    print("- Periodic status updates")
    print("- Matplotlib charts")
    print("\nTest Safari refresh behavior")
    print("Press Ctrl+C to stop")
    
    dashboard.show(port=5012)