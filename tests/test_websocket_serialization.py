#!/usr/bin/env python3
"""
Test WebSocket serialization with different Panel configurations
This will help us verify the exact cause of the Safari refresh issue
"""
import panel as pn
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# Test configurations
TEST_CONFIGS = {
    "baseline": {
        "name": "Baseline (no defer_load)",
        "defer_load": False,
        "use_pn_panel": False,
        "use_periodic_callback": False
    },
    "defer_load_simple": {
        "name": "Simple defer_load",
        "defer_load": True,
        "use_pn_panel": True,
        "use_periodic_callback": False
    },
    "defer_load_with_callback": {
        "name": "defer_load with periodic callback",
        "defer_load": True,
        "use_pn_panel": True,
        "use_periodic_callback": True
    },
    "matplotlib_no_defer": {
        "name": "Matplotlib without defer_load",
        "defer_load": False,
        "use_pn_panel": False,
        "use_periodic_callback": False,
        "use_matplotlib": True
    },
    "matplotlib_with_defer": {
        "name": "Matplotlib with defer_load",
        "defer_load": True,
        "use_pn_panel": True,
        "use_periodic_callback": False,
        "use_matplotlib": True
    }
}


def create_simple_component():
    """Create a simple component for testing"""
    return pn.pane.Markdown(f"# Simple Component\nTime: {time.time()}")


def create_matplotlib_component():
    """Create a matplotlib chart similar to price_components"""
    # Generate sample data
    dates = pd.date_range(end=pd.Timestamp.now(), periods=100, freq='5min')
    data = pd.DataFrame({
        'NSW1': np.random.randn(100).cumsum() + 100,
        'VIC1': np.random.randn(100).cumsum() + 95,
        'QLD1': np.random.randn(100).cumsum() + 105
    }, index=dates)
    
    # Create matplotlib figure
    fig, ax = plt.subplots(figsize=(6, 3))
    for col in data.columns:
        ax.plot(data.index, data[col], label=col)
    
    ax.set_title("Test Price Chart")
    ax.set_ylabel("Price ($/MWh)")
    ax.legend()
    plt.tight_layout()
    
    return pn.pane.Matplotlib(fig, sizing_mode='fixed', width=600, height=300)


def create_test_dashboard(config_name):
    """Create a test dashboard with the specified configuration"""
    config = TEST_CONFIGS[config_name]
    
    # Set up Panel extension
    if config["defer_load"]:
        pn.extension('tabulator', defer_load=True, loading_indicator=True)
    else:
        pn.extension('tabulator')
    
    print(f"\n{'='*60}")
    print(f"Testing: {config['name']}")
    print(f"Configuration: {config}")
    print(f"{'='*60}")
    
    # Create components based on configuration
    components = []
    
    # Add a simple component
    if config.get("use_matplotlib"):
        if config["use_pn_panel"] and config["defer_load"]:
            # Wrap matplotlib in defer_load
            component = pn.panel(create_matplotlib_component, defer_load=True)
        else:
            component = create_matplotlib_component()
    else:
        if config["use_pn_panel"] and config["defer_load"]:
            # Use defer_load
            component = pn.panel(create_simple_component, defer_load=True)
        else:
            component = create_simple_component()
    
    components.append(component)
    
    # Add a component with periodic callback if specified
    if config["use_periodic_callback"]:
        time_display = pn.pane.Markdown("Waiting for update...")
        
        def update_time():
            time_display.object = f"Periodic Update: {time.strftime('%H:%M:%S')}"
        
        # Only add callback if we're in a server context
        if hasattr(pn.state, 'add_periodic_callback'):
            pn.state.add_periodic_callback(update_time, period=1000)
        
        components.append(time_display)
    
    # Create the dashboard
    template = pn.template.MaterialTemplate(
        title=f"WebSocket Test - {config['name']}",
        sidebar=[
            pn.pane.Markdown("""
            ## Test Instructions
            
            1. Open dashboard in Safari
            2. Wait for it to fully load
            3. Try to refresh the page
            4. Note if it hangs or loads properly
            
            ## What to Look For
            
            - **Hangs on refresh**: WebSocket serialization issue
            - **Loads normally**: No serialization issue
            - **Error messages**: Check console/logs
            """)
        ]
    )
    
    template.main.extend([
        pn.pane.Markdown(f"### Configuration: {config_name}"),
        pn.pane.JSON(config, depth=2),
        pn.layout.Divider(),
        *components
    ])
    
    return template


def run_serialization_test(config_name="baseline"):
    """Run a specific serialization test"""
    dashboard = create_test_dashboard(config_name)
    
    print("\nStarting test server...")
    print("Navigate to: http://localhost:5009")
    print("\nTest refresh behavior in Safari:")
    print("1. Load the page")
    print("2. Refresh (Cmd+R)")
    print("3. Note if page hangs or loads properly")
    print("\nPress Ctrl+C to stop and test next configuration")
    
    dashboard.show(port=5009)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        config = sys.argv[1]
        if config in TEST_CONFIGS:
            run_serialization_test(config)
        else:
            print(f"Unknown config: {config}")
            print(f"Available configs: {list(TEST_CONFIGS.keys())}")
    else:
        print("WebSocket Serialization Test Suite")
        print("="*60)
        print("\nUsage: python test_websocket_serialization.py <config>")
        print("\nAvailable configurations:")
        for name, config in TEST_CONFIGS.items():
            print(f"  {name}: {config['name']}")
        print("\nExample: python test_websocket_serialization.py baseline")
        print("\nRun each configuration and test Safari refresh behavior")