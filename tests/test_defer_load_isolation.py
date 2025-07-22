#!/usr/bin/env python3
"""
Isolated test to verify defer_load causes WebSocket serialization issues
"""
import panel as pn
import pandas as pd
import numpy as np
import time
import sys

def create_price_data():
    """Create sample price data similar to real dashboard"""
    dates = pd.date_range(end=pd.Timestamp.now(), periods=48*12, freq='5min')
    return pd.DataFrame({
        'NSW1': np.random.randn(len(dates)).cumsum() + 100,
        'VIC1': np.random.randn(len(dates)).cumsum() + 95,
        'QLD1': np.random.randn(len(dates)).cumsum() + 105,
        'SA1': np.random.randn(len(dates)).cumsum() + 90,
        'TAS1': np.random.randn(len(dates)).cumsum() + 85
    }, index=dates)


def test_without_defer_load():
    """Test 1: Normal loading without defer_load"""
    pn.extension('tabulator')
    
    print("\n" + "="*60)
    print("TEST 1: Without defer_load")
    print("="*60)
    
    # Create components directly
    prices = create_price_data()
    
    # Create table
    table = pn.pane.DataFrame(
        prices.tail(5).round(2),
        width=600,
        height=200,
        name="Price Table"
    )
    
    # Create simple chart
    chart = pn.pane.Markdown(f"""
    ### Price Chart Placeholder
    Latest price: NSW1=${prices['NSW1'].iloc[-1]:.2f}
    Data points: {len(prices)}
    Last update: {prices.index[-1]}
    """)
    
    # Create dashboard
    template = pn.template.MaterialTemplate(
        title="Test WITHOUT defer_load",
        header_background='green',
    )
    
    template.main.extend([
        pn.pane.Markdown("""
        ## Test Instructions
        1. Page should load immediately
        2. Refresh browser (Cmd+R in Safari)
        3. Page should reload without hanging
        """),
        table,
        chart
    ])
    
    return template


def test_with_defer_load():
    """Test 2: With defer_load enabled"""
    pn.extension('tabulator', defer_load=True, loading_indicator=True)
    
    print("\n" + "="*60)
    print("TEST 2: With defer_load")
    print("="*60)
    
    # Create deferred components
    def create_deferred_table():
        prices = create_price_data()
        return pn.pane.DataFrame(
            prices.tail(5).round(2),
            width=600,
            height=200,
            name="Deferred Price Table"
        )
    
    def create_deferred_chart():
        prices = create_price_data()
        return pn.pane.Markdown(f"""
        ### Deferred Price Chart
        Latest price: NSW1=${prices['NSW1'].iloc[-1]:.2f}
        Data points: {len(prices)}
        Last update: {prices.index[-1]}
        """)
    
    # Use pn.panel with defer_load
    table = pn.panel(create_deferred_table, defer_load=True)
    chart = pn.panel(create_deferred_chart, defer_load=True)
    
    # Create dashboard
    template = pn.template.MaterialTemplate(
        title="Test WITH defer_load",
        header_background='red',
    )
    
    template.main.extend([
        pn.pane.Markdown("""
        ## Test Instructions
        1. Page should show loading indicators briefly
        2. Refresh browser (Cmd+R in Safari)
        3. **EXPECTED**: Page may hang or fail to reload
        """),
        table,
        chart
    ])
    
    return template


def test_mixed_components():
    """Test 3: Mix of deferred and non-deferred components"""
    pn.extension('tabulator', defer_load=True, loading_indicator=True)
    
    print("\n" + "="*60)
    print("TEST 3: Mixed components (some deferred, some not)")
    print("="*60)
    
    # Non-deferred component
    static_info = pn.pane.Markdown("""
    ### Static Information (NOT deferred)
    This component loads immediately.
    """)
    
    # Deferred component
    def create_deferred_data():
        time.sleep(0.5)  # Simulate slow loading
        prices = create_price_data()
        return pn.pane.DataFrame(
            prices.describe().round(2),
            width=600,
            height=300,
            name="Price Statistics"
        )
    
    deferred_data = pn.panel(create_deferred_data, defer_load=True)
    
    # Create dashboard
    template = pn.template.MaterialTemplate(
        title="Test MIXED Components",
        header_background='orange',
    )
    
    template.main.extend([
        pn.pane.Markdown("""
        ## Test Instructions
        1. Static content loads immediately
        2. Deferred content shows loading indicator
        3. Refresh browser - note which components cause issues
        """),
        static_info,
        pn.layout.Divider(),
        deferred_data
    ])
    
    return template


def run_test(test_number=1):
    """Run a specific test"""
    tests = {
        1: test_without_defer_load,
        2: test_with_defer_load,
        3: test_mixed_components
    }
    
    if test_number not in tests:
        print(f"Invalid test number: {test_number}")
        print("Available tests: 1, 2, 3")
        return
    
    dashboard = tests[test_number]()
    
    print(f"\nStarting test {test_number}...")
    print("Navigate to: http://localhost:5010")
    print("\nIMPORTANT: Test in Safari browser")
    print("1. Let page fully load")
    print("2. Refresh with Cmd+R")
    print("3. Note if page hangs or reloads properly")
    print("\nPress Ctrl+C to stop")
    
    dashboard.show(port=5010)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            test_num = int(sys.argv[1])
            run_test(test_num)
        except ValueError:
            print("Please provide a test number (1, 2, or 3)")
    else:
        print("\nDefer Load Isolation Tests")
        print("="*60)
        print("\nUsage: python test_defer_load_isolation.py <test_number>")
        print("\nAvailable tests:")
        print("  1: Without defer_load (should work fine)")
        print("  2: With defer_load (should hang on refresh)")
        print("  3: Mixed components (partial hang expected)")
        print("\nRun each test and verify Safari refresh behavior")