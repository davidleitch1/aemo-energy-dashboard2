#!/usr/bin/env python3
"""
Test to verify that loading 346,655 price records causes Panel/WebSocket hang
"""
import panel as pn
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_small_data():
    """Test with small dataset (should work fine)"""
    print("\n" + "="*60)
    print("TEST 1: Small dataset (48 hours)")
    print("="*60)
    
    # Create small dataset similar to normal operation
    dates = pd.date_range(end=pd.Timestamp.now(), periods=48*12, freq='5min')
    prices = pd.DataFrame({
        'NSW1': np.random.randn(len(dates)).cumsum() + 100,
        'VIC1': np.random.randn(len(dates)).cumsum() + 95,
        'QLD1': np.random.randn(len(dates)).cumsum() + 105,
        'SA1': np.random.randn(len(dates)).cumsum() + 90,
        'TAS1': np.random.randn(len(dates)).cumsum() + 85
    }, index=dates)
    
    print(f"Created {len(prices):,} price records")
    return create_price_dashboard(prices, "Small Dataset (48 hours)")


def test_large_data():
    """Test with 346,655 records (should cause hang)"""
    print("\n" + "="*60)
    print("TEST 2: Large dataset (346,655 records - 5+ years)")
    print("="*60)
    
    # Create dataset matching the problematic size
    # 346,655 records at 5-minute intervals = ~3.3 years
    dates = pd.date_range(end=pd.Timestamp.now(), periods=346655, freq='5min')
    
    print("Creating large dataset...")
    start_time = time.time()
    
    # Create data in chunks to avoid memory issues
    chunk_size = 50000
    chunks = []
    
    for i in range(0, len(dates), chunk_size):
        chunk_dates = dates[i:i+chunk_size]
        chunk_data = pd.DataFrame({
            'NSW1': np.random.randn(len(chunk_dates)).cumsum() + 100,
            'VIC1': np.random.randn(len(chunk_dates)).cumsum() + 95,
            'QLD1': np.random.randn(len(chunk_dates)).cumsum() + 105,
            'SA1': np.random.randn(len(chunk_dates)).cumsum() + 90,
            'TAS1': np.random.randn(len(chunk_dates)).cumsum() + 85
        }, index=chunk_dates)
        chunks.append(chunk_data)
    
    prices = pd.concat(chunks)
    print(f"Created {len(prices):,} price records in {time.time() - start_time:.2f}s")
    
    return create_price_dashboard(prices, "Large Dataset (346,655 records)")


def create_price_dashboard(prices, title):
    """Create dashboard with price table and chart"""
    pn.extension('tabulator')
    
    print(f"\nCreating dashboard components for {len(prices):,} records...")
    
    # Create price table (last 5 rows)
    table_start = time.time()
    table = pn.pane.DataFrame(
        prices.tail(5).round(2),
        width=600,
        height=200,
        name="Price Table"
    )
    print(f"Table created in {time.time() - table_start:.2f}s")
    
    # Create matplotlib chart (last 120 points with EWM smoothing)
    chart_start = time.time()
    
    # Use same logic as price_components.py
    rows_to_take = min(120, len(prices))
    df = prices.tail(rows_to_take).ewm(alpha=0.22, adjust=False).mean()
    
    # Create matplotlib figure
    fig, ax = plt.subplots()
    fig.set_size_inches(5.5, 2.5)
    
    for col in df.columns:
        ax.plot(df.index, df[col], label=col)
    
    ax.set_title(f"Price Chart - {rows_to_take} points")
    ax.set_ylabel("$/MWh")
    ax.legend(fontsize=7)
    plt.tight_layout()
    
    chart = pn.pane.Matplotlib(fig, sizing_mode='fixed', width=550, height=250)
    print(f"Chart created in {time.time() - chart_start:.2f}s")
    
    # Create info pane
    info = pn.pane.Markdown(f"""
    ### Dataset Info
    - **Total records**: {len(prices):,}
    - **Date range**: {prices.index[0]} to {prices.index[-1]}
    - **Regions**: {list(prices.columns)}
    - **Memory usage**: {prices.memory_usage(deep=True).sum() / 1024**2:.1f} MB
    
    ### Test Instructions
    1. Page should load
    2. Try to refresh browser (Cmd+R)
    3. With large dataset, refresh may hang
    """)
    
    # Create dashboard
    template = pn.template.MaterialTemplate(
        title=f"Price Data Test - {title}",
        header_background='orange' if len(prices) > 10000 else 'green',
    )
    
    template.main.extend([
        info,
        pn.layout.Divider(),
        pn.pane.Markdown("### Price Table (Last 5 Records)"),
        table,
        pn.layout.Divider(),
        pn.pane.Markdown("### Price Chart (Smoothed)"),
        chart
    ])
    
    return template


def run_test(test_type="small"):
    """Run the specified test"""
    if test_type == "small":
        dashboard = test_small_data()
    elif test_type == "large":
        dashboard = test_large_data()
    else:
        print(f"Unknown test type: {test_type}")
        return
    
    print("\nStarting Panel server...")
    print("Navigate to: http://localhost:5014")
    print("\nIMPORTANT:")
    print("1. Load the page")
    print("2. Try to refresh (Cmd+R)")
    print("3. Note if it hangs with large dataset")
    print("\nPress Ctrl+C to stop")
    
    dashboard.show(port=5014)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
    else:
        print("\nPrice Data Hang Test")
        print("="*60)
        print("\nUsage:")
        print("  python test_large_price_data_hang.py small   # Test with 576 records (should work)")
        print("  python test_large_price_data_hang.py large   # Test with 346,655 records (may hang)")
        print("\nThe large test reproduces the issue seen in the dashboard")
        sys.exit(0)
    
    run_test(test_type)