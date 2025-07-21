#!/usr/bin/env python3
"""
Profile price component loading to find actual bottlenecks
"""
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
os.environ.setdefault('AEMO_DASHBOARD_ENV', 'development')

def profile_price_loading():
    """Profile each step of price loading"""
    
    print("Profiling price component loading...")
    print("=" * 60)
    
    # Import phase
    import_start = time.time()
    from aemo_dashboard.nem_dash.price_components import (
        load_price_data, create_price_table, create_price_chart, create_price_section
    )
    import_time = time.time() - import_start
    print(f"Import time: {import_time:.3f}s")
    
    # Load price data
    load_start = time.time()
    prices = load_price_data()
    load_time = time.time() - load_start
    print(f"Load price data: {load_time:.3f}s")
    print(f"  - Records: {len(prices) if not prices.empty else 0}")
    print(f"  - Regions: {len(prices.columns) if not prices.empty else 0}")
    
    # Create table
    table_start = time.time()
    table = create_price_table(prices)
    table_time = time.time() - table_start
    print(f"Create price table: {table_time:.3f}s")
    
    # Create chart
    chart_start = time.time()
    chart = create_price_chart(prices)
    chart_time = time.time() - chart_start
    print(f"Create price chart: {chart_time:.3f}s")
    
    # Create full section
    section_start = time.time()
    section = create_price_section()
    section_time = time.time() - section_start
    print(f"Create price section: {section_time:.3f}s")
    
    # Total time
    total = load_time + table_time + chart_time + section_time
    print(f"\nTotal time: {total:.3f}s")
    
    # Breakdown
    print("\nBreakdown:")
    print(f"  - Data loading: {load_time:.3f}s ({load_time/total*100:.1f}%)")
    print(f"  - Table creation: {table_time:.3f}s ({table_time/total*100:.1f}%)")
    print(f"  - Chart creation: {chart_time:.3f}s ({chart_time/total*100:.1f}%)")
    print(f"  - Section wrapper: {section_time:.3f}s ({section_time/total*100:.1f}%)")
    
    # Find the bottleneck
    bottleneck = max(
        ('Data loading', load_time),
        ('Table creation', table_time),
        ('Chart creation', chart_time),
        key=lambda x: x[1]
    )
    print(f"\nBottleneck: {bottleneck[0]} at {bottleneck[1]:.3f}s")
    
    return prices


if __name__ == "__main__":
    prices = profile_price_loading()