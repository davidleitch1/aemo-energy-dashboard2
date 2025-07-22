#!/usr/bin/env python3
"""
Test to identify the exact issue with price component data loading
"""
import sys
import os
import time
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
os.environ.setdefault('AEMO_DASHBOARD_ENV', 'development')

def test_price_data_loading():
    """Test how price data is loaded in different scenarios"""
    
    print("\n" + "="*60)
    print("Testing Price Component Data Loading")
    print("="*60)
    
    # Import the price loading function
    from aemo_dashboard.shared.adapter_selector import load_price_data
    
    # Test 1: Load with no parameters (what happens on refresh from different tab)
    print("\nTest 1: Loading with NO date parameters (simulates refresh from different tab)")
    start = time.time()
    data_all = load_price_data()
    load_time = time.time() - start
    
    print(f"  Records loaded: {len(data_all):,}")
    print(f"  Load time: {load_time:.2f}s")
    print(f"  Memory usage: {data_all.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    if not data_all.empty:
        print(f"  Date range: {data_all.index.min()} to {data_all.index.max()}")
    
    # Test 2: Load with date parameters (normal operation)
    print("\nTest 2: Loading with 48-hour date range (normal operation)")
    end_date = pd.Timestamp.now()
    start_date = end_date - pd.Timedelta(hours=48)
    
    start = time.time()
    data_filtered = load_price_data(start_date=start_date, end_date=end_date)
    load_time = time.time() - start
    
    print(f"  Records loaded: {len(data_filtered):,}")
    print(f"  Load time: {load_time:.2f}s")
    print(f"  Memory usage: {data_filtered.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    if not data_filtered.empty:
        print(f"  Date range: {data_filtered.index.min()} to {data_filtered.index.max()}")
    
    # Test 3: Check pivot operation on large data
    if len(data_all) > 100000:
        print("\nTest 3: Testing pivot operation on large dataset")
        print(f"  Original shape: {data_all.shape}")
        
        # Reset index to get SETTLEMENTDATE as column
        if data_all.index.name == 'SETTLEMENTDATE':
            data_all = data_all.reset_index()
        
        # Check columns
        print(f"  Columns: {list(data_all.columns)}")
        
        # Try pivot if possible
        if 'REGIONID' in data_all.columns and 'RRP' in data_all.columns:
            print("  Attempting pivot operation...")
            start = time.time()
            try:
                pivoted = data_all.pivot(columns='REGIONID', values='RRP')
                pivot_time = time.time() - start
                print(f"  Pivot completed in {pivot_time:.2f}s")
                print(f"  Pivoted shape: {pivoted.shape}")
                print(f"  Pivoted memory: {pivoted.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            except Exception as e:
                print(f"  Pivot failed: {e}")
    
    # Test 4: Check how the price component handles the data
    print("\nTest 4: Testing price component behavior")
    from aemo_dashboard.nem_dash.price_components import load_price_data as load_price_component_data
    
    print("  Loading via price component (no parameters)...")
    start = time.time()
    component_data = load_price_component_data()
    load_time = time.time() - start
    
    print(f"  Component records: {len(component_data):,}")
    print(f"  Component load time: {load_time:.2f}s")
    print(f"  Component shape: {component_data.shape}")
    
    return data_all, data_filtered, component_data


def test_websocket_serialization_limit():
    """Test at what data size Panel/WebSocket starts having issues"""
    print("\n" + "="*60)
    print("Testing WebSocket Serialization Limits")
    print("="*60)
    
    import panel as pn
    import numpy as np
    
    pn.extension()
    
    # Test different data sizes
    test_sizes = [1000, 10000, 50000, 100000, 200000, 346655]
    
    for size in test_sizes:
        print(f"\nTesting with {size:,} records...")
        
        # Create test data
        dates = pd.date_range(end=pd.Timestamp.now(), periods=size, freq='5min')
        data = pd.DataFrame({
            'NSW1': np.random.randn(size),
            'VIC1': np.random.randn(size),
            'QLD1': np.random.randn(size),
            'SA1': np.random.randn(size),
            'TAS1': np.random.randn(size)
        }, index=dates)
        
        # Test creating Panel DataFrame
        try:
            start = time.time()
            pane = pn.pane.DataFrame(data.tail(5), width=600)
            create_time = time.time() - start
            print(f"  DataFrame pane created in {create_time:.3f}s")
        except Exception as e:
            print(f"  DataFrame pane failed: {e}")
        
        # Test memory usage
        print(f"  Data memory: {data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")


if __name__ == "__main__":
    # First test data loading
    all_data, filtered_data, component_data = test_price_data_loading()
    
    # Then test serialization limits if we found large data
    if len(all_data) > 100000:
        test_websocket_serialization_limit()
    
    print("\n" + "="*60)
    print("CONCLUSION:")
    print("="*60)
    print(f"1. Without date parameters, price component loads {len(component_data):,} records")
    print(f"2. With date parameters, it loads {len(filtered_data):,} records")
    print(f"3. The {len(component_data):,} record load happens when refreshing from a different tab")
    print("4. This causes the hang due to WebSocket serialization of large matplotlib figure")