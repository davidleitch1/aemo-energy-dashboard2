#!/usr/bin/env python3
"""
Test HoloViews performance vs matplotlib
"""
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
os.environ.setdefault('AEMO_DASHBOARD_ENV', 'development')

def test_hvplot_performance():
    """Compare matplotlib vs hvplot performance"""
    
    print("Comparing matplotlib vs HoloViews performance...")
    print("=" * 60)
    
    # Test original matplotlib version
    print("\n1. Testing MATPLOTLIB version:")
    from aemo_dashboard.nem_dash.price_components import (
        load_price_data as load_mpl,
        create_price_chart as create_chart_mpl,
        create_price_section as create_section_mpl
    )
    
    # Load data once
    prices = load_mpl()
    
    # Time matplotlib chart
    mpl_start = time.time()
    chart_mpl = create_chart_mpl(prices)
    mpl_time = time.time() - mpl_start
    print(f"  Matplotlib chart creation: {mpl_time:.3f}s")
    
    # Time full section
    section_mpl_start = time.time()
    section_mpl = create_section_mpl()
    section_mpl_time = time.time() - section_mpl_start
    print(f"  Matplotlib full section: {section_mpl_time:.3f}s")
    
    # Test HoloViews version
    print("\n2. Testing HOLOVIEWS version:")
    from aemo_dashboard.nem_dash.price_components_hvplot import (
        create_price_chart as create_chart_hv,
        create_price_section as create_section_hv
    )
    
    # Time HoloViews chart
    hv_start = time.time()
    chart_hv = create_chart_hv(prices)
    hv_time = time.time() - hv_start
    print(f"  HoloViews chart creation: {hv_time:.3f}s")
    
    # Time full section
    section_hv_start = time.time()
    section_hv = create_section_hv()
    section_hv_time = time.time() - section_hv_start
    print(f"  HoloViews full section: {section_hv_time:.3f}s")
    
    # Results
    print("\n3. RESULTS:")
    print(f"  Chart creation speedup: {mpl_time/hv_time:.1f}x faster")
    print(f"  Full section speedup: {section_mpl_time/section_hv_time:.1f}x faster")
    print(f"  Time saved: {mpl_time - hv_time:.3f}s on chart, {section_mpl_time - section_hv_time:.3f}s on section")
    
    return prices


if __name__ == "__main__":
    prices = test_hvplot_performance()