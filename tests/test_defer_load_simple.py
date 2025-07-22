#!/usr/bin/env python3
"""
Simple test to measure defer_load performance improvement
"""
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_dashboard_creation():
    """Test dashboard creation time with defer_load"""
    
    # Set environment
    os.environ.setdefault('AEMO_DASHBOARD_ENV', 'development')
    
    print("Testing dashboard creation with defer_load...")
    print("=" * 60)
    
    # Time the imports
    import_start = time.time()
    from aemo_dashboard.generation.gen_dash import EnergyDashboard, create_app
    import_time = time.time() - import_start
    print(f"Import time: {import_time:.3f}s")
    
    # Time dashboard creation
    creation_start = time.time()
    dashboard = EnergyDashboard()
    creation_time = time.time() - creation_start
    print(f"Dashboard instance creation: {creation_time:.3f}s")
    
    # Time UI creation
    ui_start = time.time()
    dashboard_ui = dashboard.create_dashboard()
    ui_time = time.time() - ui_start
    print(f"Dashboard UI creation: {ui_time:.3f}s")
    
    # Check if components are deferred
    print("\nChecking component status:")
    print(f"- Has tabs: {hasattr(dashboard_ui, 'tabs') or 'Tabs' in str(type(dashboard_ui))}")
    
    # Try to check if loading indicators are present
    if hasattr(dashboard_ui, '_repr_mimebundle_'):
        try:
            bundle = dashboard_ui._repr_mimebundle_()
            if 'text/html' in bundle:
                html = bundle['text/html']
                has_loading = 'LoadingSpinner' in html or 'loading' in html.lower()
                print(f"- Has loading indicators: {has_loading}")
        except:
            pass
    
    total_time = import_time + creation_time + ui_time
    print(f"\nTotal time: {total_time:.3f}s")
    
    # Compare with baseline
    baseline_dashboard_time = 5.668  # From baseline profiling
    improvement = baseline_dashboard_time - (creation_time + ui_time)
    improvement_pct = (improvement / baseline_dashboard_time) * 100
    
    print(f"\nComparison with baseline:")
    print(f"- Baseline dashboard creation: {baseline_dashboard_time:.3f}s")
    print(f"- Current dashboard creation: {creation_time + ui_time:.3f}s")
    print(f"- Improvement: {improvement:.3f}s ({improvement_pct:.1f}%)")
    
    if improvement > 0:
        print("\n✅ Performance improved with defer_load!")
    else:
        print("\n⚠️  No performance improvement detected")
    
    return dashboard_ui


if __name__ == "__main__":
    try:
        dashboard_ui = test_dashboard_creation()
        print("\n✅ Dashboard created successfully")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()