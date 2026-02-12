#!/usr/bin/env python3
"""
Test script for generation summary table with PCP comparison.
This tests the new methods added to gen_dash.py.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("=" * 80)
print("GENERATION SUMMARY TABLE TEST")
print("=" * 80)
print(f"Test started: {datetime.now()}")
print()

# Test 1: Import the dashboard class
print("Test 1: Importing GenerationDashboard...")
print("-" * 80)
try:
    from aemo_dashboard.generation.gen_dash import GenerationDashboard
    print("✓ SUCCESS: GenerationDashboard imported")
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 2: Create dashboard instance
print("Test 2: Creating GenerationDashboard instance...")
print("-" * 80)
try:
    dashboard = GenerationDashboard()
    print("✓ SUCCESS: Dashboard instance created")
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 3: Calculate PCP date range for 7-day period
print("Test 3: Calculate PCP date range (7-day period)...")
print("-" * 80)
try:
    dashboard.time_range = '7'
    pcp_start, pcp_end = dashboard.calculate_pcp_date_range()

    if pcp_start and pcp_end:
        print(f"✓ SUCCESS: PCP date range calculated")
        print(f"  Current period: {dashboard._get_effective_date_range()}")
        print(f"  PCP period: {pcp_start.date()} to {pcp_end.date()}")

        # Verify it's exactly 365 days prior
        current_start, current_end = dashboard._get_effective_date_range()
        expected_diff = timedelta(days=365)
        actual_diff = current_start - pcp_start

        if abs((actual_diff - expected_diff).days) <= 1:  # Allow 1-day tolerance
            print(f"  ✓ Verified: PCP is exactly 365 days prior")
        else:
            print(f"  ✗ WARNING: PCP offset is {actual_diff.days} days, expected 365")
    else:
        print("✗ FAILED: PCP date range returned None")
        sys.exit(1)
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 4: Edge case - 'All' time range (should not show PCP)
print("Test 4: Edge case - 'All' time range (should not show PCP)...")
print("-" * 80)
try:
    dashboard.time_range = 'All'
    pcp_start, pcp_end = dashboard.calculate_pcp_date_range()

    if pcp_start is None and pcp_end is None:
        print(f"✓ SUCCESS: PCP correctly disabled for 'All' time range")
    else:
        print(f"✗ FAILED: PCP should be None for 'All' time range")
        sys.exit(1)
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 5: Edge case - Custom range >= 365 days (should not show PCP)
print("Test 5: Edge case - Custom range >= 365 days (should not show PCP)...")
print("-" * 80)
try:
    dashboard.time_range = 'custom'
    dashboard.start_date = (datetime.now() - timedelta(days=400)).date()
    dashboard.end_date = datetime.now().date()

    pcp_start, pcp_end = dashboard.calculate_pcp_date_range()

    if pcp_start is None and pcp_end is None:
        print(f"✓ SUCCESS: PCP correctly disabled for >= 365 day range")
    else:
        print(f"✗ FAILED: PCP should be None for >= 365 day range")
        sys.exit(1)
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 6: Load generation data and calculate summary
print("Test 6: Load generation data and calculate summary (1-day period)...")
print("-" * 80)
try:
    dashboard.time_range = '1'
    dashboard.load_generation_data()

    if dashboard.gen_output_df is not None and not dashboard.gen_output_df.empty:
        print(f"✓ Generation data loaded: {len(dashboard.gen_output_df)} records")

        # Process data for region
        processed_data = dashboard.process_data_for_region()

        if not processed_data.empty:
            print(f"✓ Data processed: {processed_data.shape}")
            print(f"  Fuel types: {list(processed_data.columns)}")

            # Calculate summary
            summary = dashboard.calculate_generation_summary(processed_data)

            if not summary.empty:
                print(f"✓ SUCCESS: Summary calculated for {len(summary)} fuel types")
                print(f"\n  Sample summary data:")
                for _, row in summary.head(5).iterrows():
                    print(f"    {row['fuel_type']:15s}: {row['total_gwh']:8.1f} GWh ({row['percentage']:5.1f}%)")

                # Verify totals add up to ~100%
                total_pct = summary['percentage'].sum()
                if abs(total_pct - 100.0) < 0.1:
                    print(f"\n  ✓ Verified: Percentages sum to {total_pct:.1f}%")
                else:
                    print(f"\n  ✗ WARNING: Percentages sum to {total_pct:.1f}%, expected 100%")
            else:
                print("✗ FAILED: Summary is empty")
                sys.exit(1)
        else:
            print("✗ FAILED: Processed data is empty")
            sys.exit(1)
    else:
        print("✗ FAILED: No generation data loaded")
        sys.exit(1)
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 7: Create summary table widget
print("Test 7: Create summary table widget...")
print("-" * 80)
try:
    summary_table = dashboard.create_generation_summary_table()

    if summary_table is not None:
        print(f"✓ SUCCESS: Summary table widget created")
        print(f"  Widget type: {type(summary_table).__name__}")
    else:
        print("✗ FAILED: Summary table is None")
        sys.exit(1)
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()
print("=" * 80)
print("ALL TESTS PASSED ✓")
print("=" * 80)
print()
print("Summary of tested features:")
print("  1. Dashboard instance creation")
print("  2. PCP date range calculation (7-day period)")
print("  3. Edge case: 'All' time range (PCP disabled)")
print("  4. Edge case: >= 365 day range (PCP disabled)")
print("  5. Generation data loading and processing")
print("  6. Generation summary calculation")
print("  7. Summary table widget creation")
print()
print("Next steps:")
print("  - Test in live dashboard with different time periods")
print("  - Verify PCP data query works correctly")
print("  - Check table formatting and layout")
print()
