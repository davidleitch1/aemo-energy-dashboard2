#!/usr/bin/env python3
"""
Test script to verify rooftop solar PCP data is loading correctly.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("=" * 80)
print("ROOFTOP SOLAR PCP FIX VERIFICATION TEST")
print("=" * 80)
print(f"Test started: {datetime.now()}")
print()

# Test: Load PCP generation data and verify rooftop solar is present
print("Test: Loading PCP generation data with rooftop solar...")
print("-" * 80)

try:
    from aemo_dashboard.generation.gen_dash import EnergyDashboard

    # Create dashboard instance
    dashboard = EnergyDashboard()

    # Set to 30-day period for NSW
    dashboard.time_range = '30'
    dashboard.region = 'NSW1'

    print(f"Region: {dashboard.region}")
    print(f"Time range: {dashboard.time_range}")

    # Calculate PCP date range
    pcp_start, pcp_end = dashboard.calculate_pcp_date_range()
    print(f"PCP period: {pcp_start.date()} to {pcp_end.date()}")
    print()

    # Load PCP generation data
    print("Loading PCP generation data...")
    pcp_data = dashboard.get_pcp_generation_data()

    if pcp_data.empty:
        print("✗ FAILED: PCP data is empty")
        sys.exit(1)

    print(f"✓ PCP data loaded: {pcp_data.shape}")
    print(f"  Fuel types in PCP data: {list(pcp_data.columns)}")
    print()

    # Check if Rooftop Solar column exists
    if 'Rooftop Solar' in pcp_data.columns:
        rooftop_values = pcp_data['Rooftop Solar']
        rooftop_total = rooftop_values.sum()
        rooftop_max = rooftop_values.max()
        rooftop_mean = rooftop_values.mean()
        non_zero_count = (rooftop_values > 0).sum()

        print("Rooftop Solar statistics (PCP period):")
        print(f"  Total intervals: {len(rooftop_values)}")
        print(f"  Non-zero intervals: {non_zero_count}")
        print(f"  Sum: {rooftop_total:.1f} MW·intervals")
        print(f"  Max: {rooftop_max:.1f} MW")
        print(f"  Mean: {rooftop_mean:.1f} MW")
        print()

        if rooftop_total > 0:
            print("✓ SUCCESS: Rooftop Solar PCP data is loading correctly!")
            print(f"  Total rooftop solar detected: {rooftop_total:.1f} MW·intervals")
        else:
            print("✗ FAILED: Rooftop Solar column exists but all values are zero")
            sys.exit(1)
    else:
        print("✗ FAILED: Rooftop Solar column not found in PCP data")
        print(f"  Available columns: {list(pcp_data.columns)}")
        sys.exit(1)

    print()
    print("-" * 80)

    # Now calculate summary to verify it works end-to-end
    print("Test: Calculate generation summary with PCP...")
    print("-" * 80)

    pcp_summary = dashboard.calculate_generation_summary(pcp_data)

    if pcp_summary.empty:
        print("✗ FAILED: PCP summary is empty")
        sys.exit(1)

    print(f"✓ PCP summary calculated: {len(pcp_summary)} fuel types")
    print()
    print("PCP Summary:")
    for _, row in pcp_summary.iterrows():
        print(f"  {row['fuel_type']:15s}: {row['total_gwh']:8.1f} GWh ({row['percentage']:5.1f}%)")

    # Verify rooftop solar is in the summary
    rooftop_row = pcp_summary[pcp_summary['fuel_type'] == 'Rooftop Solar']
    if not rooftop_row.empty:
        rooftop_gwh = rooftop_row['total_gwh'].iloc[0]
        rooftop_pct = rooftop_row['percentage'].iloc[0]
        print()
        print(f"✓ Rooftop Solar in PCP summary: {rooftop_gwh:.1f} GWh ({rooftop_pct:.1f}%)")

        if rooftop_gwh > 0:
            print()
            print("=" * 80)
            print("ALL TESTS PASSED ✓")
            print("=" * 80)
            print()
            print("The rooftop solar PCP loading bug has been fixed!")
            print("Rooftop solar data is now correctly included in year-over-year comparisons.")
        else:
            print()
            print("✗ FAILED: Rooftop Solar in summary but value is 0 GWh")
            sys.exit(1)
    else:
        print()
        print("✗ FAILED: Rooftop Solar not found in PCP summary")
        sys.exit(1)

except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
