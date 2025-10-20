#!/usr/bin/env python3
"""
Test curtailment rate calculations to diagnose why rates are too high
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.curtailment.curtailment_query_manager import CurtailmentQueryManager

def test_curtailment_rates():
    """Test the curtailment rate calculation for top units"""

    print("=" * 80)
    print("CURTAILMENT RATE DIAGNOSIS")
    print("=" * 80)

    qm = CurtailmentQueryManager()

    # Query last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    print(f"\nQuerying data from {start_date.date()} to {end_date.date()}")

    # Get top curtailed units
    top_units = qm.query_top_curtailed_units(start_date, end_date, limit=5)

    if top_units.empty:
        print("❌ No data returned")
        return 1

    print(f"\n✅ Retrieved {len(top_units)} units")
    print("\nColumn names:", list(top_units.columns))

    # Display the results
    print("\n" + "=" * 80)
    print("TOP CURTAILED UNITS - DETAILED BREAKDOWN")
    print("=" * 80)

    for idx, row in top_units.iterrows():
        print(f"\n{idx+1}. {row['duid']} ({row['region']} - {row['fuel']})")
        print("-" * 60)

        curtailed_mwh = row['total_curtailment_mwh']
        actual_mwh = row.get('actual_generation_mwh', 0)
        rate = row.get('curtailment_rate_pct', 0)

        print(f"  Curtailed (MWh):      {curtailed_mwh:>12,.1f}")
        print(f"  Actual Output (MWh):  {actual_mwh:>12,.1f}")
        print(f"  Total (Curt + Act):   {curtailed_mwh + actual_mwh:>12,.1f}")
        print(f"  Curtailment Rate:     {rate:>12.1f}%")

        # Calculate what the rate SHOULD be
        if curtailed_mwh + actual_mwh > 0:
            correct_rate = (curtailed_mwh / (curtailed_mwh + actual_mwh)) * 100
            print(f"  Calculated Rate:      {correct_rate:>12.1f}%")

            if abs(rate - correct_rate) > 0.1:
                print(f"  ⚠️  MISMATCH: {abs(rate - correct_rate):.1f}% difference")

        # Check if actual generation is suspiciously low
        if actual_mwh < curtailed_mwh * 0.1:
            print(f"  ⚠️  WARNING: Actual generation is very low compared to curtailment")
            print(f"      This suggests SCADA data may be missing for this DUID")

    # Also get regional summary for comparison
    print("\n" + "=" * 80)
    print("REGIONAL SUMMARY (for comparison)")
    print("=" * 80)

    summary = qm.query_region_summary(start_date, end_date)

    if not summary.empty:
        for idx, row in summary.iterrows():
            print(f"\n{row['region']}:")
            print(f"  Units: {row['unit_count']}")
            print(f"  Curtailed: {row['total_curtailment_mwh']:,.0f} MWh")
            print(f"  Actual: {row['actual_generation_mwh']:,.0f} MWh")
            print(f"  Rate: {row['curtailment_rate_pct']:.1f}%")

    print("\n" + "=" * 80)
    print("DIAGNOSIS COMPLETE")
    print("=" * 80)

    return 0

if __name__ == '__main__':
    try:
        exit_code = test_curtailment_rates()
        sys.exit(exit_code)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
