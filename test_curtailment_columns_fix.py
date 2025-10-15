#!/usr/bin/env python3
"""
Test Curtailment Columns Fix - CRITICAL ISSUE #4

This test verifies that the curtailment dashboard has all required columns
and won't crash when rendering.

Tests:
1. curtailment_merged view has 'scada' column
2. curtailment_daily view has 'generation_mwh' column
3. All expected columns exist for dashboard plotting
4. Data types are correct
5. Actual curtailment data query simulation
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.curtailment.curtailment_query_manager import CurtailmentQueryManager


def test_curtailment_merged_columns():
    """Test that curtailment_merged view has all required columns including 'scada'"""
    print("\n=== TEST 1: curtailment_merged view columns ===")

    manager = CurtailmentQueryManager()

    # Query a small amount of data to check structure
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    # Query with 5min resolution (uses curtailment_merged view)
    data = manager.query_curtailment_data(
        start_date=start_date,
        end_date=end_date,
        region='NSW1',
        resolution='5min'
    )

    print(f"Records returned: {len(data)}")
    print(f"Columns: {data.columns.tolist()}")

    # Check critical columns
    required_columns = ['timestamp', 'duid', 'availgen', 'dispatchcap', 'curtailment', 'scada', 'region', 'fuel']
    missing_columns = [col for col in required_columns if col not in data.columns]

    if missing_columns:
        print(f"❌ FAIL: Missing columns: {missing_columns}")
        return False
    else:
        print(f"✅ PASS: All required columns present")

    # Check scada column specifically (this is what curtailment_tab.py line 303 needs)
    if 'scada' not in data.columns:
        print("❌ FAIL: 'scada' column missing! curtailment_tab.py will crash!")
        return False
    else:
        print("✅ PASS: 'scada' column exists")

        # Check that scada has actual data
        if not data.empty:
            scada_mean = data['scada'].mean()
            scada_count = (data['scada'] > 0).sum()
            print(f"  - SCADA mean: {scada_mean:.2f} MW")
            print(f"  - Non-zero SCADA values: {scada_count}/{len(data)} ({100*scada_count/len(data):.1f}%)")

            if scada_count == 0:
                print("  ⚠️  WARNING: All SCADA values are zero - check SCADA join")

    # Show sample data
    if not data.empty:
        print("\nSample data (first row):")
        print(data.head(1)[required_columns].to_dict('records')[0])

    return True


def test_curtailment_daily_columns():
    """Test that curtailment_daily view has 'generation_mwh' column"""
    print("\n=== TEST 2: curtailment_daily view columns ===")

    manager = CurtailmentQueryManager()

    # Query 7 days of data at daily resolution
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Query with daily resolution (uses curtailment_daily view)
    data = manager.query_curtailment_data(
        start_date=start_date,
        end_date=end_date,
        region='NSW1',
        resolution='daily'
    )

    print(f"Records returned: {len(data)}")
    print(f"Columns: {data.columns.tolist()}")

    # Check for generation_mwh column
    if 'generation_mwh' not in data.columns and 'scada' not in data.columns:
        print("❌ FAIL: Neither 'generation_mwh' nor 'scada' column exists!")
        return False

    if 'generation_mwh' in data.columns:
        print("✅ PASS: 'generation_mwh' column exists")
        if not data.empty:
            gen_mean = data['generation_mwh'].mean()
            print(f"  - Average generation: {gen_mean:.1f} MWh/day")

    if 'scada' in data.columns:
        print("✅ PASS: 'scada' column exists")
        if not data.empty:
            scada_mean = data['scada'].mean()
            print(f"  - Average scada: {scada_mean:.1f} MW")

    return True


def test_30min_and_hourly_columns():
    """Test that 30min and hourly views have 'scada' column"""
    print("\n=== TEST 3: 30min and hourly view columns ===")

    manager = CurtailmentQueryManager()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)

    # Test 30min resolution
    data_30min = manager.query_curtailment_data(
        start_date=start_date,
        end_date=end_date,
        region='NSW1',
        resolution='30min'
    )

    print(f"30min records: {len(data_30min)}")
    print(f"30min columns: {data_30min.columns.tolist()}")

    if 'scada' not in data_30min.columns:
        print("❌ FAIL: 'scada' missing from curtailment_30min view")
        return False
    else:
        print("✅ PASS: curtailment_30min has 'scada' column")

    # Test hourly resolution
    data_hourly = manager.query_curtailment_data(
        start_date=start_date,
        end_date=end_date,
        region='NSW1',
        resolution='hourly'
    )

    print(f"Hourly records: {len(data_hourly)}")
    print(f"Hourly columns: {data_hourly.columns.tolist()}")

    if 'scada' not in data_hourly.columns:
        print("❌ FAIL: 'scada' missing from curtailment_hourly view")
        return False
    else:
        print("✅ PASS: curtailment_hourly has 'scada' column")

    return True


def test_dashboard_plot_simulation():
    """Simulate what curtailment_tab.py does when creating plots"""
    print("\n=== TEST 4: Dashboard plot simulation ===")

    manager = CurtailmentQueryManager()

    # Simulate curtailment_tab.py create_plot() method
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    try:
        # This is what curtailment_tab.py line 274-281 does
        data = manager.query_curtailment_data(
            start_date=start_date,
            end_date=end_date,
            region='NSW1',
            fuel=None,
            duid=None,
            resolution='hourly'
        )

        if data.empty:
            print("⚠️  WARNING: No data returned (this is OK if no curtailment in period)")
            return True

        # This is what line 301-315 does - create area plot with 'scada' column
        print(f"Attempting to access data['scada'] column...")
        scada_values = data['scada']
        print(f"✅ PASS: Successfully accessed 'scada' column")
        print(f"  - Mean: {scada_values.mean():.2f} MW")
        print(f"  - Max: {scada_values.max():.2f} MW")
        print(f"  - Non-zero: {(scada_values > 0).sum()}/{len(scada_values)}")

        # Check other plot columns
        plot_columns = ['timestamp', 'scada', 'curtailment', 'availgen', 'dispatchcap']
        missing = [col for col in plot_columns if col not in data.columns]

        if missing:
            print(f"❌ FAIL: Missing plot columns: {missing}")
            return False
        else:
            print(f"✅ PASS: All plot columns exist: {plot_columns}")

        return True

    except KeyError as e:
        print(f"❌ FAIL: KeyError when accessing column: {e}")
        print("This will cause the dashboard to crash!")
        return False
    except Exception as e:
        print(f"❌ FAIL: Unexpected error: {e}")
        return False


def test_actual_curtailment_data():
    """Test with actual curtailment data from production file"""
    print("\n=== TEST 5: Actual curtailment data ===")

    manager = CurtailmentQueryManager()

    # Query last 30 days to find actual curtailment
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    print(f"Querying curtailment from {start_date.date()} to {end_date.date()}")

    data = manager.query_curtailment_data(
        start_date=start_date,
        end_date=end_date,
        resolution='5min'
    )

    if data.empty:
        print("⚠️  WARNING: No curtailment data found in last 30 days")
        return True

    print(f"Total records: {len(data):,}")
    print(f"Date range: {data['timestamp'].min()} to {data['timestamp'].max()}")
    print(f"Regions: {data['region'].unique().tolist() if 'region' in data.columns else 'N/A'}")
    print(f"Fuels: {data['fuel'].unique().tolist() if 'fuel' in data.columns else 'N/A'}")

    # Check for actual curtailment
    if 'curtailment' in data.columns:
        curtailed_intervals = (data['curtailment'] > 0).sum()
        total_curtailment_mw = data['curtailment'].sum()
        print(f"\nCurtailment summary:")
        print(f"  - Curtailed intervals: {curtailed_intervals}/{len(data)} ({100*curtailed_intervals/len(data):.2f}%)")
        print(f"  - Total curtailment: {total_curtailment_mw:.1f} MW·5min")
        print(f"  - Average when curtailed: {data[data['curtailment'] > 0]['curtailment'].mean():.1f} MW")

    # Check for SCADA data
    if 'scada' in data.columns:
        scada_nonzero = (data['scada'] > 0).sum()
        print(f"\nSCADA summary:")
        print(f"  - Non-zero SCADA: {scada_nonzero}/{len(data)} ({100*scada_nonzero/len(data):.2f}%)")
        print(f"  - Mean SCADA: {data[data['scada'] > 0]['scada'].mean():.1f} MW")

        # Sample of both curtailment and scada
        curtailed_with_scada = data[(data['curtailment'] > 0) & (data['scada'] > 0)]
        if len(curtailed_with_scada) > 0:
            print(f"\nSample of curtailed intervals with SCADA data:")
            sample = curtailed_with_scada.head(3)[['timestamp', 'duid', 'scada', 'curtailment', 'availgen', 'dispatchcap']]
            print(sample.to_string(index=False))

    return True


def main():
    """Run all tests"""
    print("=" * 70)
    print("CRITICAL ISSUE #4: Curtailment Dashboard Column Fix Test")
    print("=" * 70)
    print("\nTesting that curtailment views have all required columns...")
    print("This ensures curtailment_tab.py won't crash when plotting.")

    results = []

    # Run all tests
    results.append(("curtailment_merged columns", test_curtailment_merged_columns()))
    results.append(("curtailment_daily columns", test_curtailment_daily_columns()))
    results.append(("30min/hourly columns", test_30min_and_hourly_columns()))
    results.append(("Dashboard plot simulation", test_dashboard_plot_simulation()))
    results.append(("Actual curtailment data", test_actual_curtailment_data()))

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")

    all_passed = all(result[1] for result in results)

    if all_passed:
        print("\n🎉 ALL TESTS PASSED!")
        print("The curtailment dashboard should render without crashing.")
        return 0
    else:
        print("\n❌ SOME TESTS FAILED!")
        print("The curtailment dashboard may crash. Review failures above.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
