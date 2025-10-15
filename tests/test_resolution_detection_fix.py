#!/usr/bin/env python3
"""
Comprehensive tests for resolution detection fix (CRITICAL ISSUE #6)

Tests the new resolution_utils module and verifies it works correctly
with both 5-minute and 30-minute production data.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from aemo_dashboard.shared.resolution_utils import (
    detect_resolution_minutes,
    periods_for_hours,
    periods_for_days,
    detect_and_calculate_periods,
    get_decay_rate_per_period
)


class TestResolutionDetection:
    """Test suite for resolution detection utilities"""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def test_detect_resolution_5min(self):
        """Test detection of 5-minute resolution data"""
        print("\n[TEST] Detecting 5-minute resolution...")
        try:
            # Create 5-minute timestamp series
            timestamps = pd.date_range('2025-01-01', periods=100, freq='5min')
            resolution = detect_resolution_minutes(timestamps)

            assert resolution == 5, f"Expected 5, got {resolution}"
            print("  ✓ Correctly detected 5-minute resolution")
            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_detect_resolution_5min: {e}")

    def test_detect_resolution_30min(self):
        """Test detection of 30-minute resolution data"""
        print("\n[TEST] Detecting 30-minute resolution...")
        try:
            # Create 30-minute timestamp series
            timestamps = pd.date_range('2025-01-01', periods=100, freq='30min')
            resolution = detect_resolution_minutes(timestamps)

            assert resolution == 30, f"Expected 30, got {resolution}"
            print("  ✓ Correctly detected 30-minute resolution")
            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_detect_resolution_30min: {e}")

    def test_detect_resolution_hourly(self):
        """Test detection of hourly resolution data"""
        print("\n[TEST] Detecting 60-minute resolution...")
        try:
            # Create hourly timestamp series
            timestamps = pd.date_range('2025-01-01', periods=100, freq='60min')
            resolution = detect_resolution_minutes(timestamps)

            assert resolution == 60, f"Expected 60, got {resolution}"
            print("  ✓ Correctly detected 60-minute resolution")
            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_detect_resolution_hourly: {e}")

    def test_periods_for_hours_5min(self):
        """Test period calculation for 5-minute data"""
        print("\n[TEST] Calculating periods for 5-minute data...")
        try:
            # 24 hours of 5-minute data should be 288 periods
            periods = periods_for_hours(24, 5)
            assert periods == 288, f"Expected 288, got {periods}"
            print(f"  ✓ 24 hours @ 5min = {periods} periods")

            # 2 hours of 5-minute data should be 24 periods
            periods = periods_for_hours(2, 5)
            assert periods == 24, f"Expected 24, got {periods}"
            print(f"  ✓ 2 hours @ 5min = {periods} periods")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_periods_for_hours_5min: {e}")

    def test_periods_for_hours_30min(self):
        """Test period calculation for 30-minute data"""
        print("\n[TEST] Calculating periods for 30-minute data...")
        try:
            # 24 hours of 30-minute data should be 48 periods
            periods = periods_for_hours(24, 30)
            assert periods == 48, f"Expected 48, got {periods}"
            print(f"  ✓ 24 hours @ 30min = {periods} periods")

            # 2 hours of 30-minute data should be 4 periods
            periods = periods_for_hours(2, 30)
            assert periods == 4, f"Expected 4, got {periods}"
            print(f"  ✓ 2 hours @ 30min = {periods} periods")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_periods_for_hours_30min: {e}")

    def test_periods_for_days(self):
        """Test period calculation for days"""
        print("\n[TEST] Calculating periods for days...")
        try:
            # 1 day of 5-minute data should be 288 periods
            periods = periods_for_days(1, 5)
            assert periods == 288, f"Expected 288, got {periods}"
            print(f"  ✓ 1 day @ 5min = {periods} periods")

            # 1 day of 30-minute data should be 48 periods
            periods = periods_for_days(1, 30)
            assert periods == 48, f"Expected 48, got {periods}"
            print(f"  ✓ 1 day @ 30min = {periods} periods")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_periods_for_days: {e}")

    def test_detect_and_calculate(self):
        """Test combined detection and calculation"""
        print("\n[TEST] Combined detection and calculation...")
        try:
            # 5-minute data
            timestamps_5min = pd.date_range('2025-01-01', periods=100, freq='5min')
            periods = detect_and_calculate_periods(timestamps_5min, 24)
            assert periods == 288, f"Expected 288, got {periods}"
            print(f"  ✓ Auto-detected 5min and calculated 24h = {periods} periods")

            # 30-minute data
            timestamps_30min = pd.date_range('2025-01-01', periods=100, freq='30min')
            periods = detect_and_calculate_periods(timestamps_30min, 24)
            assert periods == 48, f"Expected 48, got {periods}"
            print(f"  ✓ Auto-detected 30min and calculated 24h = {periods} periods")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_detect_and_calculate: {e}")

    def test_decay_rate_calculation(self):
        """Test decay rate calculation for different resolutions"""
        print("\n[TEST] Decay rate calculation...")
        try:
            # For 2-hour half-life with 5-minute data
            decay_5min = get_decay_rate_per_period(2.0, 5)

            # Verify it produces correct half-life
            periods_2h = periods_for_hours(2, 5)  # 24 periods
            value_after_2h = decay_5min ** periods_2h

            assert abs(value_after_2h - 0.5) < 0.01, \
                f"Expected ~0.5 after 2h, got {value_after_2h:.4f}"
            print(f"  ✓ 5min: decay_rate={decay_5min:.4f}, value@2h={value_after_2h:.4f}")

            # For 2-hour half-life with 30-minute data
            decay_30min = get_decay_rate_per_period(2.0, 30)

            # Verify it produces correct half-life
            periods_2h = periods_for_hours(2, 30)  # 4 periods
            value_after_2h = decay_30min ** periods_2h

            assert abs(value_after_2h - 0.5) < 0.01, \
                f"Expected ~0.5 after 2h, got {value_after_2h:.4f}"
            print(f"  ✓ 30min: decay_rate={decay_30min:.4f}, value@2h={value_after_2h:.4f}")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_decay_rate_calculation: {e}")

    def test_production_data_5min(self, data_path):
        """Test with actual 5-minute production data"""
        print("\n[TEST] Production 5-minute data (scada5.parquet)...")
        try:
            scada5_path = Path(data_path) / 'scada5.parquet'

            if not scada5_path.exists():
                print(f"  ⚠ SKIPPED: {scada5_path} not found")
                return

            # Load small sample - sort by timestamp and drop duplicates
            df = pd.read_parquet(scada5_path, columns=['settlementdate'])
            df = df.drop_duplicates(subset=['settlementdate']).sort_values('settlementdate')
            df = df.head(1000)  # Just use first 1000 rows

            timestamps = pd.to_datetime(df['settlementdate']).sort_values()

            # Get unique sorted timestamps for resolution detection
            timestamps = pd.DatetimeIndex(timestamps.unique())

            resolution = detect_resolution_minutes(timestamps)

            assert resolution == 5, f"Expected 5, got {resolution}"
            print(f"  ✓ Detected resolution: {resolution} minutes")
            print(f"  ✓ Data range: {timestamps.min()} to {timestamps.max()}")
            print(f"  ✓ Records: {len(timestamps):,}")

            # Test period calculation
            periods_24h = periods_for_hours(24, resolution)
            print(f"  ✓ 24h = {periods_24h} periods")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_production_data_5min: {e}")

    def test_production_data_30min(self, data_path):
        """Test with actual 30-minute production data"""
        print("\n[TEST] Production 30-minute data (scada30.parquet)...")
        try:
            scada30_path = Path(data_path) / 'scada30.parquet'

            if not scada30_path.exists():
                print(f"  ⚠ SKIPPED: {scada30_path} not found")
                return

            # Load small sample - sort by timestamp and drop duplicates
            df = pd.read_parquet(scada30_path, columns=['settlementdate'])
            df = df.drop_duplicates(subset=['settlementdate']).sort_values('settlementdate')
            df = df.head(1000)  # Just use first 1000 rows

            timestamps = pd.to_datetime(df['settlementdate']).sort_values()

            # Get unique sorted timestamps for resolution detection
            timestamps = pd.DatetimeIndex(timestamps.unique())

            resolution = detect_resolution_minutes(timestamps)

            assert resolution == 30, f"Expected 30, got {resolution}"
            print(f"  ✓ Detected resolution: {resolution} minutes")
            print(f"  ✓ Data range: {timestamps.min()} to {timestamps.max()}")
            print(f"  ✓ Records: {len(timestamps):,}")

            # Test period calculation
            periods_24h = periods_for_hours(24, resolution)
            print(f"  ✓ 24h = {periods_24h} periods")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_production_data_30min: {e}")

    def test_edge_cases(self):
        """Test edge cases and error handling"""
        print("\n[TEST] Edge cases...")
        try:
            # Empty timestamps
            try:
                detect_resolution_minutes(pd.DatetimeIndex([]))
                assert False, "Should raise ValueError for empty timestamps"
            except ValueError:
                print("  ✓ Correctly raises ValueError for empty timestamps")

            # Single timestamp
            try:
                detect_resolution_minutes(pd.DatetimeIndex(['2025-01-01']))
                assert False, "Should raise ValueError for single timestamp"
            except ValueError:
                print("  ✓ Correctly raises ValueError for single timestamp")

            # Zero resolution
            try:
                periods_for_hours(24, 0)
                assert False, "Should raise ValueError for zero resolution"
            except ValueError:
                print("  ✓ Correctly raises ValueError for zero resolution")

            # Negative hours
            try:
                periods_for_hours(-1, 5)
                assert False, "Should raise ValueError for negative hours"
            except ValueError:
                print("  ✓ Correctly raises ValueError for negative hours")

            self.passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            self.failed += 1
            self.errors.append(f"test_edge_cases: {e}")

    def run_all_tests(self, data_path='/Volumes/davidleitch/aemo_production/data'):
        """Run all tests"""
        print("=" * 70)
        print("RESOLUTION DETECTION FIX - COMPREHENSIVE TEST SUITE")
        print("=" * 70)

        # Unit tests
        self.test_detect_resolution_5min()
        self.test_detect_resolution_30min()
        self.test_detect_resolution_hourly()
        self.test_periods_for_hours_5min()
        self.test_periods_for_hours_30min()
        self.test_periods_for_days()
        self.test_detect_and_calculate()
        self.test_decay_rate_calculation()
        self.test_edge_cases()

        # Production data tests
        self.test_production_data_5min(data_path)
        self.test_production_data_30min(data_path)

        # Print summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")

        if self.errors:
            print("\nERRORS:")
            for error in self.errors:
                print(f"  • {error}")

        print("\n" + "=" * 70)
        if self.failed == 0:
            print("✓ ALL TESTS PASSED")
            print("=" * 70)
            return True
        else:
            print(f"✗ {self.failed} TEST(S) FAILED")
            print("=" * 70)
            return False


def main():
    """Main test runner"""
    # Determine data path based on machine
    if os.path.exists('/Volumes/davidleitch/aemo_production/data'):
        # Development machine
        data_path = '/Volumes/davidleitch/aemo_production/data'
        print("Running on DEVELOPMENT machine")
    elif os.path.exists('/Users/davidleitch/aemo_production/data'):
        # Production machine
        data_path = '/Users/davidleitch/aemo_production/data'
        print("Running on PRODUCTION machine")
    else:
        # Fallback
        data_path = None
        print("WARNING: Could not find production data path")

    # Run tests
    tester = TestResolutionDetection()
    success = tester.run_all_tests(data_path)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
