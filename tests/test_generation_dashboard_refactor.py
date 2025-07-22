#!/usr/bin/env python3
"""
Comprehensive test suite for Generation Dashboard DuckDB refactoring
Tests memory usage, performance, data accuracy, and UI functionality
"""

import os
import sys
import time
import psutil
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, Tuple, List
import gc

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.duckdb_views import view_manager
from aemo_dashboard.shared.logging_config import setup_logging

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)

class TestResults:
    """Store and report test results"""
    def __init__(self):
        self.tests = []
        self.passed = 0
        self.failed = 0
        
    def add_test(self, name: str, passed: bool, details: str):
        self.tests.append({
            'name': name,
            'passed': passed,
            'details': details
        })
        if passed:
            self.passed += 1
        else:
            self.failed += 1
    
    def print_summary(self):
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        print(f"Total tests: {len(self.tests)}")
        print(f"Passed: {self.passed} ✅")
        print(f"Failed: {self.failed} ❌")
        print("\nDetailed Results:")
        print("-"*80)
        
        for test in self.tests:
            status = "✅ PASS" if test['passed'] else "❌ FAIL"
            print(f"{status} | {test['name']}")
            print(f"       {test['details']}")
            print("-"*80)
        
        return self.failed == 0

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def measure_memory_and_time(func, *args, **kwargs):
    """Measure memory usage and execution time of a function"""
    gc.collect()
    start_memory = get_memory_usage()
    start_time = time.time()
    
    result = func(*args, **kwargs)
    
    end_time = time.time()
    end_memory = get_memory_usage()
    
    memory_used = end_memory - start_memory
    time_taken = end_time - start_time
    
    return result, memory_used, time_taken

class GenerationDashboardTester:
    """Test the refactored generation dashboard"""
    
    def __init__(self):
        self.results = TestResults()
        self.manager = None
        
    def setup(self):
        """Initialize test environment"""
        try:
            print("Setting up test environment...")
            self.manager = GenerationQueryManager()
            print("✅ GenerationQueryManager initialized")
            
            # Verify views exist by trying a simple query
            test_query = "SELECT COUNT(*) as count FROM generation_by_fuel_30min LIMIT 1"
            try:
                result = view_manager.conn.execute(test_query).fetchdf()
                print("✅ Generation views verified")
            except:
                print("⚠️  Generation views may not exist, will be created on first use")
            
            return True
        except Exception as e:
            print(f"❌ Setup failed: {e}")
            return False
    
    def test_memory_usage(self):
        """Test 1: Memory usage with different date ranges"""
        print("\n" + "="*60)
        print("TEST 1: MEMORY USAGE")
        print("="*60)
        
        test_cases = [
            ("24 hours", 1, 100),
            ("7 days", 7, 200),
            ("30 days", 30, 300),
            ("1 year", 365, 400),
            ("All data (5+ years)", 2000, 500)
        ]
        
        for description, days, memory_limit_mb in test_cases:
            gc.collect()
            start_memory = get_memory_usage()
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            try:
                # Query data
                data, memory_used, time_taken = measure_memory_and_time(
                    self.manager.query_generation_by_fuel,
                    start_date, end_date, 'NEM'
                )
                
                passed = memory_used < memory_limit_mb
                details = f"{description}: {memory_used:.1f}MB used (limit: {memory_limit_mb}MB), " \
                         f"{len(data):,} records in {time_taken:.2f}s"
                
                self.results.add_test(f"Memory - {description}", passed, details)
                
                print(f"{'✅' if passed else '❌'} {details}")
                
            except Exception as e:
                self.results.add_test(f"Memory - {description}", False, f"Error: {str(e)}")
                print(f"❌ {description}: Error - {str(e)}")
    
    def test_performance(self):
        """Test 2: Performance with different date ranges"""
        print("\n" + "="*60)
        print("TEST 2: PERFORMANCE")
        print("="*60)
        
        test_cases = [
            ("24 hours", 1, 1.0),
            ("30 days", 30, 2.0),
            ("1 year", 365, 3.0),
            ("All data", 2000, 5.0)
        ]
        
        for description, days, time_limit in test_cases:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            try:
                # First query (cold cache)
                _, _, time_cold = measure_memory_and_time(
                    self.manager.query_generation_by_fuel,
                    start_date, end_date, 'NEM'
                )
                
                # Second query (warm cache)
                _, _, time_warm = measure_memory_and_time(
                    self.manager.query_generation_by_fuel,
                    start_date, end_date, 'NEM'
                )
                
                passed = time_cold < time_limit
                speedup = time_cold / time_warm if time_warm > 0 else 0
                
                details = f"{description}: Cold: {time_cold:.2f}s, Warm: {time_warm:.2f}s " \
                         f"(limit: {time_limit}s), Cache speedup: {speedup:.1f}x"
                
                self.results.add_test(f"Performance - {description}", passed, details)
                
                print(f"{'✅' if passed else '❌'} {details}")
                
            except Exception as e:
                self.results.add_test(f"Performance - {description}", False, f"Error: {str(e)}")
                print(f"❌ {description}: Error - {str(e)}")
    
    def test_data_accuracy(self):
        """Test 3: Data accuracy and aggregation correctness"""
        print("\n" + "="*60)
        print("TEST 3: DATA ACCURACY")
        print("="*60)
        
        # Test a specific day where we can verify results
        test_date = datetime.now() - timedelta(days=7)
        start_date = test_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        try:
            # Get aggregated data
            nem_data = self.manager.query_generation_by_fuel(start_date, end_date, 'NEM')
            
            # Get individual region data
            regions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
            region_totals = {}
            
            for region in regions:
                region_data = self.manager.query_generation_by_fuel(start_date, end_date, region)
                if not region_data.empty:
                    # Group by fuel type and sum
                    region_totals[region] = region_data.groupby('fuel_type')['total_generation_mw'].sum()
            
            # Verify NEM totals match sum of regions
            if not nem_data.empty and region_totals:
                nem_by_fuel = nem_data.groupby('fuel_type')['total_generation_mw'].sum()
                
                # Sum all regions by fuel type
                all_regions_sum = pd.Series(dtype='float64')
                for region, totals in region_totals.items():
                    all_regions_sum = all_regions_sum.add(totals, fill_value=0)
                
                # Compare totals
                fuel_types = set(nem_by_fuel.index) | set(all_regions_sum.index)
                
                accuracy_ok = True
                details_list = []
                
                for fuel in sorted(fuel_types):
                    nem_value = nem_by_fuel.get(fuel, 0)
                    regions_sum = all_regions_sum.get(fuel, 0)
                    diff_pct = abs(nem_value - regions_sum) / max(nem_value, 1) * 100
                    
                    if diff_pct > 0.1:  # Allow 0.1% tolerance
                        accuracy_ok = False
                        details_list.append(f"{fuel}: NEM={nem_value:.0f}, Regions={regions_sum:.0f} "
                                          f"(diff: {diff_pct:.2f}%)")
                
                if accuracy_ok:
                    details = f"All fuel types match within 0.1% tolerance. " \
                             f"Checked {len(fuel_types)} fuel types"
                else:
                    details = "Mismatches found: " + "; ".join(details_list[:3])
                
                self.results.add_test("Data Accuracy - NEM vs Regions", accuracy_ok, details)
                print(f"{'✅' if accuracy_ok else '❌'} {details}")
                
            else:
                self.results.add_test("Data Accuracy - NEM vs Regions", False, "No data found")
                print("❌ No data found for accuracy test")
                
        except Exception as e:
            self.results.add_test("Data Accuracy", False, f"Error: {str(e)}")
            print(f"❌ Data accuracy test error: {str(e)}")
    
    def test_region_filtering(self):
        """Test 4: Region filtering functionality"""
        print("\n" + "="*60)
        print("TEST 4: REGION FILTERING")
        print("="*60)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        regions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1', 'NEM']
        
        all_passed = True
        details_list = []
        
        for region in regions:
            try:
                data = self.manager.query_generation_by_fuel(start_date, end_date, region)
                
                if data.empty:
                    all_passed = False
                    details_list.append(f"{region}: No data")
                else:
                    total_gen = data['total_generation_mw'].sum()
                    details_list.append(f"{region}: {len(data):,} records, "
                                      f"{total_gen:,.0f} MW total")
                
            except Exception as e:
                all_passed = False
                details_list.append(f"{region}: Error - {str(e)}")
        
        details = "; ".join(details_list[:3]) + (f" + {len(details_list)-3} more" 
                                                  if len(details_list) > 3 else "")
        
        self.results.add_test("Region Filtering", all_passed, details)
        print(f"{'✅' if all_passed else '❌'} {details}")
    
    def test_cache_functionality(self):
        """Test 5: Cache functionality and performance"""
        print("\n" + "="*60)
        print("TEST 5: CACHE FUNCTIONALITY")
        print("="*60)
        
        # Clear cache first
        self.manager.clear_cache()
        initial_stats = self.manager.get_statistics()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Make same query 3 times
        times = []
        for i in range(3):
            _, _, time_taken = measure_memory_and_time(
                self.manager.query_generation_by_fuel,
                start_date, end_date, 'NSW1'
            )
            times.append(time_taken)
        
        final_stats = self.manager.get_statistics()
        
        # Check cache behavior
        cache_hits = final_stats.get('cache_hits', 0) - initial_stats.get('cache_hits', 0)
        cache_misses = final_stats.get('cache_misses', 0) - initial_stats.get('cache_misses', 0)
        
        # First query should be miss, next 2 should be hits
        cache_working = cache_misses == 1 and cache_hits == 2
        
        # Cache should be significantly faster
        speedup = times[0] / min(times[1], times[2]) if min(times[1], times[2]) > 0 else 0
        performance_ok = speedup > 10  # Expect at least 10x speedup
        
        passed = cache_working and performance_ok
        
        details = f"Hits: {cache_hits}, Misses: {cache_misses}, " \
                 f"Times: {times[0]:.3f}s → {times[1]:.3f}s → {times[2]:.3f}s, " \
                 f"Speedup: {speedup:.1f}x"
        
        self.results.add_test("Cache Functionality", passed, details)
        print(f"{'✅' if passed else '❌'} {details}")
    
    def test_edge_cases(self):
        """Test 6: Edge cases and error handling"""
        print("\n" + "="*60)
        print("TEST 6: EDGE CASES")
        print("="*60)
        
        test_cases = []
        
        # Test 1: Empty date range
        try:
            end_date = datetime.now()
            start_date = end_date
            data = self.manager.query_generation_by_fuel(start_date, end_date, 'NEM')
            passed = isinstance(data, pd.DataFrame)
            details = f"Empty date range: Returned {'empty' if data.empty else 'non-empty'} DataFrame"
            test_cases.append((passed, details))
        except Exception as e:
            test_cases.append((False, f"Empty date range: Error - {str(e)}"))
        
        # Test 2: Invalid region
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)
            data = self.manager.query_generation_by_fuel(start_date, end_date, 'INVALID')
            passed = isinstance(data, pd.DataFrame) and data.empty
            details = f"Invalid region: Returned {'empty' if data.empty else 'non-empty'} DataFrame"
            test_cases.append((passed, details))
        except Exception as e:
            test_cases.append((False, f"Invalid region: Error - {str(e)}"))
        
        # Test 3: Exactly 30 days (boundary condition)
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            data = self.manager.query_generation_by_fuel(start_date, end_date, 'NEM')
            passed = not data.empty
            details = f"30-day boundary: {len(data):,} records returned"
            test_cases.append((passed, details))
        except Exception as e:
            test_cases.append((False, f"30-day boundary: Error - {str(e)}"))
        
        all_passed = all(tc[0] for tc in test_cases)
        details = "; ".join(tc[1] for tc in test_cases[:2])
        
        self.results.add_test("Edge Cases", all_passed, details)
        print(f"{'✅' if all_passed else '❌'} {details}")
    
    def test_capacity_utilization(self):
        """Test 7: Capacity utilization queries"""
        print("\n" + "="*60)
        print("TEST 7: CAPACITY UTILIZATION")
        print("="*60)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        try:
            # Test capacity utilization query
            util_data = self.manager.query_capacity_utilization(start_date, end_date, 'NEM')
            
            if not util_data.empty:
                # Check utilization values are reasonable (0-100%)
                min_util = util_data['utilization_pct'].min()
                max_util = util_data['utilization_pct'].max()
                avg_util = util_data['utilization_pct'].mean()
                
                passed = min_util >= 0 and max_util <= 100
                details = f"{len(util_data):,} records, Utilization range: " \
                         f"{min_util:.1f}% - {max_util:.1f}% (avg: {avg_util:.1f}%)"
            else:
                passed = False
                details = "No utilization data returned"
            
            self.results.add_test("Capacity Utilization", passed, details)
            print(f"{'✅' if passed else '❌'} {details}")
            
        except Exception as e:
            self.results.add_test("Capacity Utilization", False, f"Error: {str(e)}")
            print(f"❌ Capacity utilization test error: {str(e)}")
    
    def test_fuel_capacities(self):
        """Test 8: Fuel capacity queries"""
        print("\n" + "="*60)
        print("TEST 8: FUEL CAPACITIES")
        print("="*60)
        
        try:
            # Test fuel capacity query
            capacities = self.manager.query_fuel_capacities('NEM')
            
            if capacities:
                # Check we have expected fuel types
                expected_fuels = ['Black Coal', 'Brown Coal', 'Gas', 'Hydro', 'Wind', 'Solar']
                found_fuels = [f for f in expected_fuels if f in capacities]
                
                passed = len(found_fuels) >= 4  # Should have at least 4 major fuel types
                
                total_capacity = sum(capacities.values())
                details = f"Found {len(capacities)} fuel types, Total capacity: " \
                         f"{total_capacity:,.0f} MW, Major fuels: {', '.join(found_fuels[:3])}"
            else:
                passed = False
                details = "No fuel capacities returned"
            
            self.results.add_test("Fuel Capacities", passed, details)
            print(f"{'✅' if passed else '❌'} {details}")
            
        except Exception as e:
            self.results.add_test("Fuel Capacities", False, f"Error: {str(e)}")
            print(f"❌ Fuel capacities test error: {str(e)}")
    
    def run_all_tests(self):
        """Run all tests"""
        print("\n" + "="*80)
        print("GENERATION DASHBOARD REFACTORING - COMPREHENSIVE TEST SUITE")
        print("="*80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self.setup():
            print("❌ Setup failed, cannot continue tests")
            return False
        
        # Run all test methods
        self.test_memory_usage()
        self.test_performance()
        self.test_data_accuracy()
        self.test_region_filtering()
        self.test_cache_functionality()
        self.test_edge_cases()
        self.test_capacity_utilization()
        self.test_fuel_capacities()
        
        # Print summary
        all_passed = self.results.print_summary()
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if all_passed:
            print("\n🎉 ALL TESTS PASSED! The generation dashboard refactoring is ready for production.")
        else:
            print("\n⚠️  Some tests failed. Please review the results above.")
        
        return all_passed


def main():
    """Main test runner"""
    tester = GenerationDashboardTester()
    success = tester.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()