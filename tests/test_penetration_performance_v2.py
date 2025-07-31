#!/usr/bin/env python3
"""
Test script to analyze Penetration tab performance focusing on actual bottlenecks.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import psutil
import os
from scipy.signal import savgol_filter
from statsmodels.nonparametric.smoothers_lowess import lowess
import matplotlib.pyplot as plt
from pathlib import Path

# Add the src directory to the path
import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))

from aemo_dashboard.penetration.penetration_tab import PenetrationTab
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.config import Config
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

class ImprovedPerformanceTester:
    def __init__(self):
        self.query_manager = GenerationQueryManager()
        self.config = Config()
        
    def measure_memory(self):
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def profile_current_implementation(self):
        """Profile the current implementation to find bottlenecks"""
        logger.info("\n=== PROFILING CURRENT IMPLEMENTATION ===")
        
        # Test loading data for different time ranges
        test_ranges = [
            ("1 month", datetime(2025, 6, 1), datetime(2025, 6, 30)),
            ("6 months", datetime(2025, 1, 1), datetime(2025, 6, 30)),
            ("1 year", datetime(2024, 7, 1), datetime(2025, 6, 30)),
            ("3 years", datetime(2022, 7, 1), datetime(2025, 6, 30))
        ]
        
        results = []
        
        for name, start_date, end_date in test_ranges:
            start_time = time.time()
            start_mem = self.measure_memory()
            
            # Query 30-minute data
            data = self.query_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region='NEM',
                resolution='30min'
            )
            
            query_time = time.time() - start_time
            query_mem = self.measure_memory() - start_mem
            
            if not data.empty:
                # Filter for VRE
                process_start = time.time()
                df_vre = data[data['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
                
                # Group and sum
                df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
                hourly_sum = df_vre.groupby('settlementdate')['total_generation_mw'].sum().reset_index()
                
                # Apply rolling average
                roll_start = time.time()
                hourly_sum['mw_rolling_30d'] = hourly_sum['total_generation_mw'].rolling(
                    window=1440, center=False, min_periods=720
                ).mean()
                roll_time = time.time() - roll_start
                
                process_time = time.time() - process_start
                
                results.append({
                    'range': name,
                    'data_points': len(hourly_sum),
                    'query_time': query_time,
                    'process_time': process_time,
                    'rolling_time': roll_time,
                    'total_time': query_time + process_time,
                    'memory': query_mem
                })
                
                logger.info(f"{name}: {len(hourly_sum)} points, "
                          f"Query: {query_time:.2f}s, Process: {process_time:.2f}s, "
                          f"Rolling: {roll_time:.2f}s, Memory: {query_mem:.1f}MB")
        
        return results
    
    def test_daily_aggregation(self):
        """Test performance with daily pre-aggregation"""
        logger.info("\n=== TESTING DAILY AGGREGATION ===")
        
        # Simulate daily aggregation in the database view
        years = [2022, 2023, 2024, 2025]
        start_time = time.time()
        start_mem = self.measure_memory()
        
        all_data = []
        for year in years:
            # Get 30-min data for the year
            data = self.query_manager.query_generation_by_fuel(
                start_date=datetime(year, 1, 1),
                end_date=datetime(year, 12, 31),
                region='NEM',
                resolution='30min'
            )
            
            if not data.empty:
                # Filter VRE
                df_vre = data[data['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
                
                # Daily aggregation
                df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
                df_vre['date'] = df_vre['settlementdate'].dt.date
                
                daily = df_vre.groupby('date')['total_generation_mw'].mean().reset_index()
                daily.columns = ['date', 'avg_mw']
                all_data.append(daily)
        
        if all_data:
            daily_data = pd.concat(all_data)
            
            # Apply 30-day rolling on daily data
            roll_start = time.time()
            daily_data['mw_rolling_30d'] = daily_data['avg_mw'].rolling(
                window=30, center=False, min_periods=15
            ).mean()
            roll_time = time.time() - roll_start
        
        total_time = time.time() - start_time
        total_mem = self.measure_memory() - start_mem
        
        logger.info(f"Daily aggregation: {len(daily_data)} points, "
                  f"Total time: {total_time:.2f}s, Rolling time: {roll_time:.3f}s, "
                  f"Memory: {total_mem:.1f}MB")
        
        return daily_data, total_time
    
    def test_loess_alternatives(self, daily_data):
        """Test different LOESS parameters for performance vs quality"""
        logger.info("\n=== TESTING LOESS PARAMETERS ===")
        
        if daily_data is None or daily_data.empty:
            return
        
        # Convert date to numeric for LOESS
        daily_data = daily_data.copy()
        daily_data['date_numeric'] = pd.to_datetime(daily_data['date']).astype(np.int64) / 1e9
        
        fracs = [0.02, 0.05, 0.10, 0.15]
        results = []
        
        for frac in fracs:
            start_time = time.time()
            
            try:
                smoothed = lowess(
                    daily_data['avg_mw'], 
                    daily_data['date_numeric'],
                    frac=frac,
                    it=0,  # No iterations for speed
                    return_sorted=False
                )
                
                smooth_time = time.time() - start_time
                
                results.append({
                    'frac': frac,
                    'time': smooth_time,
                    'smoothed': smoothed
                })
                
                logger.info(f"LOESS frac={frac}: {smooth_time:.3f}s")
            except Exception as e:
                logger.error(f"LOESS frac={frac} failed: {e}")
        
        return results
    
    def create_optimized_tab(self):
        """Create an optimized version of the penetration tab"""
        logger.info("\n=== CREATING OPTIMIZED TAB ===")
        
        class OptimizedPenetrationTab(PenetrationTab):
            def _get_generation_data(self, years, months_only_first_year=None):
                """Optimized data loading with daily aggregation"""
                all_data = []
                
                for i, year in enumerate(years):
                    if i == 0 and months_only_first_year is not None:
                        start_date = datetime(year, 12 - months_only_first_year + 1, 1)
                    else:
                        start_date = datetime(year, 1, 1)
                    end_date = datetime(year, 12, 31, 23, 59, 59)
                    
                    # Get 30-min data
                    data = self.query_manager.query_generation_by_fuel(
                        start_date=start_date,
                        end_date=end_date,
                        region=self.region_select.value,
                        resolution='30min'
                    )
                    
                    if not data.empty:
                        # Convert to daily immediately
                        data['settlementdate'] = pd.to_datetime(data['settlementdate'])
                        data['date'] = data['settlementdate'].dt.date
                        
                        # Daily aggregation by fuel type
                        daily = data.groupby(['date', 'fuel_type'])['total_generation_mw'].mean().reset_index()
                        daily.columns = ['settlementdate', 'fuel_type', 'total_generation_mw']
                        
                        all_data.append(daily)
                
                if not all_data:
                    return pd.DataFrame()
                
                return pd.concat(all_data, ignore_index=True)
            
            def _create_vre_production_chart(self):
                """Optimized chart creation with daily data"""
                # Use parent implementation but with daily data
                return super()._create_vre_production_chart()
        
        return OptimizedPenetrationTab()
    
    def compare_implementations(self):
        """Compare original vs optimized implementations"""
        logger.info("\n=== COMPARING IMPLEMENTATIONS ===")
        
        # Test original
        start_time = time.time()
        start_mem = self.measure_memory()
        
        original_tab = PenetrationTab()
        original_layout = original_tab.create_layout()
        
        original_time = time.time() - start_time
        original_mem = self.measure_memory() - start_mem
        
        logger.info(f"Original: {original_time:.2f}s, {original_mem:.1f}MB")
        
        # Test optimized
        start_time = time.time()
        start_mem = self.measure_memory()
        
        optimized_tab = self.create_optimized_tab()
        optimized_layout = optimized_tab.create_layout()
        
        optimized_time = time.time() - start_time
        optimized_mem = self.measure_memory() - start_mem
        
        logger.info(f"Optimized: {optimized_time:.2f}s, {optimized_mem:.1f}MB")
        
        if original_time > 0:
            speedup = original_time / optimized_time
            logger.info(f"Speedup: {speedup:.1f}x")
    
    def run_all_tests(self):
        """Run all performance tests"""
        # Profile current implementation
        profile_results = self.profile_current_implementation()
        
        # Test daily aggregation
        daily_data, daily_time = self.test_daily_aggregation()
        
        # Test LOESS alternatives
        if daily_data is not None:
            loess_results = self.test_loess_alternatives(daily_data)
        
        # Compare implementations
        self.compare_implementations()
        
        # Print summary
        print("\n=== PERFORMANCE SUMMARY ===")
        print("\nCurrent Implementation Scaling:")
        for result in profile_results:
            print(f"  {result['range']}: {result['total_time']:.2f}s for {result['data_points']} points")
        
        print("\nBottlenecks:")
        print("  1. Rolling average on 30-min data (1440 window) is expensive")
        print("  2. Loading multiple years of 30-min data uses significant memory")
        print("  3. Multiple chart updates trigger redundant calculations")
        
        print("\nRecommendations:")
        print("  1. Pre-aggregate to daily data in DuckDB view (48x reduction)")
        print("  2. Use smaller rolling window on daily data (30 vs 1440)")
        print("  3. Cache calculated results between chart updates")
        print("  4. Consider lazy loading - only load when tab is activated")
        print("  5. LOESS with frac=0.05-0.10 provides good smoothing")


if __name__ == "__main__":
    # Set up environment
    os.environ['USE_DUCKDB'] = 'true'
    
    tester = ImprovedPerformanceTester()
    tester.run_all_tests()