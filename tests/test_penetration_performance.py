#!/usr/bin/env python3
"""
Test script to analyze Penetration tab performance and compare different smoothing approaches.
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

class PerformanceTester:
    def __init__(self):
        self.query_manager = GenerationQueryManager()
        self.config = Config()
        
    def measure_memory(self):
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def test_current_implementation(self, years):
        """Test current 30-minute data implementation"""
        logger.info("Testing current implementation with 30-minute data...")
        start_mem = self.measure_memory()
        start_time = time.time()
        
        # Simulate what PenetrationTab does
        all_data = []
        for year in years:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31, 23, 59, 59)
            
            # Query 30-minute data
            data = self.query_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region='NEM',
                resolution='30min'
            )
            
            if not data.empty:
                all_data.append(data)
        
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            
            # Filter for VRE
            df_vre = df[df['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
            
            # Apply 30-day rolling average (1440 periods for 30-min data)
            df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
            df_vre = df_vre.sort_values('settlementdate')
            
            # Sum by timestamp
            hourly_sum = df_vre.groupby('settlementdate')['total_generation_mw'].sum().reset_index()
            
            # Apply rolling average
            hourly_sum['mw_rolling_30d'] = hourly_sum['total_generation_mw'].rolling(
                window=1440, center=False, min_periods=720
            ).mean()
        
        end_time = time.time()
        end_mem = self.measure_memory()
        
        return {
            'method': 'Current (30-min)',
            'load_time': end_time - start_time,
            'memory_used': end_mem - start_mem,
            'data_points': len(hourly_sum) if 'hourly_sum' in locals() else 0,
            'data': hourly_sum if 'hourly_sum' in locals() else None
        }
    
    def test_daily_resampling(self, years):
        """Test with daily resampling approach"""
        logger.info("Testing daily resampling approach...")
        start_mem = self.measure_memory()
        start_time = time.time()
        
        all_data = []
        for year in years:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31, 23, 59, 59)
            
            # Query with daily resolution
            data = self.query_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region='NEM',
                resolution='daily'  # Force daily aggregation
            )
            
            if not data.empty:
                all_data.append(data)
        
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
            
            # Filter for VRE
            df_vre = df[df['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
            
            # Sum by date
            df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
            daily_sum = df_vre.groupby('settlementdate')['total_generation_mw'].sum().reset_index()
            
            # Apply 30-day rolling average (30 periods for daily data)
            daily_sum['mw_rolling_30d'] = daily_sum['total_generation_mw'].rolling(
                window=30, center=False, min_periods=15
            ).mean()
        
        end_time = time.time()
        end_mem = self.measure_memory()
        
        return {
            'method': 'Daily Resample',
            'load_time': end_time - start_time,
            'memory_used': end_mem - start_mem,
            'data_points': len(daily_sum) if 'daily_sum' in locals() else 0,
            'data': daily_sum if 'daily_sum' in locals() else None
        }
    
    def test_loess_smoothing(self, data, frac=0.05):
        """Test LOESS smoothing on data"""
        logger.info(f"Testing LOESS smoothing with frac={frac}...")
        
        if data is None or data.empty:
            return None
            
        start_time = time.time()
        
        # Convert settlementdate to numeric for LOESS
        data = data.copy()
        data['date_numeric'] = (data['settlementdate'] - data['settlementdate'].min()).dt.total_seconds()
        
        # Apply LOESS
        smoothed = lowess(
            data['total_generation_mw'], 
            data['date_numeric'],
            frac=frac,
            return_sorted=False
        )
        
        data['mw_loess'] = smoothed
        
        end_time = time.time()
        
        return {
            'method': f'LOESS (frac={frac})',
            'smooth_time': end_time - start_time,
            'data': data
        }
    
    def test_savgol_smoothing(self, data, window_length=61, polyorder=3):
        """Test Savitzky-Golay filter"""
        logger.info(f"Testing Savitzky-Golay filter with window={window_length}, order={polyorder}...")
        
        if data is None or data.empty:
            return None
            
        start_time = time.time()
        
        data = data.copy()
        
        # Ensure window length is odd
        if window_length % 2 == 0:
            window_length += 1
            
        # Apply Savitzky-Golay filter
        data['mw_savgol'] = savgol_filter(
            data['total_generation_mw'],
            window_length=window_length,
            polyorder=polyorder
        )
        
        end_time = time.time()
        
        return {
            'method': f'Savitzky-Golay (w={window_length}, o={polyorder})',
            'smooth_time': end_time - start_time,
            'data': data
        }
    
    def plot_comparison(self, results):
        """Plot comparison of different smoothing methods"""
        plt.figure(figsize=(15, 10))
        
        # Plot raw data
        if results['current']['data'] is not None:
            data = results['current']['data']
            plt.subplot(2, 2, 1)
            plt.plot(data['settlementdate'], data['total_generation_mw'], 'k-', alpha=0.3, label='Raw 30-min')
            plt.plot(data['settlementdate'], data['mw_rolling_30d'], 'b-', label='30-day MA')
            plt.title('Current Implementation (30-min data)')
            plt.ylabel('MW')
            plt.legend()
            plt.xticks(rotation=45)
        
        # Plot daily resampled
        if results['daily']['data'] is not None:
            data = results['daily']['data']
            plt.subplot(2, 2, 2)
            plt.plot(data['settlementdate'], data['total_generation_mw'], 'k-', alpha=0.3, label='Daily avg')
            plt.plot(data['settlementdate'], data['mw_rolling_30d'], 'g-', label='30-day MA')
            plt.title('Daily Resampling')
            plt.ylabel('MW')
            plt.legend()
            plt.xticks(rotation=45)
        
        # Plot LOESS
        if 'loess' in results and results['loess']['data'] is not None:
            data = results['loess']['data']
            plt.subplot(2, 2, 3)
            plt.plot(data['settlementdate'], data['total_generation_mw'], 'k-', alpha=0.3, label='Daily avg')
            plt.plot(data['settlementdate'], data['mw_loess'], 'r-', label='LOESS')
            plt.title('LOESS Smoothing')
            plt.ylabel('MW')
            plt.legend()
            plt.xticks(rotation=45)
        
        # Plot Savitzky-Golay
        if 'savgol' in results and results['savgol']['data'] is not None:
            data = results['savgol']['data']
            plt.subplot(2, 2, 4)
            plt.plot(data['settlementdate'], data['total_generation_mw'], 'k-', alpha=0.3, label='Daily avg')
            plt.plot(data['settlementdate'], data['mw_savgol'], 'm-', label='Savitzky-Golay')
            plt.title('Savitzky-Golay Filter')
            plt.ylabel('MW')
            plt.legend()
            plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('penetration_smoothing_comparison.png', dpi=150)
        logger.info("Saved comparison plot to penetration_smoothing_comparison.png")
    
    def run_tests(self):
        """Run all performance tests"""
        # Test with recent 2 years of data
        years = [2023, 2024, 2025]
        
        results = {}
        
        # Test current implementation
        results['current'] = self.test_current_implementation(years)
        logger.info(f"Current implementation: {results['current']['load_time']:.2f}s, "
                   f"{results['current']['memory_used']:.1f}MB, "
                   f"{results['current']['data_points']} points")
        
        # Test daily resampling
        results['daily'] = self.test_daily_resampling(years)
        logger.info(f"Daily resampling: {results['daily']['load_time']:.2f}s, "
                   f"{results['daily']['memory_used']:.1f}MB, "
                   f"{results['daily']['data_points']} points")
        
        # Test alternative smoothing methods on daily data
        if results['daily']['data'] is not None:
            # LOESS
            results['loess'] = self.test_loess_smoothing(results['daily']['data'], frac=0.05)
            if results['loess']:
                logger.info(f"LOESS smoothing: {results['loess']['smooth_time']:.2f}s")
            
            # Savitzky-Golay
            results['savgol'] = self.test_savgol_smoothing(results['daily']['data'], window_length=31, polyorder=3)
            if results['savgol']:
                logger.info(f"Savitzky-Golay smoothing: {results['savgol']['smooth_time']:.2f}s")
        
        # Performance comparison
        print("\n=== PERFORMANCE COMPARISON ===")
        print(f"Current (30-min): {results['current']['load_time']:.2f}s load, "
              f"{results['current']['memory_used']:.1f}MB memory, "
              f"{results['current']['data_points']} points")
        print(f"Daily resample:   {results['daily']['load_time']:.2f}s load, "
              f"{results['daily']['memory_used']:.1f}MB memory, "
              f"{results['daily']['data_points']} points")
        
        if results['daily']['data_points'] > 0 and results['current']['data_points'] > 0:
            speedup = results['current']['load_time'] / results['daily']['load_time']
            reduction = results['current']['data_points'] / results['daily']['data_points']
            print(f"\nDaily resampling is {speedup:.1f}x faster with {reduction:.0f}x less data")
        
        # Plot comparison
        self.plot_comparison(results)
        
        return results
    
    def test_full_tab_load(self):
        """Test full penetration tab load time"""
        logger.info("\n=== Testing Full Tab Load ===")
        
        start_mem = self.measure_memory()
        start_time = time.time()
        
        # Create the tab (this triggers initial load)
        tab = PenetrationTab()
        
        mid_time = time.time()
        mid_mem = self.measure_memory()
        
        # Create the layout (may trigger additional loads)
        layout = tab.create_layout()
        
        end_time = time.time()
        end_mem = self.measure_memory()
        
        logger.info(f"Tab initialization: {mid_time - start_time:.2f}s, {mid_mem - start_mem:.1f}MB")
        logger.info(f"Layout creation: {end_time - mid_time:.2f}s, {end_mem - mid_mem:.1f}MB")
        logger.info(f"Total: {end_time - start_time:.2f}s, {end_mem - start_mem:.1f}MB")
        
        return tab


if __name__ == "__main__":
    # Set up environment
    os.environ['USE_DUCKDB'] = 'true'
    
    tester = PerformanceTester()
    
    # Run performance comparison
    results = tester.run_tests()
    
    # Test full tab load
    print("\n" + "="*50)
    tab = tester.test_full_tab_load()
    
    print("\n=== RECOMMENDATIONS ===")
    print("1. Daily resampling provides significant performance improvement")
    print("2. LOESS smoothing provides better visual quality but has computational cost")
    print("3. Consider lazy loading - only load data when tab is activated")
    print("4. Consider caching processed data between tab switches")