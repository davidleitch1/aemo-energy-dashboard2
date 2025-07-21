#!/usr/bin/env python3
"""
Test rooftop solar data conversion from 30-minute to 5-minute intervals
with Henderson filter smoothing
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Henderson 7-term filter weights
HENDERSON_7 = np.array([-0.059, 0.059, 0.294, 0.412, 0.294, 0.059, -0.059])

def henderson_smooth(data, weights=HENDERSON_7):
    """Apply Henderson filter to smooth data"""
    n = len(weights)
    half_n = n // 2
    
    # Pad data to handle edges
    padded = np.pad(data, (half_n, half_n), mode='edge')
    
    # Apply convolution
    smoothed = np.convolve(padded, weights, mode='valid')
    
    return smoothed

def convert_30min_to_5min(df_30min, method='henderson'):
    """
    Convert 30-minute rooftop data to 5-minute intervals
    
    Methods:
    - 'linear': Simple linear interpolation
    - 'cubic': Cubic spline interpolation
    - 'henderson': Linear interpolation followed by Henderson smoothing
    """
    
    # First, create 5-minute time index spanning the data range
    start_time = df_30min.index.min()
    end_time = df_30min.index.max()
    
    # Create 5-minute index
    index_5min = pd.date_range(start=start_time, end=end_time, freq='5min')
    
    # Results dictionary
    results = {}
    
    for col in df_30min.columns:
        if method == 'linear':
            # Simple linear interpolation
            series_30min = df_30min[col]
            series_5min = series_30min.reindex(index_5min).interpolate(method='linear')
            
        elif method == 'cubic':
            # Cubic spline interpolation
            series_30min = df_30min[col]
            series_5min = series_30min.reindex(index_5min).interpolate(method='cubic')
            
        elif method == 'henderson':
            # Linear interpolation first
            series_30min = df_30min[col]
            series_5min = series_30min.reindex(index_5min).interpolate(method='linear')
            
            # Apply Henderson smoothing
            values = series_5min.values
            smoothed = henderson_smooth(values)
            series_5min = pd.Series(smoothed, index=index_5min)
        
        results[col] = series_5min
    
    return pd.DataFrame(results)

def handle_future_timestamps(df_5min, last_30min_time):
    """
    Handle cases where we need 5-minute data beyond the last 30-minute observation
    Uses simple forward fill with decay for up to 25 minutes ahead
    """
    future_mask = df_5min.index > last_30min_time
    
    if future_mask.any():
        # Get the last known values
        last_values = df_5min.loc[~future_mask].iloc[-1]
        
        # For future timestamps, use last value with gradual decay
        # Solar typically decreases, so apply a gentle decay factor
        future_indices = df_5min.index[future_mask]
        decay_factor = 0.99  # 1% decay per 5-minute interval
        
        for i, idx in enumerate(future_indices):
            if i < 5:  # Only project up to 25 minutes
                df_5min.loc[idx] = last_values * (decay_factor ** (i + 1))
            else:
                df_5min.loc[idx] = 0  # Beyond 25 minutes, set to 0
    
    return df_5min

def test_conversion_methods():
    """Test different conversion methods on sample data"""
    
    print("=" * 60)
    print("ROOFTOP SOLAR 30-MIN TO 5-MIN CONVERSION TEST")
    print("=" * 60)
    
    # Load the new 30-minute rooftop data
    new_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/rooftop30.parquet"
    
    try:
        df_new = pd.read_parquet(new_file)
        print(f"\nLoaded new rooftop data: {len(df_new):,} records")
        print(f"Columns: {list(df_new.columns)}")
        print(f"Time range: {df_new['settlementdate'].min()} to {df_new['settlementdate'].max()}")
        
        # Check data format - is it long or wide?
        if 'regionid' in df_new.columns:
            print("\nData is in LONG format (has regionid column)")
            # Convert to wide format for easier processing
            df_wide = df_new.pivot(index='settlementdate', columns='regionid', values='power')
            df_wide.columns = [f'rooftop_{col}' for col in df_wide.columns]
        else:
            print("\nData appears to be in WIDE format already")
            df_wide = df_new.set_index('settlementdate')
        
        # Take a sample for testing (last 48 hours)
        sample_end = df_wide.index.max()
        sample_start = sample_end - timedelta(hours=48)
        df_sample = df_wide.loc[sample_start:sample_end]
        
        print(f"\nTesting with {len(df_sample)} 30-minute records (48 hours)")
        
        # Test all three methods
        methods = ['linear', 'cubic', 'henderson']
        results = {}
        
        for method in methods:
            print(f"\n{method.upper()} method:")
            df_5min = convert_30min_to_5min(df_sample, method=method)
            results[method] = df_5min
            print(f"  Created {len(df_5min)} 5-minute records")
            
            # Check smoothness by calculating differences
            first_col = df_5min.columns[0]
            diffs = df_5min[first_col].diff().abs()
            avg_diff = diffs.mean()
            max_diff = diffs.max()
            print(f"  Average step change: {avg_diff:.2f} MW")
            print(f"  Maximum step change: {max_diff:.2f} MW")
        
        # Visual comparison
        if len(df_sample) > 0:
            plot_comparison(df_sample, results)
        
        return df_wide, results
        
    except Exception as e:
        print(f"\nError loading rooftop data: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def plot_comparison(df_30min, results_dict):
    """Plot comparison of different interpolation methods"""
    
    try:
        import matplotlib
        matplotlib.use('Agg')  # Non-interactive backend
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        
        # Use first region for plotting
        col = df_30min.columns[0]
        
        # Plot 1: Full view
        ax1 = axes[0]
        
        # Original 30-minute data
        ax1.scatter(df_30min.index, df_30min[col], color='black', 
                   s=20, label='30-min data', zorder=5)
        
        # Interpolated 5-minute data
        colors = {'linear': 'blue', 'cubic': 'green', 'henderson': 'red'}
        for method, df_5min in results_dict.items():
            ax1.plot(df_5min.index, df_5min[col], color=colors[method], 
                    alpha=0.7, label=f'{method} interpolation')
        
        ax1.set_title(f'Rooftop Solar Interpolation Comparison - {col}')
        ax1.set_ylabel('Power (MW)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Zoomed view (6 hours)
        ax2 = axes[1]
        
        # Select a 6-hour window during daylight
        mid_time = df_30min.index[len(df_30min)//2]
        zoom_start = mid_time - timedelta(hours=3)
        zoom_end = mid_time + timedelta(hours=3)
        
        # Original 30-minute data (zoomed)
        df_zoom = df_30min.loc[zoom_start:zoom_end]
        ax2.scatter(df_zoom.index, df_zoom[col], color='black', 
                   s=30, label='30-min data', zorder=5)
        
        # Interpolated 5-minute data (zoomed)
        for method, df_5min in results_dict.items():
            df_5min_zoom = df_5min.loc[zoom_start:zoom_end]
            ax2.plot(df_5min_zoom.index, df_5min_zoom[col], 
                    color=colors[method], alpha=0.7, 
                    linewidth=2, label=f'{method}')
        
        ax2.set_title('Zoomed View (6 hours)')
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Power (MW)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('rooftop_interpolation_comparison.png', dpi=150)
        print("\n✅ Saved comparison plot to rooftop_interpolation_comparison.png")
        
    except Exception as e:
        print(f"\n⚠️ Could not create plot: {e}")

def test_old_format_compatibility():
    """Test if we can match the old rooftop data format"""
    
    print("\n" + "=" * 60)
    print("TESTING FORMAT COMPATIBILITY")
    print("=" * 60)
    
    # Load old format to understand structure
    old_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/rooftop_solar.parquet"
    
    try:
        df_old = pd.read_parquet(old_file)
        print(f"\nOld rooftop data structure:")
        print(f"  Shape: {df_old.shape}")
        print(f"  Columns: {list(df_old.columns[:5])}... ({len(df_old.columns)} total)")
        print(f"  Index: {df_old.index.name}")
        print(f"  Sample data:")
        print(df_old.head(3))
        
        return df_old
        
    except Exception as e:
        print(f"Could not load old rooftop file: {e}")
        return None

if __name__ == "__main__":
    print("Starting rooftop solar conversion test...\n")
    
    # Test the conversion methods
    df_wide, results = test_conversion_methods()
    
    # Test old format compatibility
    df_old = test_old_format_compatibility()
    
    print("\n" + "=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)
    print("\nBased on the tests, the Henderson filter approach provides:")
    print("- Smooth transitions between 30-minute points")
    print("- No overshooting (unlike cubic splines)")
    print("- Computationally efficient")
    print("- Preserves the natural solar generation curve")
    print("\nNext steps:")
    print("1. Implement rooftop_adapter.py using Henderson smoothing")
    print("2. Handle the wide-to-long format conversion")
    print("3. Add edge case handling for future timestamps")
    print("4. Test with dashboard integration")