#!/usr/bin/env python3
"""
Analyze high price runs (>$1000) in prices30.parquet
Calculates duration statistics and patterns for each region
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import duckdb

# Data path
DATA_PATH = '/Volumes/davidleitch/aemo_production/data'

def identify_price_runs(df, threshold=1000):
    """
    Identify continuous runs where price exceeds threshold
    Returns list of runs with start, end, duration, and statistics
    """
    # Create boolean mask for high prices
    high_price_mask = df['rrp'] > threshold
    
    # Identify run starts and ends
    run_starts = high_price_mask & ~high_price_mask.shift(1, fill_value=False)
    run_ends = ~high_price_mask & high_price_mask.shift(1, fill_value=False)
    
    # Get indices of starts and ends
    start_indices = df.index[run_starts].tolist()
    end_indices = df.index[run_ends].tolist()
    
    # Handle case where run continues to end of data
    if len(start_indices) > len(end_indices):
        end_indices.append(len(df))
    
    # Build list of runs
    runs = []
    for start_idx, end_idx in zip(start_indices, end_indices):
        if end_idx > start_idx:  # Valid run
            run_data = df.iloc[start_idx:end_idx]
            
            # Calculate run statistics
            run_info = {
                'start_time': run_data.iloc[0]['settlementdate'],
                'end_time': run_data.iloc[-1]['settlementdate'],
                'duration_periods': len(run_data),
                'duration_hours': len(run_data) * 0.5,  # 30-min periods
                'max_price': run_data['rrp'].max(),
                'min_price': run_data['rrp'].min(),
                'avg_price': run_data['rrp'].mean(),
                'total_revenue': run_data['rrp'].sum() * 0.5,  # $/MWh * 0.5h
                'start_price': run_data.iloc[0]['rrp'],
                'end_price': run_data.iloc[-1]['rrp'],
                'peak_time': run_data.loc[run_data['rrp'].idxmax()]['settlementdate']
            }
            runs.append(run_info)
    
    return runs

def analyze_region_high_prices(region_name):
    """Analyze high price runs for a specific region"""
    
    print(f"\n{'='*60}")
    print(f"Analyzing high price runs for {region_name}")
    print(f"{'='*60}")
    
    # Connect to DuckDB and load data
    conn = duckdb.connect(':memory:')
    
    # Query prices for this region
    prices_path = Path(DATA_PATH) / 'prices30.parquet'
    
    # Get all data for the region
    query = f"""
    SELECT settlementdate, rrp
    FROM parquet_scan('{prices_path}')
    WHERE regionid = '{region_name}'
    ORDER BY settlementdate
    """
    
    df = conn.execute(query).fetchdf()
    conn.close()
    
    if len(df) == 0:
        print(f"No data found for region {region_name}")
        return None
    
    # Convert settlementdate to datetime
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    
    # Basic statistics
    total_periods = len(df)
    high_price_periods = len(df[df['rrp'] > 1000])
    pct_high = (high_price_periods / total_periods) * 100
    
    print(f"\nOverall Statistics:")
    print(f"- Total periods: {total_periods:,}")
    print(f"- High price periods (>$1000): {high_price_periods:,} ({pct_high:.2f}%)")
    print(f"- Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
    
    # Identify runs
    runs = identify_price_runs(df, threshold=1000)
    
    if not runs:
        print(f"\nNo high price runs found for {region_name}")
        return None
    
    print(f"\nRun Analysis:")
    print(f"- Total number of runs: {len(runs)}")
    
    # Convert to DataFrame for easier analysis
    runs_df = pd.DataFrame(runs)
    
    # Duration statistics
    print(f"\nDuration Statistics (hours):")
    print(f"- Shortest run: {runs_df['duration_hours'].min():.1f} hours")
    print(f"- Longest run: {runs_df['duration_hours'].max():.1f} hours")
    print(f"- Average duration: {runs_df['duration_hours'].mean():.1f} hours")
    print(f"- Median duration: {runs_df['duration_hours'].median():.1f} hours")
    print(f"- Std deviation: {runs_df['duration_hours'].std():.1f} hours")
    
    # Price statistics during runs
    print(f"\nPrice Statistics During Runs:")
    print(f"- Highest price reached: ${runs_df['max_price'].max():,.0f}")
    print(f"- Average peak price: ${runs_df['max_price'].mean():,.0f}")
    print(f"- Average price during runs: ${runs_df['avg_price'].mean():,.0f}")
    
    # Time patterns
    runs_df['start_hour'] = pd.to_datetime(runs_df['start_time']).dt.hour
    runs_df['start_month'] = pd.to_datetime(runs_df['start_time']).dt.month
    runs_df['start_year'] = pd.to_datetime(runs_df['start_time']).dt.year
    
    print(f"\nTemporal Patterns:")
    print(f"- Most common start hour: {runs_df['start_hour'].mode().values[0]}:00")
    print(f"- Runs by year:")
    for year, count in runs_df['start_year'].value_counts().sort_index().items():
        print(f"  - {year}: {count} runs")
    
    # Duration distribution
    print(f"\nDuration Distribution:")
    duration_bins = [0, 0.5, 1, 2, 4, 8, 16, 24, np.inf]
    duration_labels = ['<30min', '30min-1h', '1-2h', '2-4h', '4-8h', '8-16h', '16-24h', '>24h']
    runs_df['duration_category'] = pd.cut(runs_df['duration_hours'], bins=duration_bins, labels=duration_labels)
    
    for category, count in runs_df['duration_category'].value_counts().sort_index().items():
        pct = (count / len(runs_df)) * 100
        print(f"  - {category}: {count} runs ({pct:.1f}%)")
    
    # Top 10 longest runs
    print(f"\nTop 10 Longest Runs:")
    print(f"{'Start Time':<20} {'Duration':<10} {'Max Price':<12} {'Avg Price':<12}")
    print("-" * 55)
    
    for _, run in runs_df.nlargest(10, 'duration_hours').iterrows():
        start_str = pd.to_datetime(run['start_time']).strftime('%Y-%m-%d %H:%M')
        print(f"{start_str:<20} {run['duration_hours']:>8.1f}h  ${run['max_price']:>10,.0f}  ${run['avg_price']:>10,.0f}")
    
    return runs_df

def main():
    """Analyze high price runs for all NEM regions"""
    
    regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
    
    # Store results for each region
    all_results = {}
    
    for region in regions:
        runs_df = analyze_region_high_prices(region)
        if runs_df is not None:
            all_results[region] = runs_df
    
    # Summary comparison across regions
    print(f"\n{'='*60}")
    print("SUMMARY COMPARISON ACROSS REGIONS")
    print(f"{'='*60}")
    
    print(f"\n{'Region':<8} {'Runs':<8} {'Avg Duration':<15} {'Max Duration':<15} {'Highest Price':<15}")
    print("-" * 70)
    
    for region in regions:
        if region in all_results:
            runs_df = all_results[region]
            print(f"{region:<8} {len(runs_df):<8} {runs_df['duration_hours'].mean():>12.1f}h  "
                  f"{runs_df['duration_hours'].max():>12.1f}h  ${runs_df['max_price'].max():>13,.0f}")
    
    # Save detailed results to CSV
    print("\nSaving detailed results to CSV files...")
    for region, runs_df in all_results.items():
        output_file = f"high_price_runs_{region}.csv"
        runs_df.to_csv(output_file, index=False)
        print(f"- Saved {output_file}")

if __name__ == "__main__":
    main()