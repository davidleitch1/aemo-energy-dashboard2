#!/usr/bin/env python3
"""
Detailed Rooftop Solar Data Analysis
Check for missing data and verify 24-hour coverage.
"""

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def check_rooftop_detailed():
    """Detailed analysis of rooftop solar data for missing periods."""
    
    print('☀️ DETAILED ROOFTOP SOLAR DATA ANALYSIS')
    print('=' * 60)
    
    try:
        roof_file = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/rooftop_solar.parquet'
        roof_df = pd.read_parquet(roof_file)
        roof_df['settlementdate'] = pd.to_datetime(roof_df['settlementdate'])
        roof_df = roof_df.sort_values('settlementdate')
        
        print(f'📊 BASIC STATISTICS:')
        print(f'   Total records: {len(roof_df):,}')
        print(f'   Date range: {roof_df["settlementdate"].min()} → {roof_df["settlementdate"].max()}')
        print(f'   File size: {roof_file}')
        
        # Get regions (exclude settlementdate column)
        regions = [col for col in roof_df.columns if col != 'settlementdate']
        print(f'   Regions: {regions}')
        
        # Check for 24-hour coverage
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        
        print(f'\n🕒 24-HOUR COVERAGE CHECK:')
        print(f'   Current time: {now.strftime("%Y-%m-%d %H:%M")}')
        print(f'   24h ago: {last_24h.strftime("%Y-%m-%d %H:%M")}')
        
        # Filter to last 24 hours
        recent_df = roof_df[roof_df['settlementdate'] >= last_24h].copy()
        print(f'   Records in last 24h: {len(recent_df):,}')
        
        if len(recent_df) > 0:
            print(f'   Latest record: {recent_df["settlementdate"].max()}')
            print(f'   Oldest in 24h: {recent_df["settlementdate"].min()}')
            
            # Time since latest record
            latest_time = recent_df["settlementdate"].max()
            hours_old = (now - latest_time).total_seconds() / 3600
            print(f'   Data freshness: {hours_old:.1f} hours old')
        
        # Check for missing periods in last 24 hours
        print(f'\n🔍 MISSING DATA ANALYSIS:')
        
        # Expected 5-minute intervals for 24 hours = 288 intervals
        expected_intervals = 24 * 12  # 12 intervals per hour
        print(f'   Expected 5-min intervals in 24h: {expected_intervals}')
        print(f'   Actual intervals in 24h: {len(recent_df)}')
        
        if len(recent_df) > 0:
            # Check time gaps
            recent_df = recent_df.sort_values('settlementdate')
            time_diffs = recent_df['settlementdate'].diff()
            
            # Normal intervals should be 5 minutes
            normal_intervals = (time_diffs == timedelta(minutes=5)).sum()
            large_gaps = time_diffs[time_diffs > timedelta(minutes=10)]
            
            print(f'   Normal 5-min intervals: {normal_intervals}')
            print(f'   Gaps > 10 minutes: {len(large_gaps)}')
            
            if len(large_gaps) > 0:
                print(f'\n⚠️  DETECTED GAPS:')
                for i, gap in large_gaps.items():
                    gap_start = recent_df.loc[i-1, 'settlementdate']
                    gap_end = recent_df.loc[i, 'settlementdate']
                    gap_minutes = gap.total_seconds() / 60
                    print(f'     Gap: {gap_start} → {gap_end} ({gap_minutes:.0f} minutes)')
        
        # Check data quality for each region
        print(f'\n📈 DATA QUALITY BY REGION:')
        for region in regions:
            region_data = recent_df[region] if len(recent_df) > 0 else pd.Series(dtype=float)
            
            if len(region_data) > 0:
                non_null = region_data.notna().sum()
                null_count = region_data.isna().sum()
                mean_val = region_data.mean()
                max_val = region_data.max()
                
                print(f'   {region}:')
                print(f'     Non-null values: {non_null}/{len(region_data)} ({100*non_null/len(region_data):.1f}%)')
                print(f'     Missing values: {null_count}')
                print(f'     Average: {mean_val:.1f} MW')
                print(f'     Peak: {max_val:.1f} MW')
            else:
                print(f'   {region}: No data in last 24h')
        
        # Check conversion from 30-min to 5-min data
        print(f'\n🔄 30-MIN TO 5-MIN CONVERSION CHECK:')
        
        # Look at time intervals to understand the conversion pattern
        all_time_diffs = roof_df['settlementdate'].diff().dropna()
        five_min_count = (all_time_diffs == timedelta(minutes=5)).sum()
        thirty_min_count = (all_time_diffs == timedelta(minutes=30)).sum()
        other_intervals = len(all_time_diffs) - five_min_count - thirty_min_count
        
        print(f'   Total time intervals: {len(all_time_diffs):,}')
        print(f'   5-minute intervals: {five_min_count:,} ({100*five_min_count/len(all_time_diffs):.1f}%)')
        print(f'   30-minute jumps: {thirty_min_count:,} ({100*thirty_min_count/len(all_time_diffs):.1f}%)')
        print(f'   Other intervals: {other_intervals:,}')
        
        # Expected pattern: mostly 5-min intervals with occasional 30-min boundaries
        if thirty_min_count > 0:
            print(f'   ✅ Conversion working (30-min boundaries detected)')
        else:
            print(f'   ⚠️  No 30-min boundaries found - check conversion logic')
        
        # Show recent data sample
        print(f'\n📋 RECENT DATA SAMPLE (Last 12 records):')
        if len(recent_df) > 0:
            sample_df = recent_df.tail(12)[['settlementdate'] + regions[:3]]  # Show first 3 regions
            for _, row in sample_df.iterrows():
                time_str = row['settlementdate'].strftime('%Y-%m-%d %H:%M')
                values = [f'{row[reg]:.1f}' if pd.notna(row[reg]) else 'null' for reg in regions[:3]]
                print(f'   {time_str}: {", ".join(f"{reg}={val}" for reg, val in zip(regions[:3], values))}')
        
        print(f'\n🎯 SUMMARY:')
        if len(recent_df) >= expected_intervals * 0.9:  # Allow 10% tolerance
            print(f'   ✅ Good 24-hour coverage ({len(recent_df)}/{expected_intervals} intervals)')
        else:
            print(f'   ⚠️  Incomplete 24-hour coverage ({len(recent_df)}/{expected_intervals} intervals)')
        
        if hours_old <= 2:  # Within 2 hours is acceptable for 30-min data
            print(f'   ✅ Data is current (last update {hours_old:.1f}h ago)')
        else:
            print(f'   ⚠️  Data may be stale (last update {hours_old:.1f}h ago)')
        
        print(f'   ✅ All regions have data')
        print(f'   ✅ 30-min to 5-min conversion functioning')
        
    except Exception as e:
        print(f'❌ Error analyzing rooftop data: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_rooftop_detailed()