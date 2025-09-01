#!/usr/bin/env python3
"""
Verify SCADA aggregation consistency between 5-min and 30-min data
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))
from src.data_service.shared_data_duckdb import duckdb_data_service

def verify_aggregation_consistency():
    """Check if 30-min SCADA data correctly aggregates from 5-min data"""
    
    print("="*80)
    print("SCADA AGGREGATION VERIFICATION")
    print("="*80)
    
    # Test with multiple batteries known to charge/discharge
    test_duids = ['HPR1', 'DALNTHL1', 'BALBG1', 'WGWF1']
    
    # Use a recent date range
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=7)
    
    print(f"Test Period: {start_date} to {end_date}")
    print(f"Test DUIDs: {', '.join(test_duids)}")
    print()
    
    all_issues = []
    
    for duid in test_duids:
        print(f"\n{'='*40}")
        print(f"Testing {duid}")
        print('='*40)
        
        # Get 5-minute data
        query_5min = f"""
            SELECT 
                settlementdate,
                scadavalue
            FROM generation_5min
            WHERE duid = '{duid}'
              AND settlementdate >= '{start_date.isoformat()}'
              AND settlementdate < '{end_date.isoformat()}'
            ORDER BY settlementdate
        """
        
        df_5min = duckdb_data_service.conn.execute(query_5min).df()
        
        if df_5min.empty:
            print(f"  No 5-minute data found")
            continue
            
        df_5min['settlementdate'] = pd.to_datetime(df_5min['settlementdate'])
        
        # Get 30-minute data  
        query_30min = f"""
            SELECT 
                settlementdate,
                scadavalue
            FROM generation_30min
            WHERE duid = '{duid}'
              AND settlementdate >= '{start_date.isoformat()}'
              AND settlementdate < '{end_date.isoformat()}'
            ORDER BY settlementdate
        """
        
        df_30min = duckdb_data_service.conn.execute(query_30min).df()
        
        if df_30min.empty:
            print(f"  No 30-minute data found")
            continue
            
        df_30min['settlementdate'] = pd.to_datetime(df_30min['settlementdate'])
        
        # Aggregate 5-min to 30-min for comparison
        df_5min['endpoint'] = df_5min['settlementdate'].dt.floor('30min') + pd.Timedelta(minutes=30)
        df_5min_agg = df_5min.groupby('endpoint')['scadavalue'].mean().reset_index()
        df_5min_agg.columns = ['settlementdate', 'scadavalue_5min_agg']
        
        # Merge for comparison
        comparison = pd.merge(
            df_30min,
            df_5min_agg,
            on='settlementdate',
            how='outer'
        )
        
        # Rename columns for clarity
        comparison = comparison.rename(columns={'scadavalue': 'scadavalue_30min'})
        
        # Calculate differences
        comparison['difference'] = comparison['scadavalue_30min'] - comparison['scadavalue_5min_agg']
        comparison['pct_diff'] = (comparison['difference'] / comparison['scadavalue_5min_agg'].abs() * 100).fillna(0)
        
        # Find significant differences (> 1% or > 1 MW)
        significant_diffs = comparison[
            (comparison['pct_diff'].abs() > 1) | 
            (comparison['difference'].abs() > 1)
        ]
        
        # Statistics
        print(f"  5-min records: {len(df_5min)}")
        print(f"  30-min records: {len(df_30min)}")
        print(f"  5-min aggregated periods: {len(df_5min_agg)}")
        
        # Check for negative values (charging)
        neg_5min = df_5min[df_5min['scadavalue'] < 0]
        neg_30min = df_30min[df_30min['scadavalue'] < 0]
        neg_5min_agg = df_5min_agg[df_5min_agg['scadavalue_5min_agg'] < 0]
        
        print(f"\n  Charging periods:")
        print(f"    5-min raw: {len(neg_5min)} periods")
        print(f"    5-min aggregated: {len(neg_5min_agg)} periods")  
        print(f"    30-min native: {len(neg_30min)} periods")
        
        if len(neg_5min_agg) != len(neg_30min):
            issue = f"MISMATCH: {duid} has {len(neg_5min_agg)} charging periods in 5-min agg but {len(neg_30min)} in 30-min native"
            print(f"    ⚠️ {issue}")
            all_issues.append(issue)
        
        # Check totals
        total_5min = df_5min['scadavalue'].sum() / 12  # Convert to MWh
        total_5min_agg = df_5min_agg['scadavalue_5min_agg'].sum() / 2  # Convert to MWh
        total_30min = df_30min['scadavalue'].sum() / 2  # Convert to MWh
        
        print(f"\n  Total MWh:")
        print(f"    5-min raw: {total_5min:.1f} MWh")
        print(f"    5-min aggregated: {total_5min_agg:.1f} MWh")
        print(f"    30-min native: {total_30min:.1f} MWh")
        print(f"    Difference: {total_30min - total_5min_agg:.1f} MWh ({(total_30min - total_5min_agg)/abs(total_5min_agg)*100:.1f}%)")
        
        if abs(total_30min - total_5min_agg) > 10:  # More than 10 MWh difference
            issue = f"ENERGY MISMATCH: {duid} has {abs(total_30min - total_5min_agg):.1f} MWh difference"
            print(f"    ⚠️ {issue}")
            all_issues.append(issue)
        
        # Show sample differences
        if not significant_diffs.empty:
            print(f"\n  Significant differences found: {len(significant_diffs)} periods")
            print(f"  Sample differences (first 5):")
            for idx, row in significant_diffs.head().iterrows():
                print(f"    {row['settlementdate']}: 30min={row['scadavalue_30min']:.1f} MW, 5min_agg={row['scadavalue_5min_agg']:.1f} MW, diff={row['difference']:.1f} MW")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    if all_issues:
        print("Issues found:")
        for issue in all_issues:
            print(f"  - {issue}")
    else:
        print("✅ No significant issues found - aggregation appears consistent")
    
    return len(all_issues) == 0

if __name__ == "__main__":
    verify_aggregation_consistency()