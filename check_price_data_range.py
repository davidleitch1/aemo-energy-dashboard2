#!/usr/bin/env python3
"""
Check the date range available in price parquet files.
"""

import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

def check_price_data_range():
    """Check and display date ranges in price parquet files."""
    
    # Load environment variables
    load_dotenv()
    
    # Get file paths from environment
    prices5_path = os.getenv('SPOT_HIST_FILE')
    
    # For prices30, we need to construct the path based on the pattern
    if prices5_path:
        base_dir = Path(prices5_path).parent
        prices30_path = base_dir / 'prices30.parquet'
    else:
        print("ERROR: SPOT_HIST_FILE not found in environment variables")
        return
    
    print("=" * 80)
    print("Price Data Range Analysis")
    print("=" * 80)
    
    # Check prices5.parquet
    if prices5_path and Path(prices5_path).exists():
        print(f"\nAnalyzing: {prices5_path}")
        try:
            df5 = pd.read_parquet(prices5_path)
            
            # Get basic info
            print(f"Total records: {len(df5):,}")
            print(f"Columns: {list(df5.columns)}")
            
            # Get date range
            if 'SETTLEMENTDATE' in df5.columns:
                date_col = 'SETTLEMENTDATE'
            elif 'settlementdate' in df5.columns:
                date_col = 'settlementdate'
            else:
                print("WARNING: No settlement date column found")
                date_col = None
            
            if date_col:
                df5[date_col] = pd.to_datetime(df5[date_col])
                min_date = df5[date_col].min()
                max_date = df5[date_col].max()
                print(f"Date range: {min_date} to {max_date}")
                print(f"Duration: {(max_date - min_date).days} days")
                
                # Check regions if available
                if 'REGIONID' in df5.columns:
                    regions = df5['REGIONID'].unique()
                    print(f"Regions: {sorted(regions)}")
                elif 'regionid' in df5.columns:
                    regions = df5['regionid'].unique()
                    print(f"Regions: {sorted(regions)}")
                
                # Sample recent data
                print("\nMost recent 5 records:")
                print(df5.nlargest(5, date_col)[[date_col] + [col for col in df5.columns if col != date_col][:3]])
                
        except Exception as e:
            print(f"ERROR reading {prices5_path}: {e}")
    else:
        print(f"\nFile not found: {prices5_path}")
    
    # Check prices30.parquet
    if prices30_path and Path(prices30_path).exists():
        print(f"\n{'-' * 80}")
        print(f"\nAnalyzing: {prices30_path}")
        try:
            df30 = pd.read_parquet(prices30_path)
            
            # Get basic info
            print(f"Total records: {len(df30):,}")
            print(f"Columns: {list(df30.columns)}")
            
            # Get date range
            if 'SETTLEMENTDATE' in df30.columns:
                date_col = 'SETTLEMENTDATE'
            elif 'settlementdate' in df30.columns:
                date_col = 'settlementdate'
            else:
                print("WARNING: No settlement date column found")
                date_col = None
            
            if date_col:
                df30[date_col] = pd.to_datetime(df30[date_col])
                min_date = df30[date_col].min()
                max_date = df30[date_col].max()
                print(f"Date range: {min_date} to {max_date}")
                print(f"Duration: {(max_date - min_date).days} days")
                
                # Check regions if available
                if 'REGIONID' in df30.columns:
                    regions = df30['REGIONID'].unique()
                    print(f"Regions: {sorted(regions)}")
                elif 'regionid' in df30.columns:
                    regions = df30['regionid'].unique()
                    print(f"Regions: {sorted(regions)}")
                
                # Sample recent data
                print("\nMost recent 5 records:")
                print(df30.nlargest(5, date_col)[[date_col] + [col for col in df30.columns if col != date_col][:3]])
                
        except Exception as e:
            print(f"ERROR reading {prices30_path}: {e}")
    else:
        print(f"\nFile not found: {prices30_path}")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    check_price_data_range()