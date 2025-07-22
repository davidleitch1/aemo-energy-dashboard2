#!/usr/bin/env python3
"""
Proof of Concept: Test reading price data from new structure
Compare old spot_hist.parquet with new prices5.parquet
Implement column mapping adapter
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

def compare_price_data():
    """Compare old and new price data files"""
    
    # File paths
    old_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-spot-dashboard/spot_hist.parquet")
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices5.parquet")
    
    print("=" * 60)
    print("PRICE DATA COMPARISON")
    print("=" * 60)
    
    # Read old data
    try:
        old_df = pd.read_parquet(old_file)
        print(f"\n✅ Old file loaded: {old_file.name}")
        print(f"   Shape: {old_df.shape}")
        print(f"   Columns: {list(old_df.columns)}")
        print(f"   Index type: {type(old_df.index)}")
        if hasattr(old_df.index, 'name'):
            print(f"   Index name: {old_df.index.name}")
        print(f"   First few rows:")
        print(old_df.head(3))
        
        # Check if datetime is in index
        if isinstance(old_df.index, pd.DatetimeIndex):
            print(f"   Date range (index): {old_df.index.min()} to {old_df.index.max()}")
        
        print(f"   Regions: {sorted(old_df['REGIONID'].unique())}")
    except Exception as e:
        print(f"\n❌ Error loading old file: {e}")
        old_df = None
    
    # Read new data
    try:
        new_df = pd.read_parquet(new_file)
        print(f"\n✅ New file loaded: {new_file.name}")
        print(f"   Shape: {new_df.shape}")
        print(f"   Columns: {list(new_df.columns)}")
        print(f"   Date range: {new_df['settlementdate'].min()} to {new_df['settlementdate'].max()}")
        print(f"   Regions: {sorted(new_df['regionid'].unique())}")
        print(f"   First few rows:")
        print(new_df.head(3))
    except Exception as e:
        print(f"\n❌ Error loading new file: {e}")
        return False
    
    # Analyze structure differences
    print("\n" + "=" * 60)
    print("STRUCTURE ANALYSIS")
    print("=" * 60)
    
    if old_df is not None:
        print("\nOld structure:")
        print(f"  - Datetime in index: {isinstance(old_df.index, pd.DatetimeIndex)}")
        print(f"  - Columns: {list(old_df.columns)}")
        print(f"  - Data types: {dict(old_df.dtypes)}")
        
        print("\nNew structure:")
        print(f"  - Datetime as column 'settlementdate'")
        print(f"  - Columns: {list(new_df.columns)}")
        print(f"  - Data types: {dict(new_df.dtypes)}")
        
        print("\nMapping required:")
        print("  - 'regionid' → 'REGIONID' (uppercase)")
        print("  - 'rrp' → 'RRP' (uppercase)")
        print("  - 'settlementdate' column → datetime index")
    
    return True, old_df, new_df

def create_price_adapter(df):
    """
    Adapter function to convert new price format to old format
    This allows the dashboard to work without code changes
    """
    print("\n" + "=" * 60)
    print("CREATING PRICE DATA ADAPTER")
    print("=" * 60)
    
    # Create a copy to avoid modifying original
    adapted_df = df.copy()
    
    # Step 1: Rename columns to match old format
    adapted_df = adapted_df.rename(columns={
        'regionid': 'REGIONID',
        'rrp': 'RRP'
    })
    print("✅ Renamed columns: regionid→REGIONID, rrp→RRP")
    
    # Step 2: Set datetime as index
    adapted_df = adapted_df.set_index('settlementdate')
    print("✅ Set settlementdate as index")
    
    # Step 3: Sort by index (datetime)
    adapted_df = adapted_df.sort_index()
    print("✅ Sorted by datetime index")
    
    print(f"\nAdapted structure:")
    print(f"  Shape: {adapted_df.shape}")
    print(f"  Columns: {list(adapted_df.columns)}")
    print(f"  Index type: {type(adapted_df.index)}")
    print(f"  Sample:")
    print(adapted_df.head(3))
    
    return adapted_df

def test_adapter_compatibility(old_df, adapted_df):
    """Test if adapted data is compatible with old data structure"""
    
    print("\n" + "=" * 60)
    print("TESTING ADAPTER COMPATIBILITY")
    print("=" * 60)
    
    if old_df is None:
        print("⚠️  No old data to compare with")
        return True
    
    # Check column names
    old_cols = set(old_df.columns)
    new_cols = set(adapted_df.columns)
    
    if old_cols == new_cols:
        print("✅ Column names match exactly")
    else:
        print("❌ Column name mismatch:")
        print(f"   Old: {old_cols}")
        print(f"   New: {new_cols}")
        return False
    
    # Check index type
    if type(old_df.index) == type(adapted_df.index):
        print("✅ Index types match")
    else:
        print("❌ Index type mismatch")
        return False
    
    # Check data types
    for col in old_df.columns:
        if col in adapted_df.columns:
            if old_df[col].dtype == adapted_df[col].dtype:
                print(f"✅ '{col}' dtype matches: {old_df[col].dtype}")
            else:
                print(f"⚠️  '{col}' dtype differs: {old_df[col].dtype} vs {adapted_df[col].dtype}")
    
    return True

def test_dashboard_operations_with_adapter():
    """Test typical dashboard operations with adapted price data"""
    
    print("\n" + "=" * 60)
    print("TESTING DASHBOARD OPERATIONS")
    print("=" * 60)
    
    # Load and adapt new data
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices5.parquet")
    new_df = pd.read_parquet(new_file)
    adapted_df = create_price_adapter(new_df)
    
    try:
        # Test 1: Region filtering (common operation)
        nsw_prices = adapted_df[adapted_df['REGIONID'] == 'NSW1']
        print(f"✅ Region filtering works: {len(nsw_prices)} NSW1 records")
        
        # Test 2: Time range filtering using index
        end_time = adapted_df.index.max()
        start_time = end_time - pd.Timedelta(days=1)
        recent_prices = adapted_df.loc[start_time:end_time]
        print(f"✅ Time filtering works: {len(recent_prices)} records in last 24 hours")
        
        # Test 3: Pivot for multi-region display
        pivot_prices = adapted_df.pivot(columns='REGIONID', values='RRP')
        print(f"✅ Pivot operation works: shape {pivot_prices.shape}")
        
        # Test 4: Price statistics
        price_stats = adapted_df.groupby('REGIONID')['RRP'].agg(['mean', 'min', 'max'])
        print(f"✅ Aggregation works:")
        print(price_stats.round(2))
        
        # Test 5: Resampling (hourly average)
        hourly_avg = adapted_df.groupby('REGIONID')['RRP'].resample('1h').mean()
        print(f"✅ Time resampling works: {len(hourly_avg)} hourly records")
        
        return True
        
    except Exception as e:
        print(f"❌ Dashboard operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_price_adapter_module():
    """Create the adapter module that the dashboard will use"""
    
    adapter_code = '''"""
Price data adapter for migrating to new data structure
Converts new prices5.parquet format to match old spot_hist.parquet format
"""

import pandas as pd
from pathlib import Path
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

def load_price_data(file_path=None):
    """
    Load price data with automatic format adaptation
    
    Args:
        file_path: Path to price parquet file (uses config default if None)
        
    Returns:
        DataFrame with old format structure (uppercase columns, datetime index)
    """
    if file_path is None:
        from ..shared.config import config
        file_path = config.spot_hist_file
    
    # Load the parquet file
    df = pd.read_parquet(file_path)
    
    # Check if this is new format (lowercase columns)
    if 'regionid' in df.columns and 'rrp' in df.columns:
        logger.info("Detected new price data format, applying adapter")
        
        # Apply adaptations
        df = df.rename(columns={
            'regionid': 'REGIONID',
            'rrp': 'RRP'
        })
        
        # Set datetime as index if it's a column
        if 'settlementdate' in df.columns:
            df = df.set_index('settlementdate')
            df = df.sort_index()
        
        logger.info(f"Adapted price data: {len(df)} records")
    
    # Old format - return as is
    elif 'REGIONID' in df.columns and 'RRP' in df.columns:
        logger.info("Using existing price data format")
    
    else:
        raise ValueError(f"Unknown price data format. Columns: {list(df.columns)}")
    
    return df
'''
    
    print("\n" + "=" * 60)
    print("PRICE ADAPTER MODULE")
    print("=" * 60)
    print("Creating src/aemo_dashboard/shared/price_adapter.py")
    
    adapter_path = Path("src/aemo_dashboard/shared/price_adapter.py")
    adapter_path.write_text(adapter_code)
    print("✅ Adapter module created")
    
    return adapter_path

if __name__ == "__main__":
    print("Starting Price Data Proof of Concept...\n")
    
    # Run comparison
    success, old_df, new_df = compare_price_data()
    
    if success and new_df is not None:
        print("\n✅ Data structure comparison successful!")
        
        # Create and test adapter
        adapted_df = create_price_adapter(new_df)
        
        if test_adapter_compatibility(old_df, adapted_df):
            print("\n✅ Adapter compatibility verified!")
            
            if test_dashboard_operations_with_adapter():
                print("\n✅ Dashboard operations verified!")
                
                # Create the adapter module
                adapter_path = create_price_adapter_module()
                
                print("\n" + "🎉" * 30)
                print("\nPRICE DATA MIGRATION READY!")
                print("\nSummary:")
                print("✅ Column mapping adapter created and tested")
                print("✅ All dashboard operations work with adapter")
                print(f"✅ Adapter module saved to: {adapter_path}")
                print("\nNext steps:")
                print("1. Update dashboard code to use price_adapter.load_price_data()")
                print("2. Test with actual dashboard")
                print("3. Update .env to point to new prices5.parquet")
                print("\n" + "🎉" * 30)
            else:
                print("\n❌ Dashboard operations need attention")
        else:
            print("\n❌ Adapter compatibility issues")
    else:
        print("\n❌ Data comparison failed")