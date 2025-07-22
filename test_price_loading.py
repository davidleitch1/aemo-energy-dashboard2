#!/usr/bin/env python3
"""Test price data loading to debug the issue"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set up environment
os.environ['AEMO_DATA_PATH'] = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2'

from aemo_dashboard.shared.price_adapter import load_price_data
from aemo_dashboard.shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

# Test different date ranges
test_cases = [
    {
        'name': 'Last 30 days',
        'end_date': datetime.now(),
        'start_date': datetime.now() - timedelta(days=30)
    },
    {
        'name': 'Last 90 days',
        'end_date': datetime.now(),
        'start_date': datetime.now() - timedelta(days=90)
    },
    {
        'name': 'Last year',
        'end_date': datetime.now(),
        'start_date': datetime.now() - timedelta(days=365)
    },
    {
        'name': 'June 2025 to now',
        'end_date': datetime.now(),
        'start_date': datetime(2025, 6, 1)
    },
    {
        'name': 'May 2025 to now (should trigger hybrid)',
        'end_date': datetime.now(),
        'start_date': datetime(2025, 5, 1)
    }
]

# Test regions
test_regions = ['NSW1', 'VIC1']

print("Testing price data loading...")
print("="*60)

for test in test_cases:
    print(f"\nTest: {test['name']}")
    print(f"Date range: {test['start_date'].date()} to {test['end_date'].date()}")
    
    try:
        data = load_price_data(
            start_date=test['start_date'],
            end_date=test['end_date'],
            regions=test_regions,
            resolution='auto'
        )
        
        if data.empty:
            print("  Result: NO DATA RETURNED")
        else:
            print(f"  Result: {len(data):,} records loaded")
            print(f"  Type: {type(data)}")
            print(f"  Columns: {data.columns.tolist() if hasattr(data, 'columns') else 'No columns attribute'}")
            print(f"  Index type: {type(data.index) if hasattr(data, 'index') else 'No index'}")
            # If index is datetime, it might be the settlementdate
            if hasattr(data.index, 'name'):
                print(f"  Index name: {data.index.name}")
            # Try to access data in different ways
            if 'SETTLEMENTDATE' in data.columns:
                date_col = 'SETTLEMENTDATE'
            elif 'settlementdate' in data.columns:
                date_col = 'settlementdate'
            elif isinstance(data.index, pd.DatetimeIndex):
                print(f"  Date range in index: {data.index.min()} to {data.index.max()}")
                date_col = None
            else:
                date_col = None
                
            if date_col:
                print(f"  Date range in data: {data[date_col].min()} to {data[date_col].max()}")
            
            # Check for region and price columns
            for col_upper, col_lower in [('REGIONID', 'regionid'), ('RRP', 'rrp')]:
                if col_upper in data.columns:
                    print(f"  {col_upper}: {sorted(data[col_upper].unique()) if col_upper == 'REGIONID' else f'${data[col_upper].min():.2f} to ${data[col_upper].max():.2f}'}")
                elif col_lower in data.columns:
                    print(f"  {col_lower}: {sorted(data[col_lower].unique()) if col_lower == 'regionid' else f'${data[col_lower].min():.2f} to ${data[col_lower].max():.2f}'}")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "="*60)
print("\nChecking file availability directly...")

# Check files directly
files = {
    'prices5': '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices5.parquet',
    'prices30': '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices30.parquet'
}

import pandas as pd

for name, path in files.items():
    if os.path.exists(path):
        df = pd.read_parquet(path)
        print(f"\n{name}: {path}")
        print(f"  Exists: Yes")
        print(f"  Records: {len(df):,}")
        print(f"  Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
    else:
        print(f"\n{name}: {path}")
        print(f"  Exists: No")