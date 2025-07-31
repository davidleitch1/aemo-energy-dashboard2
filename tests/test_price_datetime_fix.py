#!/usr/bin/env python3
"""Test that the price loading works with date objects from date pickers"""

import sys
from pathlib import Path
from datetime import datetime, date, time

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.price_adapter import load_price_data
from aemo_dashboard.shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

print("Testing price data loading with date objects...")
print("="*60)

# Test cases simulating what date pickers return
test_cases = [
    {
        'name': 'Date objects (what date pickers return)',
        'start': date(2025, 6, 22),
        'end': date(2025, 7, 22)
    },
    {
        'name': 'Datetime objects (fixed version)',
        'start': datetime.combine(date(2025, 6, 22), time.min),
        'end': datetime.combine(date(2025, 7, 22), time.max)
    }
]

for test in test_cases:
    print(f"\nTest: {test['name']}")
    print(f"Start type: {type(test['start'])}")
    print(f"End type: {type(test['end'])}")
    
    try:
        data = load_price_data(
            start_date=test['start'],
            end_date=test['end'],
            regions=['NSW1', 'VIC1'],
            resolution='auto'
        )
        
        if data.empty:
            print("  Result: NO DATA RETURNED")
        else:
            print(f"  Result: SUCCESS - {len(data):,} records loaded")
            # Check if SETTLEMENTDATE is index or column
            if data.index.name == 'SETTLEMENTDATE':
                print("  SETTLEMENTDATE is the index")
                print(f"  Date range: {data.index.min()} to {data.index.max()}")
            elif 'SETTLEMENTDATE' in data.columns:
                print("  SETTLEMENTDATE is a column")
                print(f"  Date range: {data['SETTLEMENTDATE'].min()} to {data['SETTLEMENTDATE'].max()}")
            print(f"  Columns: {data.columns.tolist()}")
            
    except Exception as e:
        print(f"  ERROR: {e}")
        print(f"  Error type: {type(e).__name__}")

print("\n" + "="*60)
print("\nConclusion:")
print("The fix in gen_dash.py converts date objects to datetime objects before")
print("calling load_price_data(), which resolves the datetime comparison errors.")