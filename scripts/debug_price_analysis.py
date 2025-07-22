#!/usr/bin/env python3
"""
Debug price analysis issue
"""
import os
import sys
from pathlib import Path

# Set up environment
os.environ['USE_DUCKDB'] = 'true'
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from datetime import datetime, timedelta
from aemo_dashboard.analysis.price_analysis import PriceAnalysisMotor

print("Testing Price Analysis...")

# Create motor
motor = PriceAnalysisMotor()

# Load data
print("\n1. Loading data...")
if motor.load_data(use_30min_data=False):
    print("✅ Data loaded successfully")
    print(f"   Date ranges: {motor.date_ranges}")
else:
    print("❌ Failed to load data")
    sys.exit(1)

# Set date range
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

print(f"\n2. Integrating data for {start_date.date()} to {end_date.date()}...")
motor.integrate_data(start_date=start_date, end_date=end_date)

if motor.integrated_data is not None:
    print(f"✅ Data integrated: {len(motor.integrated_data)} records")
    print(f"   Columns: {list(motor.integrated_data.columns)}")
    print(f"   Sample data:")
    print(motor.integrated_data.head())
else:
    print("❌ No integrated data")
    sys.exit(1)

# Test aggregation
print("\n3. Testing aggregation by Fuel...")
try:
    result = motor.calculate_aggregated_prices(['Fuel'])
    if result.empty:
        print("❌ Empty result")
    else:
        print(f"✅ Aggregation successful: {len(result)} rows")
        print(result)
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc()

# Check for revenue columns
print("\n4. Checking revenue columns...")
cols = motor.integrated_data.columns
revenue_cols = [col for col in cols if 'revenue' in col.lower()]
print(f"   Revenue columns found: {revenue_cols}")

# Check column data types
print("\n5. Column data types:")
print(motor.integrated_data.dtypes)