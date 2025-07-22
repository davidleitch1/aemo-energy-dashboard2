#!/usr/bin/env python3
"""
Test if the price analysis fix works
"""
import os
import sys
from pathlib import Path

# Set up environment
os.environ['USE_DUCKDB'] = 'true'
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from datetime import datetime, timedelta
from aemo_dashboard.analysis.price_analysis import PriceAnalysisMotor

print("Testing Price Analysis Fix...")

# Create motor
motor = PriceAnalysisMotor()

# Load data
print("\n1. Loading data...")
motor.load_data(use_30min_data=False)

# Set date range
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

print(f"\n2. Integrating data for {start_date.date()} to {end_date.date()}...")
motor.integrate_data(start_date=start_date, end_date=end_date)

print(f"   Integrated data shape: {motor.integrated_data.shape}")
print(f"   Columns: {list(motor.integrated_data.columns)}")

# Test aggregation with correct column name
print("\n3. Testing aggregation by fuel_type...")
try:
    result = motor.calculate_aggregated_prices(['fuel_type'])
    if result.empty:
        print("❌ Empty result")
    else:
        print(f"✅ Aggregation successful: {len(result)} rows")
        print("\nTop 5 by revenue:")
        print(result.head())
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {str(e)}")

# Test aggregation by region
print("\n4. Testing aggregation by region...")
try:
    result = motor.calculate_aggregated_prices(['region'])
    if result.empty:
        print("❌ Empty result")
    else:
        print(f"✅ Aggregation successful: {len(result)} rows")
        print(result)
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {str(e)}")

# Test mixed aggregation
print("\n5. Testing aggregation by region and fuel_type...")
try:
    result = motor.calculate_aggregated_prices(['region', 'fuel_type'])
    if result.empty:
        print("❌ Empty result")
    else:
        print(f"✅ Aggregation successful: {len(result)} rows")
        print("\nSample results:")
        print(result.head(10))
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {str(e)}")