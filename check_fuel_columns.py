#!/usr/bin/env python3
"""
Check fuel column options in the Excel file
"""

import pandas as pd

# Load Excel file with correct headers
excel_path = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"
df = pd.read_excel(excel_path, header=1)

print("Analyzing fuel-related columns...")
print("="*80)

# Check Asset Type values
print("\n1. Asset Type unique values:")
asset_types = df['Asset Type'].value_counts()
for val, count in asset_types.items():
    print(f"   {val}: {count}")

# Check Fuel Type values
print("\n2. Fuel Type unique values (top 20):")
fuel_types = df['Fuel Type'].value_counts().head(20)
for val, count in fuel_types.items():
    print(f"   {val}: {count}")

# Check Technology Type values
print("\n3. Technology Type unique values (top 20):")
tech_types = df['Technology Type'].value_counts().head(20)
for val, count in tech_types.items():
    print(f"   {val}: {count}")

# Check Fuel Bucket Summary
print("\n4. Fuel Bucket Summary unique values:")
fuel_bucket = df['Fuel Bucket Summary'].value_counts()
for val, count in fuel_bucket.items():
    print(f"   {val}: {count}")

# Check capacity columns
print("\n5. Capacity column analysis:")
print(f"   Lower Nameplate Capacity (MW) - Non-null: {df['Lower Nameplate Capacity (MW)'].notna().sum()}")
print(f"   Upper Nameplate Capacity (MW) - Non-null: {df['Upper Nameplate Capacity (MW)'].notna().sum()}")
print(f"   Nameplate Capacity (MW) - Non-null: {df['Nameplate Capacity (MW)'].notna().sum()}")
print(f"   Aggregated Lower Nameplate Capacity (MW) - Non-null: {df['Aggregated Lower Nameplate Capacity (MW)'].notna().sum()}")
print(f"   Aggregated Upper Nameplate Capacity (MW) - Non-null: {df['Aggregated Upper Nameplate Capacity (MW)'].notna().sum()}")

# Show sample of existing plants with all key fields
print("\n6. Sample of existing plants with DUID:")
existing_with_duid = df[(df['Asset Type'] == 'Existing Plant') & (df['DUID'].notna())].head(10)
cols_to_show = ['Region', 'Site Name', 'Owner', 'DUID', 'Fuel Type', 'Fuel Bucket Summary', 'Nameplate Capacity (MW)', 'Storage Capacity (MWh)']
print(existing_with_duid[cols_to_show].to_string())