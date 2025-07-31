#!/usr/bin/env python3
"""
Final column mapping summary between geninfo Excel and gen_info.pkl
"""

import pandas as pd

# Load Excel file
excel_path = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"
df = pd.read_excel(excel_path, header=1)

print("="*80)
print("FINAL COLUMN MAPPING RECOMMENDATION")
print("="*80)

print("\nTarget pickle DataFrame columns:")
print("  • Region")
print("  • Site Name")
print("  • Owner")
print("  • DUID")
print("  • Capacity(MW)")
print("  • Storage(MWh)")
print("  • Fuel")

print("\n" + "="*80)
print("RECOMMENDED MAPPING:")
print("="*80)

mapping = {
    'Region': 'Region',
    'Site Name': 'Site Name',
    'Owner': 'Owner',
    'DUID': 'DUID',
    'Capacity(MW)': 'Nameplate Capacity (MW)',  # Better choice than Lower Nameplate
    'Storage(MWh)': 'Storage Capacity (MWh)',
    'Fuel': 'Fuel Bucket Summary'  # Better choice than Asset Type
}

print("\n{:<20} -> {:<30} (Notes)".format("Pickle Column", "Excel Column"))
print("-"*80)

for pickle_col, excel_col in mapping.items():
    notes = ""
    if pickle_col == 'Capacity(MW)':
        notes = "Most complete (1553 non-null vs 935)"
    elif pickle_col == 'Fuel':
        notes = "Clean fuel categories (Coal, Gas, Solar, etc.)"
    elif pickle_col == 'DUID':
        notes = "Only 36% filled (mainly existing plants)"
    elif pickle_col == 'Storage(MWh)':
        notes = "Only for battery storage (37% filled)"
    
    print("{:<20} -> {:<30} {}".format(pickle_col, excel_col, notes))

print("\n" + "="*80)
print("IMPLEMENTATION CODE:")
print("="*80)

print("""
# Code to create gen_info DataFrame from Excel:

import pandas as pd

# Load Excel file with proper headers
excel_path = "geninfo_july25.xlsx"
df_excel = pd.read_excel(excel_path, header=1)

# Filter for existing plants (optional - depends on use case)
# df_excel = df_excel[df_excel['Asset Type'] == 'Existing Plant']

# Create gen_info DataFrame with renamed columns
gen_info = df_excel[['Region', 'Site Name', 'Owner', 'DUID', 
                     'Nameplate Capacity (MW)', 'Storage Capacity (MWh)', 
                     'Fuel Bucket Summary']].copy()

gen_info.columns = ['Region', 'Site Name', 'Owner', 'DUID', 
                    'Capacity(MW)', 'Storage(MWh)', 'Fuel']

# Save as pickle
gen_info.to_pickle('gen_info.pkl')
""")

print("\n" + "="*80)
print("DATA QUALITY NOTES:")
print("="*80)

print("\n1. DUID Coverage:")
print(f"   - Total rows: {len(df)}")
print(f"   - Rows with DUID: {df['DUID'].notna().sum()} ({df['DUID'].notna().sum()/len(df)*100:.1f}%)")
print(f"   - Existing plants with DUID: {df[(df['Asset Type'] == 'Existing Plant') & df['DUID'].notna()].shape[0]}")
print(f"   - Projects with DUID: {df[(df['Asset Type'] == 'Project') & df['DUID'].notna()].shape[0]}")

print("\n2. Fuel Categories in 'Fuel Bucket Summary':")
fuel_counts = df['Fuel Bucket Summary'].value_counts()
for fuel, count in fuel_counts.items():
    print(f"   - {fuel}: {count}")

print("\n3. Consider filtering:")
print("   - If you only need existing plants: df[df['Asset Type'] == 'Existing Plant']")
print("   - If you only need plants with DUID: df[df['DUID'].notna()]")
print("   - If you want both existing and committed: df[df['Status Bucket Summary'].isin(['Existing less Announced Withdrawal', 'Committed'])]")