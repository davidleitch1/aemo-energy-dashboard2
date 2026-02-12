#!/usr/bin/env python3
"""
Analyze gas fuel types in gen_info.pkl
"""

import pickle
import pandas as pd
from pathlib import Path
from collections import defaultdict

def analyze_gas_fuel_types():
    # Load the pickle file
    pkl_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl")
    
    print(f"Loading data from: {pkl_path}")
    
    try:
        with open(pkl_path, 'rb') as f:
            gen_info = pickle.load(f)
    except Exception as e:
        print(f"Error loading pickle file: {e}")
        return
    
    # Convert to DataFrame for easier analysis
    if isinstance(gen_info, dict):
        df = pd.DataFrame(gen_info).T
    else:
        df = gen_info
    
    print(f"\nTotal generators in dataset: {len(df)}")
    print(f"Columns available: {list(df.columns)}")
    
    # Check what fuel type column exists
    fuel_col = None
    for col in ['Fuel', 'Fuel Source - Primary', 'fuel_type', 'FuelType', 'Fuel_Type']:
        if col in df.columns:
            fuel_col = col
            break
    
    if not fuel_col:
        print("\nAvailable columns (showing first 20):")
        for i, col in enumerate(df.columns[:20]):
            print(f"  {i}: {col}")
        print("\nCouldn't find fuel type column. Please check column names above.")
        return
    
    print(f"\nUsing fuel type column: '{fuel_col}'")
    
    # Get all unique fuel types
    all_fuel_types = df[fuel_col].unique()
    print(f"\nAll unique fuel types ({len(all_fuel_types)}):")
    for ft in sorted(all_fuel_types):
        print(f"  - {ft}")
    
    # Filter for gas-related fuel types
    gas_keywords = ['gas', 'ccgt', 'ocgt', 'gt', 'natural gas', 'lng', 'csg', 'methane']
    
    # Find gas-related fuel types
    gas_fuel_types = []
    for fuel_type in all_fuel_types:
        if pd.notna(fuel_type):
            fuel_lower = str(fuel_type).lower()
            if any(keyword in fuel_lower for keyword in gas_keywords):
                gas_fuel_types.append(fuel_type)
    
    print(f"\n\nGAS-RELATED FUEL TYPES ANALYSIS")
    print("=" * 80)
    
    # Analyze each gas fuel type
    gas_summary = defaultdict(lambda: {'count': 0, 'capacity': 0, 'examples': []})
    
    # Check for capacity column
    cap_col = None
    for col in ['Capacity(MW)', 'Reg Cap (MW)', 'Nameplate Capacity (MW)', 'capacity', 'Capacity']:
        if col in df.columns:
            cap_col = col
            break
    
    # Check for site/station name column
    site_col = None
    for col in ['Site Name', 'Station Name', 'site', 'Site', 'Plant', 'plant_name']:
        if col in df.columns:
            site_col = col
            break
    
    for fuel_type in gas_fuel_types:
        gas_gens = df[df[fuel_col] == fuel_type]
        
        gas_summary[fuel_type]['count'] = len(gas_gens)
        
        if cap_col:
            # Convert capacity to numeric, handling any non-numeric values
            gas_summary[fuel_type]['capacity'] = pd.to_numeric(gas_gens[cap_col], errors='coerce').sum()
        
        # Get examples (up to 3)
        for idx, row in gas_gens.head(3).iterrows():
            example = {'duid': idx}  # idx should be DUID
            if site_col and site_col in row:
                example['site'] = row[site_col]
            if cap_col and cap_col in row:
                example['capacity'] = row[cap_col]
            gas_summary[fuel_type]['examples'].append(example)
    
    # Sort by capacity
    sorted_gas_types = sorted(gas_summary.items(), 
                             key=lambda x: x[1]['capacity'], 
                             reverse=True)
    
    print(f"\nFound {len(gas_fuel_types)} gas-related fuel types:\n")
    
    total_gas_count = 0
    total_gas_capacity = 0
    
    for fuel_type, info in sorted_gas_types:
        print(f"\nFuel Type: '{fuel_type}'")
        print(f"  Count: {info['count']} generators")
        if cap_col:
            print(f"  Total Capacity: {info['capacity']:,.1f} MW")
            total_gas_capacity += info['capacity']
        total_gas_count += info['count']
        
        print(f"  Examples:")
        for ex in info['examples']:
            example_str = f"    - {ex['duid']}"
            if 'site' in ex:
                example_str += f" ({ex['site']})"
            if 'capacity' in ex:
                example_str += f" - {ex['capacity']} MW"
            print(example_str)
    
    print("\n" + "=" * 80)
    print(f"TOTAL GAS GENERATION:")
    print(f"  Total Generators: {total_gas_count}")
    if cap_col:
        print(f"  Total Capacity: {total_gas_capacity:,.1f} MW")
    
    # Check for potential gas types that might not have obvious keywords
    print("\n\nOTHER FUEL TYPES (that might be gas-related):")
    print("=" * 80)
    
    # Look for fuel types that might be gas but don't match our keywords
    suspicious_keywords = ['turbine', 'peaker', 'reciprocating', 'engine', 'dual']
    
    other_suspicious = []
    for fuel_type in all_fuel_types:
        if pd.notna(fuel_type) and fuel_type not in gas_fuel_types:
            fuel_lower = str(fuel_type).lower()
            if any(keyword in fuel_lower for keyword in suspicious_keywords):
                other_suspicious.append(fuel_type)
    
    if other_suspicious:
        print("\nFuel types with keywords that might indicate gas:")
        for fuel_type in other_suspicious:
            count = len(df[df[fuel_col] == fuel_type])
            print(f"  - '{fuel_type}' ({count} generators)")
    else:
        print("\nNo other suspicious fuel types found.")
    
    # Show fuel types with small counts that might be misclassified
    print("\n\nFuel types with few generators (might be misclassified):")
    fuel_counts = df[fuel_col].value_counts()
    small_fuels = fuel_counts[fuel_counts <= 3]
    
    for fuel_type, count in small_fuels.items():
        if pd.notna(fuel_type):
            print(f"  - '{fuel_type}' ({count} generators)")

if __name__ == "__main__":
    analyze_gas_fuel_types()