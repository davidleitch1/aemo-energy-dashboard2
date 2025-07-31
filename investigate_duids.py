#!/usr/bin/env python3
"""
Investigate DUID naming conventions in gen_info.pkl
Search for DUIDs that might exist under different names
"""

import pickle
import pandas as pd
from pathlib import Path

def load_pickle_file(filepath):
    """Load pickle file and return dataframe"""
    try:
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            if isinstance(data, dict):
                return pd.DataFrame.from_dict(data, orient='index')
            return data
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def search_duids(df, search_terms):
    """Search for DUIDs containing any of the search terms"""
    matches = pd.DataFrame()
    
    # Check if DUID is a column instead of index
    if 'DUID' in df.columns:
        for term in search_terms:
            # Search in DUID column - case insensitive
            mask = df['DUID'].str.contains(term, case=False, na=False)
            # Also search in Site Name
            if 'Site Name' in df.columns:
                mask |= df['Site Name'].str.contains(term, case=False, na=False)
            if mask.any():
                matches = pd.concat([matches, df[mask]])
    else:
        # Search in index (DUID) - case insensitive
        for term in search_terms:
            mask = df.index.astype(str).str.contains(term, case=False, na=False)
            if mask.any():
                matches = pd.concat([matches, df[mask]])
    
    # Remove duplicates if any
    if not matches.empty:
        matches = matches.drop_duplicates()
    
    return matches

def main():
    # Path to gen_info.pkl - try multiple locations
    possible_paths = [
        Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl"),
        Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl"),
        Path("/Volumes/davidleitch/aemo_production/gen_info.pkl")
    ]
    
    gen_info_path = None
    for path in possible_paths:
        if path.exists():
            gen_info_path = path
            print(f"Found gen_info.pkl at: {path}")
            break
    
    if gen_info_path is None:
        print("Could not find gen_info.pkl in any of the expected locations")
        return
    
    print("Loading gen_info.pkl...")
    gen_info = load_pickle_file(gen_info_path)
    
    if gen_info is None:
        print("Failed to load gen_info.pkl")
        return
    
    print(f"\nTotal DUIDs in gen_info.pkl: {len(gen_info)}")
    print(f"Columns: {list(gen_info.columns)}")
    
    # Define search terms for each unit
    search_groups = {
        "Hornsdale Power Reserve": ["HPR", "HORNSDALE"],
        "Shoalhaven": ["SHGEN", "SHOALHAVEN"],
        "Koorangie": ["KESSB", "KOORANGIE", "KORAN"]
    }
    
    print("\n" + "="*80)
    print("SEARCHING FOR DUIDS BY KEYWORDS")
    print("="*80)
    
    all_matches = pd.DataFrame()
    
    for unit_name, search_terms in search_groups.items():
        print(f"\n### Searching for {unit_name} ###")
        print(f"Search terms: {search_terms}")
        
        matches = search_duids(gen_info, search_terms)
        
        if not matches.empty:
            print(f"\nFound {len(matches)} matching DUIDs:")
            for idx, row in matches.iterrows():
                print(f"\n  DUID: {row.get('DUID', idx)}")
                if 'Site Name' in matches.columns:
                    print(f"  Site Name: {row['Site Name']}")
                if 'station_name' in matches.columns:
                    print(f"  Station: {row['station_name']}")
                if 'Fuel' in matches.columns:
                    print(f"  Fuel Type: {row['Fuel']}")
                if 'fuel_type' in matches.columns:
                    print(f"  Fuel Type: {row['fuel_type']}")
                if 'Capacity(MW)' in matches.columns:
                    print(f"  Capacity: {row['Capacity(MW)']} MW")
                if 'reg_cap' in matches.columns:
                    print(f"  Registered Capacity: {row['reg_cap']} MW")
                if 'Storage(MWh)' in matches.columns and pd.notna(row['Storage(MWh)']):
                    print(f"  Storage: {row['Storage(MWh)']} MWh")
                if 'Region' in matches.columns:
                    print(f"  Region: {row['Region']}")
                if 'Owner' in matches.columns:
                    print(f"  Owner: {row['Owner']}")
            
            all_matches = pd.concat([all_matches, matches])
        else:
            print("  No matches found")
    
    # Check exact DUIDs mentioned in the comparison
    print("\n" + "="*80)
    print("CHECKING EXACT DUIDS FROM COMPARISON REPORT")
    print("="*80)
    
    exact_duids = ["HPR1", "SHGEN1", "KESSB1"]
    
    for duid in exact_duids:
        print(f"\nChecking for exact DUID: {duid}")
        # Check if DUID is in column
        if 'DUID' in gen_info.columns:
            mask = gen_info['DUID'] == duid
            if mask.any():
                print(f"  ✓ Found in gen_info.pkl")
                row = gen_info[mask].iloc[0]
                if 'Site Name' in gen_info.columns:
                    print(f"  Site Name: {row['Site Name']}")
                if 'Fuel' in gen_info.columns:
                    print(f"  Fuel Type: {row['Fuel']}")
                if 'Capacity(MW)' in gen_info.columns:
                    print(f"  Capacity: {row['Capacity(MW)']} MW")
                if 'Storage(MWh)' in gen_info.columns and pd.notna(row['Storage(MWh)']):
                    print(f"  Storage: {row['Storage(MWh)']} MWh")
                if 'Region' in gen_info.columns:
                    print(f"  Region: {row['Region']}")
            else:
                print(f"  ✗ NOT found in gen_info.pkl")
        else:
            # Check in index
            if duid in gen_info.index:
                print(f"  ✓ Found in gen_info.pkl")
                row = gen_info.loc[duid]
                if 'station_name' in gen_info.columns:
                    print(f"  Station: {row['station_name']}")
                if 'fuel_type' in gen_info.columns:
                    print(f"  Fuel Type: {row['fuel_type']}")
                if 'reg_cap' in gen_info.columns:
                    print(f"  Registered Capacity: {row['reg_cap']} MW")
            else:
                print(f"  ✗ NOT found in gen_info.pkl")
    
    # Show all battery/storage units
    print("\n" + "="*80)
    print("ALL BATTERY/STORAGE UNITS IN GEN_INFO.PKL")
    print("="*80)
    
    fuel_col = 'Fuel' if 'Fuel' in gen_info.columns else 'fuel_type' if 'fuel_type' in gen_info.columns else None
    
    if fuel_col:
        battery_mask = gen_info[fuel_col].str.contains('Battery|Storage', case=False, na=False)
        battery_units = gen_info[battery_mask]
        
        print(f"\nFound {len(battery_units)} battery/storage units:")
        for idx, row in battery_units.iterrows():
            print(f"\n  DUID: {row.get('DUID', idx)}")
            if 'Site Name' in battery_units.columns:
                print(f"  Site Name: {row['Site Name']}")
            if 'station_name' in battery_units.columns:
                print(f"  Station: {row['station_name']}")
            if fuel_col in battery_units.columns:
                print(f"  Fuel Type: {row[fuel_col]}")
            if 'Capacity(MW)' in battery_units.columns:
                print(f"  Capacity: {row['Capacity(MW)']} MW")
            if 'reg_cap' in battery_units.columns:
                print(f"  Registered Capacity: {row['reg_cap']} MW")
            if 'Storage(MWh)' in battery_units.columns and pd.notna(row['Storage(MWh)']):
                print(f"  Storage: {row['Storage(MWh)']} MWh")
            if 'Region' in battery_units.columns:
                print(f"  Region: {row['Region']}")
    
    # Also check if there's a mapping or alias column
    print("\n" + "="*80)
    print("CHECKING FOR POSSIBLE ALIAS/MAPPING COLUMNS")
    print("="*80)
    
    potential_alias_cols = ['alias', 'old_duid', 'new_duid', 'alternative_name', 'previous_name']
    found_alias_cols = [col for col in potential_alias_cols if col in gen_info.columns]
    
    if found_alias_cols:
        print(f"Found potential alias columns: {found_alias_cols}")
        # Check if any of our target DUIDs appear in these columns
    else:
        print("No obvious alias columns found")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nTotal unique matches found: {len(all_matches)}")
    print(f"Exact DUIDs found: {sum(1 for duid in exact_duids if duid in gen_info.index)}/{len(exact_duids)}")

if __name__ == "__main__":
    main()