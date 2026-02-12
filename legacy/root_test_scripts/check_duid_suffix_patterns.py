#!/usr/bin/env python3
"""
Check for DUID suffix patterns between AEMO data and gen_info.pkl
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

def main():
    # Load gen_info.pkl
    gen_info_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl")
    gen_info = load_pickle_file(gen_info_path)
    
    if gen_info is None:
        print("Failed to load gen_info.pkl")
        return
    
    # Get all DUIDs from gen_info.pkl
    pickle_duids = set(gen_info['DUID'].values)
    
    # Load comparison report to get AEMO DUIDs
    comparison_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/gen_info_comparison_report.txt")
    
    print("Checking for DUID suffix patterns...")
    print("="*80)
    
    # Check for DUIDs in pickle that might match AEMO DUIDs with added suffix
    print("\nDUIDs in gen_info.pkl that might have suffix variants in AEMO data:")
    print("-"*80)
    
    potential_matches = []
    
    for duid in pickle_duids:
        # Check if adding common suffixes would create a match
        for suffix in ['1', '2', '3', '4', '5', 'A', 'B', 'C']:
            potential_duid = duid + suffix
            # We'll need to check if these exist in AEMO data
            
        # Also check if the DUID ends with a letter (common pattern)
        if duid and not duid[-1].isdigit():
            potential_matches.append(duid)
    
    # Special cases we know about
    known_mismatches = [
        ("SHGEN", "SHGEN1", "Shoalhaven - missing suffix '1' in pickle"),
    ]
    
    print("\nKnown DUID naming mismatches:")
    print("-"*80)
    for pickle_duid, aemo_duid, description in known_mismatches:
        if pickle_duid in pickle_duids:
            print(f"  Pickle: {pickle_duid} -> AEMO: {aemo_duid} ({description})")
    
    # Check for DUIDs without numeric suffixes
    print("\nDUIDs in pickle without numeric suffixes (potential issues):")
    print("-"*80)
    count = 0
    for duid in sorted(pickle_duids):
        # Skip if last character is a digit
        if duid and not duid[-1].isdigit():
            # Get the unit info
            unit_info = gen_info[gen_info['DUID'] == duid].iloc[0]
            print(f"  {duid}: {unit_info['Site Name']} ({unit_info['Fuel']})")
            count += 1
            if count >= 20:  # Limit output
                print(f"  ... and {len([d for d in pickle_duids if d and not d[-1].isdigit()]) - 20} more")
                break
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total DUIDs in gen_info.pkl: {len(pickle_duids)}")
    print(f"DUIDs without numeric suffix: {len([d for d in pickle_duids if d and not d[-1].isdigit()])}")
    print("\nRecommendation: The gen_info.pkl file needs to be updated to match AEMO's")
    print("current DUID naming conventions, particularly adding numeric suffixes where")
    print("appropriate (e.g., SHGEN -> SHGEN1)")

if __name__ == "__main__":
    main()