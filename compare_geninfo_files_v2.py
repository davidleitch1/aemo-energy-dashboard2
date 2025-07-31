#!/usr/bin/env python3
"""
Compare gen_info.pkl with geninfo_july25.xlsx to find differences in DUIDs that exist in both files.
"""

import pandas as pd
import pickle
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any
import warnings

warnings.filterwarnings('ignore')

# File paths
PICKLE_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl"
EXCEL_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"

def load_pickle_data(path: str) -> pd.DataFrame:
    """Load the gen_info.pkl file."""
    with open(path, 'rb') as f:
        data = pickle.load(f)
    return data

def load_excel_data(path: str) -> pd.DataFrame:
    """Load the geninfo_july25.xlsx file."""
    # Read the Excel file, using the first row as headers
    df = pd.read_excel(path, header=1)
    
    # Print columns to verify
    print(f"Excel columns after loading: {list(df.columns)[:10]}...")
    
    return df

def normalize_string(s: Any) -> str:
    """Normalize string values for comparison."""
    if pd.isna(s) or s is None:
        return ""
    return str(s).strip().upper()

def parse_capacity(capacity: Any) -> float:
    """Parse capacity values, handling ranges like '100 - 200'."""
    if pd.isna(capacity) or capacity is None:
        return 0.0
    
    capacity_str = str(capacity).strip()
    
    # Handle range (e.g., "100 - 200")
    if ' - ' in capacity_str:
        parts = capacity_str.split(' - ')
        try:
            # Use the maximum value from the range
            return float(parts[1])
        except (ValueError, IndexError):
            return 0.0
    
    # Handle normal numeric values
    try:
        return float(capacity_str)
    except ValueError:
        return 0.0

def are_strings_different(val1: str, val2: str) -> bool:
    """Check if two strings are meaningfully different."""
    norm1 = normalize_string(val1)
    norm2 = normalize_string(val2)
    
    # Both empty
    if not norm1 and not norm2:
        return False
    
    # One empty, one not
    if bool(norm1) != bool(norm2):
        return True
    
    return norm1 != norm2

def are_numbers_different(val1: float, val2: float, threshold: float = 0.1) -> bool:
    """Check if two numbers are meaningfully different."""
    # Handle NaN values
    if np.isnan(val1) and np.isnan(val2):
        return False
    if np.isnan(val1) or np.isnan(val2):
        return True
    
    # Check if difference is above threshold
    return abs(val1 - val2) > threshold

def compare_duid_records(pickle_row: pd.Series, excel_row: pd.Series) -> Dict[str, Tuple[Any, Any]]:
    """Compare two DUID records and return differences."""
    differences = {}
    
    # First check what columns we actually have in Excel
    excel_cols = excel_row.index.tolist()
    
    # Map pickle columns to excel columns (trying different possible names)
    field_mappings = {
        'Region': ('Region', 'Region'),
        'Site Name': ('Site Name', 'Station Name'),
        'Owner': ('Owner', 'Owner'),
        'Capacity(MW)': ('Capacity(MW)', 'Nameplate Capacity'),
        'Storage(MWh)': ('Storage(MWh)', 'Storage Capacity'),
        'Fuel': ('Fuel', 'Fuel Bucket Summary')
    }
    
    for field_name, (pickle_col, excel_col) in field_mappings.items():
        # Check if columns exist
        if excel_col not in excel_cols:
            # Try to find similar column
            similar_cols = [col for col in excel_cols if excel_col.lower() in col.lower() or col.lower() in excel_col.lower()]
            if similar_cols:
                excel_col = similar_cols[0]
            else:
                continue
        
        pickle_val = pickle_row.get(pickle_col, np.nan)
        excel_val = excel_row.get(excel_col, np.nan)
        
        # Handle capacity and storage as numbers
        if field_name in ['Capacity(MW)', 'Storage(MWh)']:
            pickle_num = parse_capacity(pickle_val)
            excel_num = parse_capacity(excel_val)
            
            if are_numbers_different(pickle_num, excel_num):
                differences[field_name] = (pickle_val, excel_val)
        
        # Handle other fields as strings
        else:
            if are_strings_different(pickle_val, excel_val):
                differences[field_name] = (pickle_val, excel_val)
    
    return differences

def main():
    print("Loading data files...")
    
    # Load both datasets
    pickle_df = load_pickle_data(PICKLE_PATH)
    excel_df = load_excel_data(EXCEL_PATH)
    
    print(f"Pickle file: {len(pickle_df)} records")
    print(f"Excel file: {len(excel_df)} records")
    
    # Check if Excel has 'DUID' column or similar
    excel_cols = excel_df.columns.tolist()
    duid_col = None
    for col in excel_cols:
        if 'DUID' in col.upper():
            duid_col = col
            break
    
    if not duid_col:
        print(f"ERROR: Could not find DUID column in Excel file. Available columns: {excel_cols}")
        return
    
    print(f"Using Excel DUID column: '{duid_col}'")
    
    # Find common DUIDs
    pickle_duids = set(pickle_df['DUID'].str.strip().str.upper())
    excel_duids = set(excel_df[duid_col].dropna().astype(str).str.strip().str.upper())
    common_duids = pickle_duids & excel_duids
    
    print(f"\nCommon DUIDs: {len(common_duids)}")
    print(f"DUIDs only in pickle: {len(pickle_duids - excel_duids)}")
    print(f"DUIDs only in excel: {len(excel_duids - pickle_duids)}")
    
    # Compare common DUIDs
    all_differences = []
    
    for duid in sorted(common_duids):
        # Get records from both datasets
        pickle_row = pickle_df[pickle_df['DUID'].str.strip().str.upper() == duid].iloc[0]
        excel_row = excel_df[excel_df[duid_col].astype(str).str.strip().str.upper() == duid].iloc[0]
        
        # Compare records
        differences = compare_duid_records(pickle_row, excel_row)
        
        if differences:
            all_differences.append({
                'DUID': duid,
                'differences': differences
            })
    
    # Generate report
    print(f"\n{'='*80}")
    print("COMPARISON REPORT")
    print(f"{'='*80}")
    print(f"\nTotal DUIDs with differences: {len(all_differences)}")
    
    if all_differences:
        # Detailed differences
        print(f"\n{'='*80}")
        print("DETAILED DIFFERENCES")
        print(f"{'='*80}")
        
        for item in all_differences[:20]:  # Show first 20 for brevity
            duid = item['DUID']
            differences = item['differences']
            
            print(f"\nDUID: {duid}")
            print("-" * 40)
            
            for field, (old_val, new_val) in differences.items():
                print(f"  {field}:")
                print(f"    Pickle: {old_val}")
                print(f"    Excel:  {new_val}")
        
        if len(all_differences) > 20:
            print(f"\n... and {len(all_differences) - 20} more DUIDs with differences")
        
        # Summary table
        print(f"\n{'='*80}")
        print("SUMMARY BY FIELD")
        print(f"{'='*80}")
        
        field_counts = {}
        for item in all_differences:
            for field in item['differences'].keys():
                field_counts[field] = field_counts.get(field, 0) + 1
        
        print(f"\n{'Field':<20} {'Count':<10} {'Percentage':<10}")
        print("-" * 40)
        for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(common_duids)) * 100
            print(f"{field:<20} {count:<10} {percentage:.1f}%")
        
        # Create DataFrame for export
        export_data = []
        for item in all_differences:
            duid = item['DUID']
            for field, (old_val, new_val) in item['differences'].items():
                export_data.append({
                    'DUID': duid,
                    'Field': field,
                    'Pickle_Value': old_val,
                    'Excel_Value': new_val
                })
        
        export_df = pd.DataFrame(export_data)
        output_file = "geninfo_comparison_report.csv"
        export_df.to_csv(output_file, index=False)
        print(f"\nDetailed report saved to: {output_file}")
        
        # Sample differences for each field
        print(f"\n{'='*80}")
        print("SAMPLE DIFFERENCES BY FIELD")
        print(f"{'='*80}")
        
        for field in field_counts.keys():
            print(f"\n{field} (showing up to 5 examples):")
            print("-" * 40)
            
            count = 0
            for item in all_differences:
                if field in item['differences']:
                    duid = item['DUID']
                    old_val, new_val = item['differences'][field]
                    print(f"  {duid}: '{old_val}' → '{new_val}'")
                    count += 1
                    if count >= 5:
                        break
    
    else:
        print("\nNo meaningful differences found between the two files!")

if __name__ == "__main__":
    main()