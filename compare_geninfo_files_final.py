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
    
    # Create a combined capacity column from lower and upper
    if 'Lower Nameplate Capacity (MW)' in df.columns and 'Upper Nameplate Capacity (MW)' in df.columns:
        # Use upper capacity as the main capacity value
        df['Nameplate Capacity'] = df['Upper Nameplate Capacity (MW)']
    
    print(f"Excel columns: {list(df.columns)[:15]}...")
    
    return df

def normalize_string(s: Any) -> str:
    """Normalize string values for comparison."""
    if pd.isna(s) or s is None:
        return ""
    return str(s).strip().upper()

def parse_capacity(capacity: Any) -> float:
    """Parse capacity values, handling ranges and NaN."""
    if pd.isna(capacity) or capacity is None:
        return np.nan
    
    try:
        return float(capacity)
    except ValueError:
        return np.nan

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
    # Both NaN
    if np.isnan(val1) and np.isnan(val2):
        return False
    
    # One NaN, one not (but don't count 0 vs NaN as different)
    if np.isnan(val1) and not np.isnan(val2):
        return val2 > threshold  # Only different if val2 is meaningful
    if np.isnan(val2) and not np.isnan(val1):
        return val1 > threshold  # Only different if val1 is meaningful
    
    # Check if difference is above threshold
    return abs(val1 - val2) > threshold

def compare_duid_records(pickle_row: pd.Series, excel_row: pd.Series) -> Dict[str, Tuple[Any, Any]]:
    """Compare two DUID records and return differences."""
    differences = {}
    
    # Get pickle values
    pickle_vals = {
        'Region': pickle_row.get('Region', np.nan),
        'Site Name': pickle_row.get('Site Name', np.nan),
        'Owner': pickle_row.get('Owner', np.nan),
        'Capacity(MW)': parse_capacity(pickle_row.get('Capacity(MW)', np.nan)),
        'Storage(MWh)': parse_capacity(pickle_row.get('Storage(MWh)', np.nan)),
        'Fuel': pickle_row.get('Fuel', np.nan)
    }
    
    # Get excel values
    excel_vals = {
        'Region': excel_row.get('Region', np.nan),
        'Site Name': excel_row.get('Site Name', np.nan),
        'Owner': excel_row.get('Owner', np.nan),
        'Capacity(MW)': parse_capacity(excel_row.get('Nameplate Capacity', np.nan)),
        'Storage(MWh)': parse_capacity(excel_row.get('Storage Capacity (MWh)', np.nan)),
        'Fuel': excel_row.get('Fuel Type', np.nan)
    }
    
    # Compare each field
    for field in pickle_vals.keys():
        pickle_val = pickle_vals[field]
        excel_val = excel_vals[field]
        
        # Handle capacity and storage as numbers
        if field in ['Capacity(MW)', 'Storage(MWh)']:
            if are_numbers_different(pickle_val, excel_val):
                # Show original values for report
                pickle_orig = pickle_row.get(field, np.nan)
                if field == 'Capacity(MW)':
                    excel_orig = excel_row.get('Nameplate Capacity', np.nan)
                else:
                    excel_orig = excel_row.get('Storage Capacity (MWh)', np.nan)
                differences[field] = (pickle_orig, excel_orig)
        
        # Handle other fields as strings
        else:
            if are_strings_different(pickle_val, excel_val):
                differences[field] = (pickle_vals[field], excel_vals[field])
    
    return differences

def main():
    print("Loading data files...")
    
    # Load both datasets
    pickle_df = load_pickle_data(PICKLE_PATH)
    excel_df = load_excel_data(EXCEL_PATH)
    
    print(f"Pickle file: {len(pickle_df)} records")
    print(f"Excel file: {len(excel_df)} records")
    
    # Find common DUIDs
    pickle_duids = set(pickle_df['DUID'].str.strip().str.upper())
    excel_duids = set(excel_df['DUID'].dropna().astype(str).str.strip().str.upper())
    common_duids = pickle_duids & excel_duids
    
    print(f"\nCommon DUIDs: {len(common_duids)}")
    print(f"DUIDs only in pickle: {len(pickle_duids - excel_duids)}")
    print(f"DUIDs only in excel: {len(excel_duids - pickle_duids)}")
    
    # Show DUIDs only in pickle
    only_pickle = sorted(pickle_duids - excel_duids)
    print(f"\nDUIDs only in pickle file: {only_pickle}")
    
    # Compare common DUIDs
    all_differences = []
    
    for duid in sorted(common_duids):
        # Get records from both datasets
        pickle_row = pickle_df[pickle_df['DUID'].str.strip().str.upper() == duid].iloc[0]
        excel_row = excel_df[excel_df['DUID'].astype(str).str.strip().str.upper() == duid].iloc[0]
        
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
        # Summary table first
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
        
        # Create DataFrame for export with more details
        export_data = []
        for item in all_differences:
            duid = item['DUID']
            pickle_row = pickle_df[pickle_df['DUID'].str.strip().str.upper() == duid].iloc[0]
            excel_row = excel_df[excel_df['DUID'].astype(str).str.strip().str.upper() == duid].iloc[0]
            
            row_data = {
                'DUID': duid,
                'Pickle_Site_Name': pickle_row.get('Site Name', ''),
                'Excel_Site_Name': excel_row.get('Site Name', ''),
                'Pickle_Region': pickle_row.get('Region', ''),
                'Excel_Region': excel_row.get('Region', ''),
                'Pickle_Owner': pickle_row.get('Owner', ''),
                'Excel_Owner': excel_row.get('Owner', ''),
                'Pickle_Capacity_MW': pickle_row.get('Capacity(MW)', ''),
                'Excel_Capacity_MW': excel_row.get('Nameplate Capacity', ''),
                'Pickle_Storage_MWh': pickle_row.get('Storage(MWh)', ''),
                'Excel_Storage_MWh': excel_row.get('Storage Capacity (MWh)', ''),
                'Pickle_Fuel': pickle_row.get('Fuel', ''),
                'Excel_Fuel': excel_row.get('Fuel Type', ''),
                'Differences': ', '.join(item['differences'].keys())
            }
            export_data.append(row_data)
        
        export_df = pd.DataFrame(export_data)
        output_file = "geninfo_comparison_report.csv"
        export_df.to_csv(output_file, index=False)
        print(f"\nDetailed report saved to: {output_file}")
        
        # Sample differences for each field
        print(f"\n{'='*80}")
        print("SAMPLE DIFFERENCES BY FIELD")
        print(f"{'='*80}")
        
        for field in sorted(field_counts.keys()):
            print(f"\n{field} (showing up to 10 examples):")
            print("-" * 60)
            
            count = 0
            for item in all_differences:
                if field in item['differences']:
                    duid = item['DUID']
                    old_val, new_val = item['differences'][field]
                    
                    # Get site name for context
                    pickle_row = pickle_df[pickle_df['DUID'].str.strip().str.upper() == duid].iloc[0]
                    site_name = pickle_row.get('Site Name', 'Unknown')
                    
                    print(f"  {duid:<12} ({site_name[:30]})")
                    print(f"    Pickle: {old_val}")
                    print(f"    Excel:  {new_val}")
                    count += 1
                    if count >= 10:
                        break
        
        # Show significant capacity changes
        print(f"\n{'='*80}")
        print("SIGNIFICANT CAPACITY CHANGES (> 10 MW difference)")
        print(f"{'='*80}")
        
        significant_changes = []
        for item in all_differences:
            if 'Capacity(MW)' in item['differences']:
                duid = item['DUID']
                old_val, new_val = item['differences']['Capacity(MW)']
                
                old_num = parse_capacity(old_val)
                new_num = parse_capacity(new_val)
                
                if not np.isnan(old_num) and not np.isnan(new_num):
                    diff = abs(old_num - new_num)
                    if diff > 10:
                        pickle_row = pickle_df[pickle_df['DUID'].str.strip().str.upper() == duid].iloc[0]
                        site_name = pickle_row.get('Site Name', 'Unknown')
                        significant_changes.append({
                            'DUID': duid,
                            'Site': site_name,
                            'Old_MW': old_num,
                            'New_MW': new_num,
                            'Difference': diff
                        })
        
        if significant_changes:
            significant_changes.sort(key=lambda x: x['Difference'], reverse=True)
            print(f"\n{'DUID':<12} {'Site':<30} {'Old MW':>10} {'New MW':>10} {'Diff MW':>10}")
            print("-" * 80)
            for change in significant_changes[:20]:
                print(f"{change['DUID']:<12} {change['Site'][:30]:<30} {change['Old_MW']:>10.1f} {change['New_MW']:>10.1f} {change['Difference']:>10.1f}")
    
    else:
        print("\nNo meaningful differences found between the two files!")

if __name__ == "__main__":
    main()