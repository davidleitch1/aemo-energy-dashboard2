#!/usr/bin/env python3
"""
Script to identify new DUIDs in geninfo_july25.xlsx that are not in gen_info.pkl
"""

import pandas as pd
import pickle
import sys
from pathlib import Path

def load_pickle_duids(pickle_path):
    """Load DUIDs from the pickle file"""
    try:
        with open(pickle_path, 'rb') as f:
            gen_info = pickle.load(f)
        
        # Get all DUIDs from the pickle file
        pickle_duids = set(gen_info['DUID'].unique())
        print(f"Loaded {len(pickle_duids)} unique DUIDs from pickle file")
        return pickle_duids, gen_info
    except Exception as e:
        print(f"Error loading pickle file: {e}")
        sys.exit(1)

def load_excel_data(excel_path):
    """Load data from the Excel file"""
    try:
        # Read the Excel file with header in row 1 (0-indexed)
        df = pd.read_excel(excel_path, header=1)
        print(f"Loaded {len(df)} rows from Excel file")
        print(f"Excel columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        sys.exit(1)

def find_new_duids(pickle_duids, excel_df):
    """Find DUIDs that exist in Excel but not in pickle"""
    # Get unique DUIDs from Excel, excluding NaN and '-'
    excel_duids = set(excel_df['DUID'].dropna().unique())
    # Remove placeholder DUIDs like '-'
    excel_duids = {duid for duid in excel_duids if duid != '-' and str(duid).strip() != '-'}
    print(f"Found {len(excel_duids)} unique valid DUIDs in Excel file")
    
    # Find new DUIDs
    new_duids = excel_duids - pickle_duids
    print(f"\nFound {len(new_duids)} new DUIDs")
    
    return new_duids

def create_new_duids_table(excel_df, new_duids):
    """Create a table of new DUIDs with mapped columns"""
    # Filter for new DUIDs, excluding '-' placeholder
    new_duids_df = excel_df[excel_df['DUID'].isin(new_duids) & (excel_df['DUID'] != '-')].copy()
    
    # Select and rename columns
    columns_mapping = {
        'DUID': 'DUID',
        'Region': 'Region',
        'Site Name': 'Site Name',
        'Owner': 'Owner',
        'Nameplate Capacity (MW)': 'Capacity(MW)',
        'Storage Capacity (MWh)': 'Storage(MWh)',
        'Fuel Bucket Summary': 'Fuel'
    }
    
    # Check which columns exist
    available_columns = {}
    for old_col, new_col in columns_mapping.items():
        if old_col in new_duids_df.columns:
            available_columns[old_col] = new_col
        else:
            print(f"Warning: Column '{old_col}' not found in Excel file")
    
    # Select and rename available columns
    result_df = new_duids_df[list(available_columns.keys())].copy()
    result_df.rename(columns=available_columns, inplace=True)
    
    # Sort by Region and then by DUID
    if 'Region' in result_df.columns:
        result_df = result_df.sort_values(['Region', 'DUID'])
    else:
        result_df = result_df.sort_values('DUID')
    
    # Reset index
    result_df = result_df.reset_index(drop=True)
    
    return result_df

def main():
    # File paths
    pickle_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl")
    excel_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx")
    
    print("Finding new DUIDs in geninfo_july25.xlsx...")
    print("=" * 80)
    
    # Load data
    pickle_duids, gen_info_df = load_pickle_duids(pickle_path)
    excel_df = load_excel_data(excel_path)
    
    # Find new DUIDs
    new_duids = find_new_duids(pickle_duids, excel_df)
    
    if not new_duids:
        print("\nNo new DUIDs found!")
        return
    
    # Create table of new DUIDs
    new_duids_table = create_new_duids_table(excel_df, new_duids)
    
    # Display results
    print("\n" + "=" * 80)
    print(f"NEW DUIDS TABLE ({len(new_duids_table)} entries)")
    print("=" * 80)
    
    # Display the table
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    print(new_duids_table.to_string(index=True))
    
    # Summary by region
    if 'Region' in new_duids_table.columns:
        print("\n" + "=" * 80)
        print("SUMMARY BY REGION")
        print("=" * 80)
        region_summary = new_duids_table['Region'].value_counts().sort_index()
        for region, count in region_summary.items():
            print(f"{region}: {count} new DUIDs")
        print(f"\nTotal: {len(new_duids_table)} new DUIDs")
    
    # Summary by fuel type
    if 'Fuel' in new_duids_table.columns:
        print("\n" + "=" * 80)
        print("SUMMARY BY FUEL TYPE")
        print("=" * 80)
        fuel_summary = new_duids_table['Fuel'].value_counts().sort_index()
        for fuel, count in fuel_summary.items():
            print(f"{fuel}: {count} new DUIDs")
    
    # Save to CSV for further analysis
    output_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/new_duids_report.csv")
    new_duids_table.to_csv(output_path, index=False)
    print(f"\n\nResults saved to: {output_path}")

if __name__ == "__main__":
    main()