#!/usr/bin/env python3
"""
Script to update gen_info.pkl with new DUIDs from Excel file
"""

import pandas as pd
import pickle
import shutil
from datetime import datetime
import os

# File paths
PICKLE_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl"
EXCEL_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"

# List of new DUIDs to add (actual DUIDs found in Excel but not in pickle)
# Excluding '-' as it's not a valid DUID
NEW_DUIDS = [
    'BROWNMT', 'ADVMH1', 'CUSF1', 'JGULLY', 'KiataWF1',
    'MLLFRHF1', 'RESS1G', 'SNB05', 'SNOWSTH1', 'TAHMOOR',
    'TB2BG1', 'ULBESS1', 'WAMBOWF1'
]

def load_gen_info():
    """Load the existing gen_info.pkl file"""
    print("Loading existing gen_info.pkl...")
    with open(PICKLE_PATH, 'rb') as f:
        df = pickle.load(f)
    return df

def load_excel_data():
    """Load and filter the Excel file for new DUIDs"""
    print("Loading Excel file...")
    # Read Excel with proper headers (skip first row which contains the title)
    excel_df = pd.read_excel(EXCEL_PATH, header=1)
    
    # Let's first check what columns we have
    print(f"Excel columns: {excel_df.columns.tolist()}")
    
    # Filter for new DUIDs
    new_records = excel_df[excel_df['DUID'].isin(NEW_DUIDS)].copy()
    
    print(f"Found {len(new_records)} new DUIDs in Excel file")
    return new_records

def map_excel_to_pickle_format(excel_df):
    """Map Excel columns to match pickle file structure"""
    print("Mapping Excel columns to pickle format...")
    
    # Create mapped DataFrame with the correct column names
    mapped_df = pd.DataFrame()
    
    # Direct mappings
    mapped_df['Region'] = excel_df['Region']
    mapped_df['Site Name'] = excel_df['Site Name']
    mapped_df['Owner'] = excel_df['Owner']
    mapped_df['DUID'] = excel_df['DUID']
    
    # Handle capacity - convert to float and handle any strings/ranges
    capacity_values = []
    for cap in excel_df['Nameplate Capacity (MW)']:
        if pd.isna(cap):
            capacity_values.append(0.0)
        elif isinstance(cap, str):
            # Handle ranges like "0.22 - 349.98" by taking the upper value
            if '-' in str(cap):
                parts = str(cap).split('-')
                capacity_values.append(float(parts[-1].strip()))
            else:
                capacity_values.append(float(cap))
        else:
            capacity_values.append(float(cap))
    
    mapped_df['Capacity(MW)'] = capacity_values
    
    # Handle storage capacity - it might be NaN for non-battery assets
    mapped_df['Storage(MWh)'] = excel_df['Storage Capacity (MWh)'].fillna(0.0)
    
    # Handle fuel type - replace NaN with empty string
    mapped_df['Fuel'] = excel_df['Fuel Bucket Summary'].fillna('')
    
    return mapped_df

def backup_original_file():
    """Create a backup of the original pickle file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{PICKLE_PATH}.backup_{timestamp}"
    print(f"Creating backup: {backup_path}")
    shutil.copy2(PICKLE_PATH, backup_path)
    return backup_path

def update_gen_info():
    """Main function to update gen_info.pkl"""
    try:
        # Load existing data
        existing_df = load_gen_info()
        print(f"Existing gen_info has {len(existing_df)} records")
        print(f"Columns: {existing_df.columns.tolist()}")
        
        # Check for any existing DUIDs that match our new ones
        existing_duids = set(existing_df['DUID'].values)
        new_duids_set = set(NEW_DUIDS)
        already_exists = existing_duids.intersection(new_duids_set)
        
        if already_exists:
            print(f"WARNING: These DUIDs already exist: {already_exists}")
            print("They will be skipped to avoid duplicates")
        
        # Load and process new data
        excel_df = load_excel_data()
        
        if len(excel_df) == 0:
            print("No new DUIDs found in Excel file. Exiting.")
            return
        
        # Map to pickle format
        new_df = map_excel_to_pickle_format(excel_df)
        
        # Remove any DUIDs that already exist
        new_df = new_df[~new_df['DUID'].isin(already_exists)]
        
        print(f"\nNew records to add ({len(new_df)}):")
        for _, row in new_df.iterrows():
            print(f"  {row['DUID']}: {row['Site Name']} - {row['Fuel']} - {row['Capacity(MW)']} MW")
        
        # Create backup
        backup_path = backup_original_file()
        
        # Append new data
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Save updated DataFrame
        print(f"\nSaving updated gen_info.pkl with {len(updated_df)} total records...")
        with open(PICKLE_PATH, 'wb') as f:
            pickle.dump(updated_df, f)
        
        # Verification
        print("\nVerification:")
        print(f"Original records: {len(existing_df)}")
        print(f"New records added: {len(new_df)}")
        print(f"Total records: {len(updated_df)}")
        print(f"Backup saved to: {backup_path}")
        
        # Double-check by loading the saved file
        with open(PICKLE_PATH, 'rb') as f:
            verify_df = pickle.load(f)
        print(f"Verified saved file has {len(verify_df)} records")
        
        # Show some of the new entries
        print("\nSample of newly added entries:")
        new_duids_in_saved = verify_df[verify_df['DUID'].isin(new_df['DUID'].values)]
        print(new_duids_in_saved[['DUID', 'Site Name', 'Fuel', 'Capacity(MW)']].to_string())
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    update_gen_info()