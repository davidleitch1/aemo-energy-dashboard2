#!/usr/bin/env python3
"""
Analyze missing DUIDs by checking if they exist in the Excel file
and displaying their details if found.
"""

import pandas as pd
import pickle
from pathlib import Path

# File paths
PARQUET_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet"
EXCEL_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"
PICKLE_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl"

def load_data():
    """Load all necessary data files."""
    print("Loading data files...")
    
    # Load parquet file to get unique DUIDs
    print(f"  Loading parquet: {PARQUET_PATH}")
    parquet_df = pd.read_parquet(PARQUET_PATH)
    parquet_duids = set(parquet_df['duid'].unique())
    print(f"  Found {len(parquet_duids)} unique DUIDs in parquet")
    
    # Load pickle file (it's a DataFrame)
    print(f"  Loading pickle: {PICKLE_PATH}")
    with open(PICKLE_PATH, 'rb') as f:
        gen_info_df = pickle.load(f)
    # Get unique DUIDs from the DataFrame
    pickle_duids = set(gen_info_df['DUID'].unique()) if 'DUID' in gen_info_df.columns else set()
    print(f"  Found {len(pickle_duids)} DUIDs in pickle")
    
    # Load Excel file
    print(f"  Loading Excel: {EXCEL_PATH}")
    excel_df = pd.read_excel(EXCEL_PATH, header=1)  # Header is in row 1
    print(f"  Found {len(excel_df)} rows in Excel")
    
    return parquet_duids, pickle_duids, excel_df, gen_info_df

def find_missing_duids(parquet_duids, pickle_duids):
    """Find DUIDs that are in parquet but not in pickle."""
    missing_duids = parquet_duids - pickle_duids
    return sorted(list(missing_duids))

def analyze_missing_duids(missing_duids, excel_df):
    """Check which missing DUIDs exist in Excel and get their details."""
    found_in_excel = []
    not_in_excel = []
    
    # Map Excel columns to our desired names
    column_mapping = {
        'Site Name': 'Site_Name',
        'Owner': 'Owner', 
        'Asset Type': 'Type',
        'Technology Type': 'Tech_Type',
        'Fuel Type': 'fuel_type',
        'Nameplate Capacity (MW)': 'TotalCapacityMW',
        'Region': 'Region',
        'Unit Status': 'Status'
    }
    
    for duid in missing_duids:
        # Check if DUID exists in Excel
        if 'DUID' in excel_df.columns:
            mask = excel_df['DUID'] == duid
            matching_rows = excel_df[mask]
            
            if len(matching_rows) > 0:
                row = matching_rows.iloc[0]
                duid_info = {'DUID': duid}
                
                # Extract relevant information
                for excel_col, our_col in column_mapping.items():
                    if excel_col in excel_df.columns:
                        duid_info[our_col] = row[excel_col]
                    else:
                        duid_info[our_col] = 'N/A'
                        
                found_in_excel.append(duid_info)
            else:
                not_in_excel.append(duid)
        else:
            not_in_excel.append(duid)
    
    return found_in_excel, not_in_excel

def display_results(missing_duids, found_in_excel, not_in_excel):
    """Display the analysis results."""
    print("\n" + "="*80)
    print("MISSING DUID ANALYSIS RESULTS")
    print("="*80)
    
    print(f"\nTotal missing DUIDs: {len(missing_duids)}")
    print(f"Found in Excel: {len(found_in_excel)}")
    print(f"NOT found in Excel: {len(not_in_excel)}")
    
    if found_in_excel:
        print("\n" + "-"*80)
        print("DUIDs FOUND IN EXCEL (with details):")
        print("-"*80)
        
        # Convert to DataFrame for better display
        df = pd.DataFrame(found_in_excel)
        
        # Reorder columns for better readability
        column_order = ['DUID', 'Site_Name', 'Owner', 'fuel_type', 
                       'TotalCapacityMW', 'Region', 'Status', 'Type', 
                       'Tech_Type']
        
        # Only include columns that exist
        columns = [col for col in column_order if col in df.columns]
        df = df[columns]
        
        # Display as table
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        print(df.to_string(index=False))
        
        # Summary by fuel type
        if 'fuel_type' in df.columns:
            print("\nSummary by Fuel Type:")
            fuel_summary = df.groupby('fuel_type').agg({
                'DUID': 'count',
                'TotalCapacityMW': 'sum'
            }).rename(columns={'DUID': 'Count'})
            print(fuel_summary.to_string())
    
    if not_in_excel:
        print("\n" + "-"*80)
        print("DUIDs NOT FOUND IN EXCEL:")
        print("-"*80)
        print("These may be aggregated units, virtual units, or special market participants")
        
        # Display in columns for readability
        cols = 4
        for i in range(0, len(not_in_excel), cols):
            row = not_in_excel[i:i+cols]
            print("  " + "  ".join(f"{duid:20}" for duid in row))
    
    # Print the actual missing DUIDs list for reference
    print("\n" + "-"*80)
    print("COMPLETE LIST OF MISSING DUIDs:")
    print("-"*80)
    print(", ".join(f"'{duid}'" for duid in missing_duids))

def main():
    """Main analysis function."""
    try:
        # Load all data
        parquet_duids, pickle_duids, excel_df, gen_info = load_data()
        
        # Find missing DUIDs
        missing_duids = find_missing_duids(parquet_duids, pickle_duids)
        print(f"\nFound {len(missing_duids)} DUIDs in parquet but not in pickle")
        
        # Analyze which are in Excel
        found_in_excel, not_in_excel = analyze_missing_duids(missing_duids, excel_df)
        
        # Display results
        display_results(missing_duids, found_in_excel, not_in_excel)
        
        # Save results to CSV for further analysis
        if found_in_excel:
            df = pd.DataFrame(found_in_excel)
            output_path = "missing_duids_found_in_excel.csv"
            df.to_csv(output_path, index=False)
            print(f"\n✅ Saved DUIDs found in Excel to: {output_path}")
        
        if not_in_excel:
            pd.DataFrame({'DUID': not_in_excel}).to_csv("missing_duids_not_in_excel.csv", index=False)
            print(f"✅ Saved DUIDs NOT in Excel to: missing_duids_not_in_excel.csv")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()