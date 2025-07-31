#!/usr/bin/env python3
"""
Analyze geninfo Excel file and compare columns to gen_info.pkl DataFrame
"""

import pandas as pd
import pickle
import os
from pathlib import Path

def analyze_excel_and_pickle():
    # File paths
    excel_path = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"
    pickle_path = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/gen_info.pkl"
    
    print("="*80)
    print("GENINFO EXCEL FILE ANALYSIS")
    print("="*80)
    
    # Load Excel file
    print(f"\n1. Loading Excel file: {excel_path}")
    try:
        # First, let's check if the first row contains the headers
        df_excel_raw = pd.read_excel(excel_path)
        print(f"   First row values: {list(df_excel_raw.iloc[0])[:10]}...")  # Show first 10 values
        
        # The headers are in the first row, so reload with header=1
        df_excel = pd.read_excel(excel_path, header=1)
        print(f"   ✓ Successfully loaded Excel file with correct headers")
        print(f"   Shape: {df_excel.shape}")
    except Exception as e:
        print(f"   ✗ Error loading Excel file: {e}")
        return
    
    # Display Excel columns
    print("\n2. Excel file columns:")
    for i, col in enumerate(df_excel.columns):
        print(f"   {i+1:2d}. {col}")
    
    # Display sample rows from Excel
    print("\n3. Sample rows from Excel file:")
    print("-"*80)
    print(df_excel.head(5).to_string())
    
    # Load pickle file for comparison
    print("\n\n4. Loading pickle file for comparison...")
    try:
        with open(pickle_path, 'rb') as f:
            df_pickle = pickle.load(f)
        print(f"   ✓ Successfully loaded pickle file")
        print(f"   Shape: {df_pickle.shape}")
        print(f"\n   Pickle file columns:")
        for i, col in enumerate(df_pickle.columns):
            print(f"   {i+1:2d}. {col}")
    except Exception as e:
        print(f"   ✗ Error loading pickle file: {e}")
        df_pickle = None
    
    # Column mapping analysis
    print("\n\n5. COLUMN MAPPING ANALYSIS")
    print("="*80)
    print("\nTarget pickle columns: Region, Site Name, Owner, DUID, Capacity(MW), Storage(MWh), Fuel")
    print("\nSuggested mapping based on column names and data:")
    
    # Analyze each target column
    target_columns = ['Region', 'Site Name', 'Owner', 'DUID', 'Capacity(MW)', 'Storage(MWh)', 'Fuel']
    excel_columns = list(df_excel.columns)
    
    mapping = {}
    
    # Try to find matches
    for target in target_columns:
        print(f"\n   {target}:")
        found = False
        
        # Look for exact matches first
        if target in excel_columns:
            mapping[target] = target
            print(f"      → EXACT MATCH: '{target}'")
            found = True
        else:
            # Look for similar columns
            target_lower = target.lower()
            for excel_col in excel_columns:
                excel_lower = excel_col.lower()
                
                # Check for common patterns
                if target == 'Region' and 'region' in excel_lower:
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
                elif target == 'Site Name' and ('site' in excel_lower or 'station' in excel_lower or 'name' in excel_lower):
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
                elif target == 'Owner' and ('owner' in excel_lower or 'participant' in excel_lower):
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
                elif target == 'DUID' and 'duid' in excel_lower:
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
                elif target == 'Capacity(MW)' and ('capacity' in excel_lower or 'mw' in excel_lower):
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
                elif target == 'Storage(MWh)' and ('storage' in excel_lower or 'mwh' in excel_lower):
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
                elif target == 'Fuel' and ('fuel' in excel_lower or 'type' in excel_lower):
                    mapping[target] = excel_col
                    print(f"      → LIKELY MATCH: '{excel_col}'")
                    found = True
                    break
        
        if not found:
            print(f"      → NO MATCH FOUND")
    
    # Display final mapping
    print("\n\n6. FINAL COLUMN MAPPING")
    print("="*80)
    print("\n{:<20} -> {:<30}".format("Pickle Column", "Excel Column"))
    print("-"*50)
    for target, excel in mapping.items():
        print("{:<20} -> {:<30}".format(target, excel))
    
    # Display unmapped Excel columns
    mapped_excel_cols = set(mapping.values())
    unmapped_excel = [col for col in excel_columns if col not in mapped_excel_cols]
    
    if unmapped_excel:
        print("\n\n7. UNMAPPED EXCEL COLUMNS (not used in mapping):")
        print("-"*50)
        for col in unmapped_excel:
            print(f"   • {col}")
            # Show sample values
            sample_values = df_excel[col].dropna().head(3).tolist()
            if sample_values:
                print(f"     Sample values: {sample_values}")
    
    # Data type analysis
    print("\n\n8. DATA TYPE ANALYSIS")
    print("="*80)
    for target, excel_col in mapping.items():
        if excel_col in df_excel.columns:
            dtype = df_excel[excel_col].dtype
            non_null_count = df_excel[excel_col].notna().sum()
            unique_count = df_excel[excel_col].nunique()
            print(f"\n   {target} ('{excel_col}'):")
            print(f"      Data type: {dtype}")
            print(f"      Non-null values: {non_null_count}/{len(df_excel)} ({non_null_count/len(df_excel)*100:.1f}%)")
            print(f"      Unique values: {unique_count}")

if __name__ == "__main__":
    analyze_excel_and_pickle()