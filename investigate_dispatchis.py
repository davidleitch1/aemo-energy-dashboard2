#!/usr/bin/env python3
"""
Investigate DISPATCHIS reports to find battery charging data.
Downloads a sample file and checks for negative values in battery DUIDs.
"""

import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import re

def download_latest_dispatchis():
    """Download the most recent DISPATCHIS file"""
    
    base_url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    
    print("Fetching DISPATCHIS file list...")
    response = requests.get(base_url)
    
    if response.status_code != 200:
        print(f"Failed to fetch file list: {response.status_code}")
        return None
    
    # Parse HTML to find DISPATCHIS files
    # The HTML has the pattern: HREF="/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_..."
    pattern = r'HREF="/Reports/Current/DispatchIS_Reports/(PUBLIC_DISPATCHIS_\d+_\d+\.zip)"'
    files = re.findall(pattern, response.text)
    
    if not files:
        print("No DISPATCHIS files found")
        return None
    
    # Sort and get the most recent
    files.sort()
    latest_file = files[-1]
    
    print(f"Downloading latest file: {latest_file}")
    file_url = base_url + latest_file
    
    response = requests.get(file_url)
    if response.status_code != 200:
        print(f"Failed to download file: {response.status_code}")
        return None
    
    return response.content

def extract_and_analyze(zip_content):
    """Extract and analyze DISPATCHIS data for battery charging"""
    
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.CSV')]
        
        if not csv_files:
            print("No CSV files found in archive")
            return None
        
        print(f"Found CSV file: {csv_files[0]}")
        
        with zf.open(csv_files[0]) as csv_file:
            # Read the entire file to understand structure
            content = csv_file.read().decode('utf-8')
            lines = content.strip().split('\n')
            
            print(f"Total lines in file: {len(lines)}")
            
            # Find different record types
            record_types = {}
            for line in lines:
                if line.startswith('D,'):
                    parts = line.split(',')
                    if len(parts) > 2:
                        record_type = parts[1]
                        if record_type not in record_types:
                            record_types[record_type] = []
                        record_types[record_type].append(line)
            
            print(f"\nRecord types found:")
            for rtype, records in record_types.items():
                print(f"  {rtype}: {len(records)} records")
            
            # Look for UNIT_SOLUTION or similar
            unit_tables = [k for k in record_types.keys() if 'UNIT' in k]
            print(f"\nUnit-related tables: {unit_tables}")
            
            # Analyze UNIT_SOLUTION if it exists
            if 'UNIT_SOLUTION' in record_types:
                print("\nAnalyzing UNIT_SOLUTION records...")
                
                # Parse first few records to understand structure
                sample_records = record_types['UNIT_SOLUTION'][:5]
                print("Sample records:")
                for record in sample_records:
                    print(f"  {record[:100]}...")  # First 100 chars
                
                # Try to identify column positions
                header_line = None
                for line in lines:
                    if line.startswith('I,UNIT_SOLUTION'):
                        header_line = line
                        break
                
                if header_line:
                    headers = header_line.split(',')
                    print(f"\nColumn headers ({len(headers)} columns):")
                    for i, h in enumerate(headers[:20]):  # First 20 columns
                        print(f"  {i}: {h}")
                    
                    # Look for important columns
                    important_cols = ['DUID', 'SETTLEMENTDATE', 'TOTALCLEARED', 
                                    'INITIALMW', 'AVAILABILITY', 'DISPATCHMODE']
                    for col in important_cols:
                        if col in headers:
                            idx = headers.index(col)
                            print(f"\n{col} at index {idx}")
                
                # Parse all UNIT_SOLUTION records into DataFrame
                unit_data = []
                for record in record_types['UNIT_SOLUTION']:
                    parts = record.split(',')
                    unit_data.append(parts)
                
                # Assume standard column positions (may need adjustment)
                # Typically: D, UNIT_SOLUTION, SETTLEMENTDATE, ..., DUID, ..., TOTALCLEARED
                if unit_data:
                    print(f"\nParsing {len(unit_data)} UNIT_SOLUTION records...")
                    
                    # Create DataFrame (adjust column indices based on actual format)
                    df_data = []
                    for row in unit_data:
                        if len(row) > 10:  # Ensure enough columns
                            try:
                                # These indices need to be verified from header
                                duid = row[6] if len(row) > 6 else None
                                totalcleared = float(row[11]) if len(row) > 11 else None
                                
                                if duid and totalcleared is not None:
                                    df_data.append({
                                        'duid': duid,
                                        'totalcleared': totalcleared
                                    })
                            except (ValueError, IndexError):
                                continue
                    
                    if df_data:
                        df = pd.DataFrame(df_data)
                        
                        # Check for known battery DUIDs
                        battery_duids = ['HPR1', 'DALNTH1', 'LBB1', 'TIB1', 'TB2B1', 
                                       'BLYTHB1', 'ADPBA1', 'LGAPBS1']
                        
                        battery_data = df[df['duid'].isin(battery_duids)]
                        
                        if not battery_data.empty:
                            print(f"\nFound {len(battery_data)} battery records")
                            print("\nBattery DUID value ranges:")
                            
                            for duid in battery_duids:
                                duid_data = battery_data[battery_data['duid'] == duid]
                                if not duid_data.empty:
                                    values = duid_data['totalcleared']
                                    neg_count = (values < 0).sum()
                                    pos_count = (values > 0).sum()
                                    zero_count = (values == 0).sum()
                                    
                                    print(f"\n{duid}:")
                                    print(f"  Positive: {pos_count}")
                                    print(f"  Negative: {neg_count}")
                                    print(f"  Zero: {zero_count}")
                                    
                                    if neg_count > 0:
                                        print(f"  ✅ CHARGING DATA FOUND!")
                                        print(f"  Charging range: {values[values < 0].min():.1f} to {values[values < 0].max():.1f} MW")
                                    
                                    if pos_count > 0:
                                        print(f"  Discharge range: {values[values > 0].min():.1f} to {values[values > 0].max():.1f} MW")
                        else:
                            print("\nNo battery DUIDs found in data")
                    else:
                        print("\nCould not parse UNIT_SOLUTION data")
            
            # Also check DISPATCH_UNIT_SCADA if it exists
            if 'DISPATCH_UNIT_SCADA' in record_types:
                print("\n\nDISPATCH_UNIT_SCADA table also found - this might contain SCADA values")
                print(f"Records: {len(record_types['DISPATCH_UNIT_SCADA'])}")
            
            return record_types

def main():
    """Main investigation function"""
    
    print("="*60)
    print("DISPATCHIS Battery Charging Investigation")
    print("="*60)
    
    # Download latest file
    zip_content = download_latest_dispatchis()
    
    if zip_content:
        print(f"\nFile downloaded successfully ({len(zip_content)/1024:.1f} KB)")
        
        # Analyze content
        extract_and_analyze(zip_content)
    else:
        print("\nFailed to download DISPATCHIS file")
    
    print("\n" + "="*60)
    print("Investigation complete")
    print("="*60)

if __name__ == "__main__":
    main()