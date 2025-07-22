#!/usr/bin/env python3
"""
Fix Rooftop Solar Data Gap
Downloads missing rooftop solar data for the 12.5-hour gap: 2025-07-13 00:55:00 → 13:30:00
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import zipfile
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import sys
from typing import List, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_data_service.collectors.rooftop_collector import RooftopCollector


async def get_all_rooftop_files_in_range(start_time: datetime, end_time: datetime) -> List[str]:
    """Get all rooftop PV files between start and end times."""
    print(f"🔍 Searching for rooftop files between {start_time} and {end_time}")
    
    base_url = "http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Get directory listing
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.get(base_url, headers=headers, timeout=30)
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all ZIP file links
        zip_files = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.zip') and 'ROOFTOP_PV_ACTUAL_MEASUREMENT' in href:
                # Extract timestamp from filename: PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20250713010000_*.zip
                if 'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_' in href:
                    try:
                        # Get just the filename, not the full path
                        filename = href.split('/')[-1]
                        
                        # Split by underscore and get the timestamp part
                        parts = filename.split('_')
                        if len(parts) >= 6:
                            timestamp_str = parts[5][:12]  # Get YYYYMMDDHHMM from the 6th part
                            file_time = datetime.strptime(timestamp_str, '%Y%m%d%H%M')
                            
                            # Check if file is in our target range
                            if start_time <= file_time <= end_time:
                                zip_files.append(href)
                                print(f"   📄 Found: {filename} ({file_time})")
                    except (IndexError, ValueError) as e:
                        # Debug: print problematic filenames but less verbose
                        if len(zip_files) < 3:  # Only show first few errors
                            print(f"   ⚠️  Could not parse: {href.split('/')[-1]} - {e}")
                        continue
        
        print(f"✅ Found {len(zip_files)} files in gap period")
        return sorted(zip_files)
        
    except Exception as e:
        print(f"❌ Error getting file list: {e}")
        return []


async def download_and_convert_file(filename: str, collector: RooftopCollector) -> Optional[pd.DataFrame]:
    """Download and convert a single rooftop file."""
    print(f"⬇️  Downloading: {filename}")
    
    try:
        # Download ZIP content
        zip_content = await collector._download_rooftop_zip(filename)
        if zip_content is None:
            print(f"   ❌ Failed to download {filename}")
            return None
        
        # Parse 30-minute data
        df_30min = collector._parse_rooftop_zip(zip_content)
        if df_30min.empty:
            print(f"   ⚠️  No data in {filename}")
            return None
        
        # Convert to 5-minute intervals
        df_5min = collector._convert_30min_to_5min(df_30min)
        if df_5min.empty:
            print(f"   ⚠️  Conversion failed for {filename}")
            return None
        
        print(f"   ✅ Converted {len(df_30min)} → {len(df_5min)} records")
        return df_5min
        
    except Exception as e:
        print(f"   ❌ Error processing {filename}: {e}")
        return None


async def fix_rooftop_gap():
    """Main function to fix the rooftop data gap."""
    print("🔧 ROOFTOP SOLAR DATA GAP REPAIR")
    print("=" * 50)
    
    # Define the gap period - expand to catch all relevant files
    gap_start = datetime(2025, 7, 13, 1, 0)   # 2025-07-13 01:00:00
    gap_end = datetime(2025, 7, 13, 13, 30)   # 2025-07-13 13:30:00
    
    print(f"📅 Target gap: {gap_start} → {gap_end}")
    print(f"⏱️  Duration: {(gap_end - gap_start).total_seconds() / 3600:.1f} hours")
    
    # Initialize collector
    collector = RooftopCollector()
    
    # Load existing data to check current state
    rooftop_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/rooftop_solar.parquet")
    
    if rooftop_file.exists():
        existing_df = pd.read_parquet(rooftop_file)
        existing_df['settlementdate'] = pd.to_datetime(existing_df['settlementdate'])
        print(f"📊 Current data: {len(existing_df):,} records")
        print(f"   Range: {existing_df['settlementdate'].min()} → {existing_df['settlementdate'].max()}")
        
        # Check gap
        gap_data = existing_df[
            (existing_df['settlementdate'] >= gap_start) & 
            (existing_df['settlementdate'] <= gap_end)
        ]
        print(f"   Gap coverage: {len(gap_data)} records (should be ~150 for 12.5 hours)")
    else:
        print("❌ Rooftop parquet file not found")
        return
    
    # Get files to download
    files_to_download = await get_all_rooftop_files_in_range(gap_start, gap_end)
    
    if not files_to_download:
        print("❌ No files found in gap period")
        return
    
    print(f"\n🔄 Processing {len(files_to_download)} files...")
    
    # Download and process each file
    new_data_list = []
    successful_downloads = 0
    
    for filename in files_to_download:
        df_5min = await download_and_convert_file(filename, collector)
        if df_5min is not None:
            new_data_list.append(df_5min)
            successful_downloads += 1
        
        # Add small delay to be respectful to NEMWEB
        await asyncio.sleep(0.5)
    
    print(f"\n📈 Download Results:")
    print(f"   Files processed: {len(files_to_download)}")
    print(f"   Successful: {successful_downloads}")
    print(f"   Failed: {len(files_to_download) - successful_downloads}")
    
    if not new_data_list:
        print("❌ No data downloaded successfully")
        return
    
    # Combine all new data
    print(f"\n🔗 Combining data from {len(new_data_list)} files...")
    combined_new = pd.concat(new_data_list, ignore_index=True)
    combined_new = combined_new.sort_values('settlementdate').drop_duplicates(subset=['settlementdate'])
    
    print(f"   Combined: {len(combined_new)} new records")
    print(f"   Range: {combined_new['settlementdate'].min()} → {combined_new['settlementdate'].max()}")
    
    # Merge with existing data
    print(f"\n🔄 Merging with existing data...")
    
    # Filter out any overlapping records
    existing_no_gap = existing_df[
        ~((existing_df['settlementdate'] >= gap_start) & 
          (existing_df['settlementdate'] <= gap_end))
    ]
    
    print(f"   Existing records outside gap: {len(existing_no_gap):,}")
    print(f"   New records for gap: {len(combined_new):,}")
    
    # Combine all data
    final_df = pd.concat([existing_no_gap, combined_new], ignore_index=True)
    final_df = final_df.sort_values('settlementdate').drop_duplicates(subset=['settlementdate'])
    
    print(f"   Final dataset: {len(final_df):,} records")
    
    # Save updated data
    print(f"\n💾 Saving updated rooftop data...")
    final_df.to_parquet(rooftop_file, index=False)
    
    file_size_mb = rooftop_file.stat().st_size / (1024 * 1024)
    print(f"   Saved to: {rooftop_file}")
    print(f"   File size: {file_size_mb:.2f} MB")
    
    # Verify gap is filled
    print(f"\n🔍 Verifying gap repair...")
    
    gap_records = final_df[
        (final_df['settlementdate'] >= gap_start) & 
        (final_df['settlementdate'] <= gap_end)
    ]
    
    expected_records = int((gap_end - gap_start).total_seconds() / 300)  # 5-minute intervals
    coverage_percent = (len(gap_records) / expected_records) * 100
    
    print(f"   Gap period records: {len(gap_records)}/{expected_records} ({coverage_percent:.1f}%)")
    
    if coverage_percent >= 95:
        print(f"   ✅ Gap successfully filled!")
    else:
        print(f"   ⚠️  Gap partially filled - some data may still be missing")
    
    # Final data integrity check
    print(f"\n📊 FINAL DATA INTEGRITY:")
    print(f"   Total records: {len(final_df):,}")
    print(f"   Date range: {final_df['settlementdate'].min()} → {final_df['settlementdate'].max()}")
    
    # Check for any remaining large gaps
    time_diffs = final_df['settlementdate'].sort_values().diff()
    large_gaps = time_diffs[time_diffs > timedelta(minutes=35)]  # More than ~6 intervals
    
    if len(large_gaps) == 0:
        print(f"   ✅ NO MISSING DATA - File is complete!")
    else:
        print(f"   ⚠️  {len(large_gaps)} gaps > 35 minutes still exist")
        for gap_time in large_gaps.head(3):
            gap_minutes = gap_time.total_seconds() / 60
            print(f"      Gap: {gap_minutes:.0f} minutes")
    
    print(f"\n🎉 Gap repair complete!")
    

if __name__ == "__main__":
    print("AEMO Rooftop Solar Gap Repair Tool")
    print("==================================")
    
    asyncio.run(fix_rooftop_gap())