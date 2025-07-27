#!/usr/bin/env python3
"""
Backfill missing SCADA30 data from MMSDM archives.

Missing periods:
- December 2020: 2020-12-01 00:30:00 to 2020-12-31 23:30:00
- October 2021: 2021-10-01 00:30:00 to 2021-10-31 23:30:00
- June 2022: 2022-06-01 00:30:00 to 2022-06-30 23:30:00
"""

import requests
import zipfile
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import logging
import re
import os
import sys
from pathlib import Path
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default data path (development)
DEFAULT_DATA_PATH = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2'

def parse_mms_csv(content):
    """Parse AEMO MMS format CSV content"""
    lines = content.decode('utf-8', errors='ignore').strip().split('\n')
    
    data_rows = []
    header_cols = None
    
    for line in lines:
        if not line.strip():
            continue
            
        parts = line.split(',')
        
        if parts[0] == 'I' and len(parts) > 2 and parts[2] == 'UNIT_SCADA':
            # Header row - extract column names
            header_cols = parts[4:]  # Skip I,DISPATCH,UNIT_SCADA,version
            
        elif parts[0] == 'D' and len(parts) > 2 and parts[2] == 'UNIT_SCADA':
            # Data row
            if header_cols:
                data_parts = parts[4:]  # Skip D,DISPATCH,UNIT_SCADA,version
                if len(data_parts) >= len(header_cols):
                    data_rows.append(data_parts[:len(header_cols)])
    
    if header_cols and data_rows:
        # Clean column names
        header_cols = [col.strip() for col in header_cols]
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=header_cols)
        
        # Convert key columns
        if 'SETTLEMENTDATE' in df.columns:
            # Remove quotes from date strings
            df['SETTLEMENTDATE'] = df['SETTLEMENTDATE'].str.strip('"')
            df['settlementdate'] = pd.to_datetime(df['SETTLEMENTDATE'], format='%Y/%m/%d %H:%M:%S')
        if 'DUID' in df.columns:
            df['duid'] = df['DUID'].str.strip()
        if 'SCADAVALUE' in df.columns:
            df['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
            
        return df[['settlementdate', 'duid', 'scadavalue']].dropna()
    
    return pd.DataFrame()

def download_and_process_month(year, month, data_path, test_mode=False):
    """Download and process MMSDM archive for a specific month"""
    
    month_str = f"{month:02d}"
    url = f"https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month_str}.zip"
    
    logger.info(f"Processing {year}-{month_str}")
    logger.info(f"Archive URL: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    # Data containers
    scada5_data = []
    scada30_data = []
    
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()
            
            # Get total size
            total_size = int(response.headers.get('content-length', 0))
            logger.info(f"Archive size: {total_size / (1024**3):.1f} GB")
            
            # Download in chunks
            chunks = []
            downloaded = 0
            
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                chunks.append(chunk)
                downloaded += len(chunk)
                
                # Progress update
                if downloaded % (100 * 1024 * 1024) == 0:
                    progress = (downloaded / total_size * 100) if total_size else 0
                    logger.info(f"Downloaded {downloaded / (1024**2):.0f} MB ({progress:.1f}%)")
                
                # Don't limit download in test mode - we need complete ZIP
            
            # Process ZIP file
            logger.info("Processing ZIP archive...")
            zip_data = b''.join(chunks)
            
            with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zf:
                # Find nested DISPATCH_UNIT_SCADA ZIP file
                nested_zip_files = [f for f in zf.namelist() if 'DISPATCH_UNIT_SCADA' in f and f.endswith('.zip')]
                
                if not nested_zip_files:
                    logger.error("No DISPATCH_UNIT_SCADA zip files found")
                    return False
                
                logger.info(f"Found nested ZIP: {nested_zip_files[0]}")
                
                # Extract the nested ZIP
                with zf.open(nested_zip_files[0]) as nested_file:
                    nested_data = nested_file.read()
                    
                # Open the nested ZIP
                with zipfile.ZipFile(io.BytesIO(nested_data), 'r') as nested_zf:
                    # Find CSV files
                    scada_files = [f for f in nested_zf.namelist() if f.endswith('.CSV')]
                    logger.info(f"Found {len(scada_files)} SCADA CSV files in nested archive")
                    
                    # In test mode, we'll filter data after parsing
                    if test_mode:
                        logger.info("Test mode: Will process only Dec 15 data after parsing")
                    
                    # Process each file
                    for i, filename in enumerate(scada_files):
                            
                        if i % 50 == 0:
                            logger.info(f"Processing file {i+1}/{len(scada_files)}")
                        
                        with nested_zf.open(filename) as f:
                            content = f.read()
                            df = parse_mms_csv(content)
                        
                        if not df.empty:
                            # In test mode, filter to only Dec 15 data
                            if test_mode:
                                test_date = datetime(year, month, 15)
                                df = df[df['settlementdate'].dt.date == test_date.date()]
                                if df.empty:
                                    logger.info(f"No data found for {test_date.date()}")
                                    continue
                                else:
                                    logger.info(f"Found {len(df)} records for {test_date.date()}")
                            
                            # Store 5-minute data
                            scada5_data.append(df)
                            
                            # Calculate 30-minute aggregates (using mean, not sum/2)
                            df_30min = df.copy()
                            df_30min['settlementdate'] = df_30min['settlementdate'].dt.floor('30min')
                            df_30min = df_30min.groupby(['settlementdate', 'duid'])['scadavalue'].mean().reset_index()
                            scada30_data.append(df_30min)
            
            # Combine all data
            if scada5_data:
                logger.info("Combining data...")
                scada5_combined = pd.concat(scada5_data, ignore_index=True)
                scada30_combined = pd.concat(scada30_data, ignore_index=True)
                
                # Remove duplicates
                scada5_combined = scada5_combined.drop_duplicates(['settlementdate', 'duid'])
                scada30_combined = scada30_combined.drop_duplicates(['settlementdate', 'duid'])
                
                # Sort by timestamp
                scada5_combined = scada5_combined.sort_values(['settlementdate', 'duid'])
                scada30_combined = scada30_combined.sort_values(['settlementdate', 'duid'])
                
                logger.info(f"Processed {len(scada5_combined)} 5-minute records")
                logger.info(f"Processed {len(scada30_combined)} 30-minute records")
                
                # Save to temporary parquet files
                temp_scada5_path = Path(data_path) / f"temp_scada5_{year}_{month_str}.parquet"
                temp_scada30_path = Path(data_path) / f"temp_scada30_{year}_{month_str}.parquet"
                
                scada5_combined.to_parquet(temp_scada5_path, engine='pyarrow')
                scada30_combined.to_parquet(temp_scada30_path, engine='pyarrow')
                
                logger.info(f"Saved temporary files:")
                logger.info(f"  - {temp_scada5_path}")
                logger.info(f"  - {temp_scada30_path}")
                
                return True
            else:
                logger.warning("No data extracted")
                return False
                
    except Exception as e:
        logger.error(f"Error processing {year}-{month_str}: {e}")
        return False

def merge_with_existing(data_path, test_mode=False):
    """Merge temporary files with existing parquet files"""
    
    logger.info("Merging with existing data...")
    
    scada5_path = Path(data_path) / "scada5.parquet"
    scada30_path = Path(data_path) / "scada30.parquet"
    
    # Read existing data
    logger.info("Reading existing parquet files...")
    existing_scada5 = pd.read_parquet(scada5_path)
    existing_scada30 = pd.read_parquet(scada30_path)
    
    logger.info(f"Existing scada5: {len(existing_scada5)} records")
    logger.info(f"Existing scada30: {len(existing_scada30)} records")
    
    # Find all temporary files
    temp_files_5 = list(Path(data_path).glob("temp_scada5_*.parquet"))
    temp_files_30 = list(Path(data_path).glob("temp_scada30_*.parquet"))
    
    if not temp_files_5:
        logger.warning("No temporary files to merge")
        return
    
    # Read and combine temporary files
    new_scada5_list = [existing_scada5]
    new_scada30_list = [existing_scada30]
    
    for temp_file in temp_files_5:
        logger.info(f"Reading {temp_file.name}")
        new_scada5_list.append(pd.read_parquet(temp_file))
    
    for temp_file in temp_files_30:
        logger.info(f"Reading {temp_file.name}")
        new_scada30_list.append(pd.read_parquet(temp_file))
    
    # Combine all data
    logger.info("Combining all data...")
    all_scada5 = pd.concat(new_scada5_list, ignore_index=True)
    all_scada30 = pd.concat(new_scada30_list, ignore_index=True)
    
    # Remove duplicates (keep first occurrence)
    logger.info("Removing duplicates...")
    all_scada5 = all_scada5.drop_duplicates(['settlementdate', 'duid'], keep='first')
    all_scada30 = all_scada30.drop_duplicates(['settlementdate', 'duid'], keep='first')
    
    # Sort by timestamp
    logger.info("Sorting data...")
    all_scada5 = all_scada5.sort_values(['settlementdate', 'duid'])
    all_scada30 = all_scada30.sort_values(['settlementdate', 'duid'])
    
    logger.info(f"Final scada5: {len(all_scada5)} records (added {len(all_scada5) - len(existing_scada5)})")
    logger.info(f"Final scada30: {len(all_scada30)} records (added {len(all_scada30) - len(existing_scada30)})")
    
    if not test_mode:
        # Backup existing files
        logger.info("Creating backups...")
        backup_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        scada5_path.rename(Path(data_path) / f"scada5_backup_{backup_time}.parquet")
        scada30_path.rename(Path(data_path) / f"scada30_backup_{backup_time}.parquet")
        
        # Save merged data
        logger.info("Saving merged data...")
        all_scada5.to_parquet(scada5_path, engine='pyarrow')
        all_scada30.to_parquet(scada30_path, engine='pyarrow')
        
        # Clean up temporary files
        logger.info("Cleaning up temporary files...")
        for temp_file in temp_files_5 + temp_files_30:
            temp_file.unlink()
        
        logger.info("✅ Merge complete!")
    else:
        logger.info("Test mode: Not saving merged data")
        
        # Verify the gaps are filled
        logger.info("\nVerifying gap filling...")
        for year, month in [(2020, 12), (2021, 10), (2022, 6)]:
            start_date = datetime(year, month, 1, 0, 30)
            if month == 12:
                end_date = datetime(year + 1, 1, 1, 0, 0) - timedelta(minutes=30)
            else:
                end_date = datetime(year, month + 1, 1, 0, 0) - timedelta(minutes=30)
                
            mask = (all_scada30['settlementdate'] >= start_date) & (all_scada30['settlementdate'] <= end_date)
            month_data = all_scada30[mask]
            
            logger.info(f"{year}-{month:02d}: {len(month_data)} records found")

def main():
    parser = argparse.ArgumentParser(description='Backfill missing SCADA30 data from MMSDM archives')
    parser.add_argument('--data-path', default=DEFAULT_DATA_PATH, help='Path to data directory')
    parser.add_argument('--test', action='store_true', help='Test mode - process limited data')
    parser.add_argument('--month', help='Process specific month (format: YYYY-MM)')
    
    args = parser.parse_args()
    
    # Verify data path exists
    data_path = Path(args.data_path)
    if not data_path.exists():
        logger.error(f"Data path does not exist: {data_path}")
        sys.exit(1)
    
    # Check if required files exist
    scada5_file = data_path / "scada5.parquet"
    scada30_file = data_path / "scada30.parquet"
    
    if not scada5_file.exists() or not scada30_file.exists():
        logger.error(f"Required parquet files not found in {data_path}")
        logger.error(f"  - scada5.parquet: {'exists' if scada5_file.exists() else 'missing'}")
        logger.error(f"  - scada30.parquet: {'exists' if scada30_file.exists() else 'missing'}")
        sys.exit(1)
    
    logger.info(f"Using data path: {data_path}")
    logger.info(f"Test mode: {args.test}")
    
    # Define missing periods
    missing_periods = [
        (2020, 12),  # December 2020
        (2021, 10),  # October 2021
        (2022, 6),   # June 2022
    ]
    
    # If specific month requested, filter
    if args.month:
        try:
            year, month = map(int, args.month.split('-'))
            if (year, month) in missing_periods:
                missing_periods = [(year, month)]
                logger.info(f"Processing only {year}-{month:02d}")
            else:
                logger.error(f"{args.month} is not in the list of missing periods")
                sys.exit(1)
        except ValueError:
            logger.error("Invalid month format. Use YYYY-MM")
            sys.exit(1)
    
    # Process each missing month
    success_count = 0
    for year, month in missing_periods:
        if download_and_process_month(year, month, data_path, test_mode=args.test):
            success_count += 1
        else:
            logger.error(f"Failed to process {year}-{month:02d}")
    
    if success_count > 0:
        # Merge all temporary files with existing data
        merge_with_existing(data_path, test_mode=args.test)
        
        if not args.test:
            logger.info(f"\n✅ Successfully backfilled {success_count} months of data")
        else:
            logger.info(f"\n✅ Test completed successfully for {success_count} months")
    else:
        logger.error("\n❌ No data was successfully processed")

if __name__ == "__main__":
    main()