#!/usr/bin/env python3
"""
AEMO Transmission Flow Historical Backfill
Downloads historical DISPATCHINTERCONNECTORRES data to match generation data timeframe.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime, timedelta
import re
import zipfile
from io import BytesIO
from pathlib import Path
import argparse

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

class TransmissionHistoricalBackfill:
    """
    Historical transmission flow data backfill
    Downloads DISPATCHINTERCONNECTORRES data for specified date range
    """
    
    def __init__(self):
        """Initialize the backfill tool"""
        self.base_url = "https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/"
        self.transmission_output_file = Path(config.transmission_output_file)
        
        # Ensure the directory exists
        self.transmission_output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing transmission data if any
        self.existing_transmission_data = self.load_existing_data()
        
        # Define interconnector mapping (same as update_transmission.py)
        self.interconnector_mapping = self.get_interconnector_mapping()
        
    def get_interconnector_mapping(self):
        """Define mapping of interconnectors to regions and flow directions"""
        return {
            'N-Q-MNSP1': {'from': 'NSW1', 'to': 'QLD1', 'name': 'NSW-QLD'},
            'NSW1-QLD1': {'from': 'NSW1', 'to': 'QLD1', 'name': 'NSW-QLD'},
            'QLD1-NSW1': {'from': 'QLD1', 'to': 'NSW1', 'name': 'QLD-NSW'},
            'V-S-MNSP1': {'from': 'VIC1', 'to': 'SA1', 'name': 'VIC-SA'},
            'VIC1-SA1': {'from': 'VIC1', 'to': 'SA1', 'name': 'VIC-SA'},
            'SA1-VIC1': {'from': 'SA1', 'to': 'VIC1', 'name': 'SA-VIC'},
            'V-T-MNSP1': {'from': 'VIC1', 'to': 'TAS1', 'name': 'VIC-TAS'},
            'T-V-MNSP1': {'from': 'TAS1', 'to': 'VIC1', 'name': 'TAS-VIC'},
            'VIC1-TAS1': {'from': 'VIC1', 'to': 'TAS1', 'name': 'VIC-TAS'},
            'TAS1-VIC1': {'from': 'TAS1', 'to': 'VIC1', 'name': 'TAS-VIC'},
            'NSW1-VIC1': {'from': 'NSW1', 'to': 'VIC1', 'name': 'NSW-VIC'},
            'VIC1-NSW1': {'from': 'VIC1', 'to': 'NSW1', 'name': 'VIC-NSW'},
            'SA1-NSW1': {'from': 'SA1', 'to': 'NSW1', 'name': 'SA-NSW'},
            'NSW1-SA1': {'from': 'NSW1', 'to': 'SA1', 'name': 'NSW-SA'},
            'V-SA': {'from': 'VIC1', 'to': 'SA1', 'name': 'VIC-SA'},
        }
        
    def load_existing_data(self):
        """Load existing transmission data to avoid duplicates"""
        if self.transmission_output_file.exists():
            try:
                df = pd.read_parquet(self.transmission_output_file)
                logger.info(f"Loaded existing transmission data: {len(df)} records")
                return df
            except Exception as e:
                logger.error(f"Error loading existing transmission data: {e}")
                return pd.DataFrame()
        else:
            logger.info("No existing transmission data found")
            return pd.DataFrame()
    
    def get_generation_data_timeframe(self):
        """Get the timeframe of existing generation data"""
        try:
            gen_df = pd.read_parquet(config.gen_output_file)
            start_date = gen_df['settlementdate'].min()
            end_date = gen_df['settlementdate'].max()
            logger.info(f"Generation data timeframe: {start_date} to {end_date}")
            return start_date, end_date
        except Exception as e:
            logger.error(f"Error reading generation data: {e}")
            return None, None
    
    def get_missing_dates(self, start_date, end_date):
        """Determine which dates need transmission data"""
        if self.existing_transmission_data.empty:
            # No existing data, need everything
            missing_dates = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
        else:
            # Find gaps and incomplete data
            # Expected records per day: 6 main interconnectors × 288 intervals = 1728
            # But we'll be conservative and check for days with less than 1000 records
            MIN_RECORDS_PER_DAY = 1000
            
            # Count records per day
            daily_counts = self.existing_transmission_data.groupby(
                self.existing_transmission_data['settlementdate'].dt.date
            ).size()
            
            all_dates = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D').date
            missing_dates = []
            
            for date in all_dates:
                if date not in daily_counts or daily_counts[date] < MIN_RECORDS_PER_DAY:
                    missing_dates.append(date)
                    if date in daily_counts:
                        logger.info(f"Date {date} has only {daily_counts[date]} records (incomplete)")
            
            missing_dates = pd.to_datetime(sorted(missing_dates))
        
        logger.info(f"Need to download transmission data for {len(missing_dates)} days")
        return missing_dates
    
    def construct_archive_url(self, date):
        """Construct URL for historical DISPATCHIS file"""
        date_str = date.strftime('%Y%m%d')
        
        # First try CURRENT reports (more recent data)
        current_url = "http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/"
        file_url = self._find_file_in_directory(current_url, date_str, "CURRENT")
        if file_url:
            return file_url
        
        # Then try ARCHIVE reports (older data) using AEMO's daily ZIP format
        archive_file_url = self._construct_archive_daily_url(date)
        if archive_file_url:
            return archive_file_url
            
        logger.warning(f"Could not find any DISPATCHIS file for {date.date()}")
        return None
    
    def _find_file_in_directory(self, url, date_str, source_type):
        """Helper method to find DISPATCHIS file in a directory"""
        try:
            headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            date_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if (href.endswith('.zip') and 
                    'DISPATCHIS' in href and 
                    date_str in href):
                    date_files.append(href)
            
            if date_files:
                # Take a file from the middle of the day for good data coverage
                chosen_file = sorted(date_files)[len(date_files)//2]
                
                # Construct full URL
                if chosen_file.startswith('/'):
                    file_url = "http://nemweb.com.au" + chosen_file
                else:
                    file_url = url + chosen_file
                    
                logger.info(f"Found {source_type} file for {date_str}: {chosen_file}")
                return file_url
                
        except Exception as e:
            logger.debug(f"Could not access {source_type} directory {url}: {e}")
            
        return None
    
    def _construct_archive_daily_url(self, date):
        """Construct URL for ARCHIVE daily ZIP file using AEMO format"""
        date_str = date.strftime('%Y%m%d')
        # AEMO ARCHIVE format: PUBLIC_DISPATCHIS_YYYYMMDD.zip
        archive_filename = f"PUBLIC_DISPATCHIS_{date_str}.zip"
        archive_url = self.base_url + archive_filename
        
        try:
            # Test if the file exists by making a HEAD request
            headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
            response = requests.head(archive_url, headers=headers, timeout=30)
            if response.status_code == 200:
                logger.info(f"Found ARCHIVE file for {date_str}: {archive_filename}")
                return archive_url
            else:
                logger.debug(f"ARCHIVE file not found (HTTP {response.status_code}): {archive_url}")
                return None
        except Exception as e:
            logger.debug(f"Error checking ARCHIVE file {archive_url}: {e}")
            return None
    
    def download_and_parse_historical_file(self, file_url, target_date):
        """Download and parse historical DISPATCHIS ZIP file"""
        try:
            logger.info(f"Downloading {file_url}")
            headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
            response = requests.get(file_url, headers=headers, timeout=120)
            response.raise_for_status()
            
            # Extract ZIP file (nested structure: daily ZIP contains 5-minute ZIPs)
            with zipfile.ZipFile(BytesIO(response.content)) as daily_zip:
                # Get all 5-minute ZIP files in the daily ZIP
                nested_zip_files = [name for name in daily_zip.namelist() if name.endswith('.zip')]
                
                if not nested_zip_files:
                    logger.error(f"No nested ZIP files found in daily archive. Files found: {daily_zip.namelist()}")
                    return None
                
                # Process all 5-minute ZIP files to get complete daily data
                all_csv_content = []
                logger.info(f"Processing {len(nested_zip_files)} 5-minute intervals for {target_date.date()}")
                
                for zip_name in sorted(nested_zip_files):
                    try:
                        # Extract each 5-minute ZIP and get its CSV content
                        with daily_zip.open(zip_name) as nested_zip_file:
                            with zipfile.ZipFile(nested_zip_file) as minute_zip:
                                # Get CSV files in the 5-minute ZIP
                                csv_files = [name for name in minute_zip.namelist() if name.endswith('.CSV') or name.endswith('.csv')]
                                
                                if csv_files:
                                    csv_filename = csv_files[0]  # Should be only one CSV per 5-minute ZIP
                                    
                                    # Read CSV content from nested ZIP
                                    with minute_zip.open(csv_filename) as csv_file:
                                        csv_content = csv_file.read().decode('utf-8')
                                        all_csv_content.append(csv_content)
                    except Exception as e:
                        logger.warning(f"Error processing 5-minute ZIP {zip_name}: {e}")
                        continue
                
                if not all_csv_content:
                    logger.error("No CSV content extracted from any 5-minute ZIP files")
                    return None
                
                # Combine all CSV content
                csv_content = '\n'.join(all_csv_content)
            
            # Parse CSV content
            lines = csv_content.strip().split('\n')
            
            # Find data lines (start with 'D,DISPATCH,INTERCONNECTORRES')
            data_rows = []
            for line in lines:
                if line.startswith('D,DISPATCH,INTERCONNECTORRES'):
                    # Split CSV line and extract required fields
                    fields = line.split(',')
                    if len(fields) >= 17:  # Ensure we have all required fields
                        settlementdate = fields[4].strip('"')
                        interconnectorid = fields[6].strip('"')
                        meteredmwflow = fields[9].strip('"')
                        mwflow = fields[10].strip('"')
                        mwlosses = fields[11].strip('"')
                        exportlimit = fields[15].strip('"') if len(fields) > 15 else '0'
                        importlimit = fields[16].strip('"') if len(fields) > 16 else '0'
                        
                        # Filter to only include records from our target date
                        try:
                            record_date = pd.to_datetime(settlementdate).date()
                            if record_date != target_date.date():
                                continue
                                
                            meteredmwflow = float(meteredmwflow) if meteredmwflow else 0.0
                            mwflow = float(mwflow) if mwflow else 0.0
                            exportlimit = float(exportlimit) if exportlimit else 0.0
                            importlimit = float(importlimit) if importlimit else 0.0
                            mwlosses = float(mwlosses) if mwlosses else 0.0
                        except ValueError:
                            continue  # Skip invalid numeric values
                            
                        data_rows.append({
                            'settlementdate': settlementdate,
                            'interconnectorid': interconnectorid,
                            'meteredmwflow': meteredmwflow,
                            'mwflow': mwflow,
                            'exportlimit': exportlimit,
                            'importlimit': importlimit,
                            'mwlosses': mwlosses
                        })
            
            if data_rows:
                df = pd.DataFrame(data_rows)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                logger.info(f"Parsed {len(df)} transmission flow records for {target_date}")
                logger.info(f"Interconnectors found: {df['interconnectorid'].unique()}")
                return df
            else:
                logger.warning(f"No valid transmission flow data found for {target_date}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading/parsing file {file_url}: {e}")
            return None
    
    def backfill_historical_data(self, start_date=None, end_date=None, max_days=None):
        """Main method to backfill historical transmission data"""
        
        # Get generation data timeframe if not specified
        if start_date is None or end_date is None:
            gen_start, gen_end = self.get_generation_data_timeframe()
            if gen_start is None:
                logger.error("Could not determine generation data timeframe")
                return False
            start_date = start_date or gen_start
            end_date = end_date or gen_end
        
        # Get missing dates
        missing_dates = self.get_missing_dates(start_date, end_date)
        
        if max_days and len(missing_dates) > max_days:
            missing_dates = missing_dates[:max_days]
            logger.info(f"Limited to {max_days} days for this run")
        
        if len(missing_dates) == 0:
            logger.info("No missing transmission data to backfill")
            return True
        
        all_historical_data = []
        successful_downloads = 0
        
        for i, date in enumerate(missing_dates):
            logger.info(f"Processing {date.date()} ({i+1}/{len(missing_dates)})")
            
            # Find archive file URL for this date
            file_url = self.construct_archive_url(date)
            
            if file_url is None:
                logger.warning(f"Could not find transmission archive file for {date.date()}")
                continue
            
            # Download and parse the file
            daily_data = self.download_and_parse_historical_file(file_url, date)
            
            if daily_data is not None and not daily_data.empty:
                all_historical_data.append(daily_data)
                successful_downloads += 1
                logger.info(f"Successfully downloaded transmission data for {date.date()}")
            else:
                logger.warning(f"No transmission data obtained for {date.date()}")
            
            # Rate limiting - be respectful to AEMO servers
            time.sleep(2)
        
        # Combine and save all historical data
        if all_historical_data:
            combined_data = pd.concat(all_historical_data, ignore_index=True)
            
            # Merge with existing data
            if not self.existing_transmission_data.empty:
                all_data = pd.concat([self.existing_transmission_data, combined_data], ignore_index=True)
                # Remove duplicates based on settlementdate and interconnectorid
                all_data = all_data.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
            else:
                all_data = combined_data
            
            # Sort by settlement date
            all_data = all_data.sort_values('settlementdate').reset_index(drop=True)
            
            # Save to parquet file
            all_data.to_parquet(self.transmission_output_file, compression='snappy', index=False)
            
            # Get file size for logging
            file_size = self.transmission_output_file.stat().st_size / (1024*1024)
            logger.info(f"Saved transmission data with {len(all_data)} total records ({file_size:.2f}MB)")
            logger.info(f"Date range: {all_data['settlementdate'].min()} to {all_data['settlementdate'].max()}")
            logger.info(f"Successfully downloaded {successful_downloads}/{len(missing_dates)} days")
            
            return True
        else:
            logger.error("No historical transmission data was successfully downloaded")
            return False


def main():
    """Main function for historical transmission data backfill"""
    parser = argparse.ArgumentParser(description='Backfill historical transmission flow data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--max-days', type=int, help='Maximum number of days to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be downloaded without downloading')
    
    args = parser.parse_args()
    
    logger.info("AEMO Transmission Flow Historical Backfill starting...")
    
    # Create backfill instance
    backfill = TransmissionHistoricalBackfill()
    
    # Parse dates if provided
    start_date = pd.to_datetime(args.start_date) if args.start_date else None
    end_date = pd.to_datetime(args.end_date) if args.end_date else None
    
    if args.dry_run:
        # Just show what would be downloaded
        if start_date is None or end_date is None:
            gen_start, gen_end = backfill.get_generation_data_timeframe()
            start_date = start_date or gen_start
            end_date = end_date or gen_end
        
        missing_dates = backfill.get_missing_dates(start_date, end_date)
        if args.max_days and len(missing_dates) > args.max_days:
            missing_dates = missing_dates[:args.max_days]
            
        logger.info(f"Would download transmission data for {len(missing_dates)} days:")
        for date in missing_dates:
            logger.info(f"  - {date.date()}")
    else:
        # Perform actual backfill
        success = backfill.backfill_historical_data(
            start_date=start_date,
            end_date=end_date,
            max_days=args.max_days
        )
        
        if success:
            logger.info("Historical transmission data backfill completed successfully")
        else:
            logger.error("Historical transmission data backfill failed")


if __name__ == "__main__":
    main()