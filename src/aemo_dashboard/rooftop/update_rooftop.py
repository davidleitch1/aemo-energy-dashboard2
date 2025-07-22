#!/usr/bin/env python3
"""
AEMO Rooftop Solar Data Updater
Downloads distributed PV data and converts 30-minute intervals to 5-minute pseudo-data
"""

import pandas as pd
import numpy as np
import requests
from pathlib import Path
import logging
from datetime import datetime, timedelta
import time
from io import StringIO

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

class RooftopDataUpdater:
    """
    Downloads and processes AEMO distributed PV (rooftop solar) data
    Converts 30-minute intervals to 5-minute pseudo-data using moving averages
    """
    
    def __init__(self):
        """Initialize the updater with configuration"""
        self.base_url = "http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"
        self.rooftop_output_file = config.data_dir / 'rooftop_solar.parquet'
        self.update_interval = 15 * 60  # 15 minutes in seconds
        
        # Load existing data or create new DataFrame
        self.rooftop_data = self.load_existing_data()
        
        # Region mapping
        self.regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        
    def load_existing_data(self):
        """Load existing rooftop data from parquet file if it exists"""
        if self.rooftop_output_file.exists():
            try:
                df = pd.read_parquet(self.rooftop_output_file)
                logger.info(f"Loaded {len(df)} existing rooftop solar records")
                return df
            except Exception as e:
                logger.error(f"Error loading existing rooftop data: {e}")
                return pd.DataFrame()
        else:
            logger.info("No existing rooftop data found, starting fresh")
            return pd.DataFrame()
    
    def get_latest_rooftop_pv_files(self):
        """Get list of the most recent rooftop PV files from AEMO"""
        try:
            import requests
            from bs4 import BeautifulSoup
            import re
            
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all ZIP file links
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'ROOFTOP_PV_ACTUAL_MEASUREMENT' in href:
                    zip_files.append(href)
            
            if not zip_files:
                logger.warning("No rooftop PV ZIP files found")
                return []
            
            # Sort to get the most recent files (AEMO files have timestamp in filename)
            zip_files.sort(reverse=True)
            
            # Get last few files to ensure we have recent data
            recent_files = zip_files[:5]  # Last 5 files
            
            logger.info(f"Found {len(recent_files)} recent rooftop PV files")
            return recent_files
            
        except Exception as e:
            logger.error(f"Error getting rooftop PV file list: {e}")
            return []
    
    def download_rooftop_pv_zip(self, filename):
        """Download a specific rooftop PV ZIP file"""
        try:
            # Construct proper URL - filename already starts with / for full path
            if filename.startswith('/'):
                file_url = "http://nemweb.com.au" + filename
            else:
                file_url = self.base_url + filename
                
            logger.info(f"Downloading rooftop PV file: {filename}")
            logger.info(f"Full URL: {file_url}")
            
            response = requests.get(file_url, timeout=30)
            response.raise_for_status()
            
            return response.content
            
        except Exception as e:
            logger.error(f"Failed to download rooftop PV file {filename}: {e}")
            return None
    
    def parse_rooftop_pv_zip(self, zip_content):
        """Parse the rooftop PV ZIP content into DataFrame"""
        try:
            import zipfile
            from io import BytesIO
            
            # Extract ZIP file
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                # List all files in the ZIP
                all_files = zip_file.namelist()
                logger.info(f"ZIP contents: {all_files}")
                
                # Look for CSV files (might have different case)
                csv_files = [name for name in all_files if name.lower().endswith('.csv')]
                
                if not csv_files:
                    logger.error(f"No CSV files found in rooftop PV ZIP archive. Contents: {all_files}")
                    return pd.DataFrame()
                    
                csv_filename = csv_files[0]
                logger.info(f"Extracting and parsing: {csv_filename}")
                
                # Read CSV content from ZIP
                with zip_file.open(csv_filename) as csv_file:
                    csv_content = csv_file.read().decode('utf-8')
            
            # Parse CSV content
            lines = csv_content.strip().split('\n')
            logger.info(f"CSV has {len(lines)} lines")
            logger.info(f"First few lines: {lines[:5]}")
            
            # Find data lines (start with 'D,ROOFTOP,ACTUAL')
            data_rows = []
            for line in lines:
                if line.startswith('D,ROOFTOP,ACTUAL'):
                    # Split CSV line and extract required fields
                    fields = line.split(',')
                    if len(fields) >= 8:  # Ensure we have all required fields
                        # Field mapping based on AEMO format:
                        # [0]D, [1]ROOFTOP, [2]ACTUAL, [3]2, [4]INTERVAL_DATETIME, [5]REGIONID, [6]POWER, [7]QI, [8]TYPE, [9]LASTCHANGED
                        interval_datetime = fields[4].strip('"')
                        regionid = fields[5].strip('"')
                        powermw = fields[6].strip('"')
                        qi = fields[7].strip('"')
                        
                        try:
                            powermw = float(powermw) if powermw else 0.0
                        except ValueError:
                            continue  # Skip invalid numeric values
                            
                        data_rows.append({
                            'settlementdate': interval_datetime,
                            'regionid': regionid,
                            'powermw': powermw
                        })
            
            if data_rows:
                df = pd.DataFrame(data_rows)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                
                # Pivot to get regions as columns
                pivot_df = df.pivot_table(
                    index='settlementdate', 
                    columns='regionid', 
                    values='powermw',
                    aggfunc='first'
                ).fillna(0)
                
                logger.info(f"Parsed {len(pivot_df)} rooftop PV records")
                logger.info(f"Date range: {pivot_df.index.min()} to {pivot_df.index.max()}")
                logger.info(f"All regions: {list(pivot_df.columns)}")
                
                # Reset index to make settlementdate a column
                pivot_df = pivot_df.reset_index()
                
                return pivot_df
            else:
                logger.warning("No valid rooftop PV data rows found in ZIP file")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error parsing rooftop PV ZIP: {e}")
            return pd.DataFrame()
    
    def convert_30min_to_5min(self, df_30min):
        """
        Convert 30-minute data to 5-minute intervals using 6-period moving average
        
        Algorithm:
        - Each 30-min period creates 6 x 5-min periods
        - Initial: all 6 periods = hh1/6
        - Transition: weighted average between consecutive 30-min values
        - End: extrapolate last calculated value
        """
        if df_30min.empty:
            return pd.DataFrame()
        
        # Sort by time
        df_30min = df_30min.sort_values('settlementdate')
        
        # Create list to store 5-minute data
        five_min_records = []
        
        # Get available regions from columns (excluding settlementdate)
        available_regions = [col for col in df_30min.columns if col != 'settlementdate']
        
        # Process each 30-minute record
        for i in range(len(df_30min)):
            current_row = df_30min.iloc[i]
            current_time = current_row['settlementdate']
            
            # Get next row if available
            if i < len(df_30min) - 1:
                next_row = df_30min.iloc[i + 1]
                has_next = True
            else:
                has_next = False
            
            # Generate 6 x 5-minute periods for this 30-minute period
            for j in range(6):
                five_min_time = current_time + timedelta(minutes=j*5)
                
                record = {'settlementdate': five_min_time}
                
                # Calculate values for each region
                for region in available_regions:
                    current_value = current_row[region]
                    
                    if pd.isna(current_value):
                        current_value = 0
                    
                    if has_next:
                        next_value = next_row[region]
                        if pd.isna(next_value):
                            next_value = 0
                        
                        # Weighted average: (6-j)*current + j*next / 6
                        value = ((6 - j) * current_value + j * next_value) / 6
                    else:
                        # No next value - use current value for all periods
                        value = current_value
                    
                    record[region] = value
                
                five_min_records.append(record)
        
        # Create DataFrame from records
        df_5min = pd.DataFrame(five_min_records)
        
        logger.info(f"Converted {len(df_30min)} 30-min records to {len(df_5min)} 5-min records")
        
        return df_5min
    
    def update_rooftop_data(self):
        """Main update function - download new data and update parquet file"""
        try:
            # Get list of recent rooftop PV files
            recent_files = self.get_latest_rooftop_pv_files()
            
            if not recent_files:
                logger.warning("No rooftop PV files available")
                return False
            
            # Download and process the most recent files
            new_data_list = []
            for filename in recent_files[:3]:  # Process last 3 files to get recent data
                zip_content = self.download_rooftop_pv_zip(filename)
                
                if zip_content is None:
                    continue
                
                # Parse ZIP file
                df_30min = self.parse_rooftop_pv_zip(zip_content)
                
                if df_30min.empty:
                    continue
                
                # Convert to 5-minute intervals
                df_5min = self.convert_30min_to_5min(df_30min)
                
                if not df_5min.empty:
                    new_data_list.append(df_5min)
            
            if not new_data_list:
                logger.warning("No valid rooftop PV data processed")
                return False
            
            # Combine all new data
            all_new_data = pd.concat(new_data_list, ignore_index=True)
            all_new_data = all_new_data.sort_values('settlementdate').drop_duplicates(subset=['settlementdate'])
            
            # Merge with existing data
            if not self.rooftop_data.empty:
                # Get the latest timestamp in existing data
                latest_existing = pd.to_datetime(self.rooftop_data['settlementdate']).max()
                
                # Only keep new records
                all_new_data['settlementdate'] = pd.to_datetime(all_new_data['settlementdate'])
                new_records = all_new_data[all_new_data['settlementdate'] > latest_existing]
                
                if not new_records.empty:
                    logger.info(f"Adding {len(new_records)} new records to existing data")
                    self.rooftop_data = pd.concat([self.rooftop_data, new_records], ignore_index=True)
                else:
                    logger.info("No new records to add")
            else:
                # First time - use all data
                self.rooftop_data = all_new_data
                logger.info(f"Initialized rooftop data with {len(all_new_data)} records")
            
            # Sort by time
            self.rooftop_data = self.rooftop_data.sort_values('settlementdate')
            
            # Save to parquet
            self.save_rooftop_data()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating rooftop data: {e}")
            return False
    
    def save_rooftop_data(self):
        """Save rooftop data to parquet file"""
        try:
            # Ensure data directory exists
            self.rooftop_output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to parquet
            self.rooftop_data.to_parquet(self.rooftop_output_file, index=False)
            logger.info(f"Saved {len(self.rooftop_data)} records to {self.rooftop_output_file}")
            
        except Exception as e:
            logger.error(f"Error saving rooftop data: {e}")
    
    def run_continuous_update(self):
        """Run continuous update loop every 15 minutes"""
        logger.info(f"Starting rooftop solar data updater (15-minute intervals)")
        
        while True:
            try:
                # Run update
                success = self.update_rooftop_data()
                
                if success:
                    logger.info("Rooftop data update completed successfully")
                else:
                    logger.warning("Rooftop data update completed with warnings")
                
                # Wait for next update
                logger.info(f"Waiting {self.update_interval/60:.0f} minutes until next update...")
                time.sleep(self.update_interval)
                
            except KeyboardInterrupt:
                logger.info("Rooftop updater stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in update loop: {e}")
                logger.info(f"Retrying in {self.update_interval/60:.0f} minutes...")
                time.sleep(self.update_interval)


    def backfill_historical_data(self, start_date, end_date):
        """Backfill historical rooftop solar data from AEMO archives"""
        try:
            from datetime import datetime, timedelta
            import requests
            import zipfile
            from io import BytesIO
            
            logger.info(f"Starting historical backfill from {start_date} to {end_date}")
            
            # Archive files are weekly starting on Thursdays
            # Map the date ranges to archive file dates
            archive_files = [
                "PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20250619.zip",  # June 19 - June 26
                "PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20250626.zip",  # June 26 - July 3  
                "PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20250703.zip",  # July 3 - July 10
            ]
            
            base_archive_url = "http://nemweb.com.au/Reports/Archive/ROOFTOP_PV/ACTUAL/"
            all_historical_data = []
            
            for archive_file in archive_files:
                logger.info(f"Processing archive: {archive_file}")
                archive_url = base_archive_url + archive_file
                
                try:
                    # Download weekly archive
                    response = requests.get(archive_url, timeout=60)
                    response.raise_for_status()
                    
                    # Process nested ZIP structure
                    with zipfile.ZipFile(BytesIO(response.content)) as outer_zip:
                        nested_files = outer_zip.namelist()
                        logger.info(f"Archive contains {len(nested_files)} files")
                        
                        for nested_file in nested_files:
                            try:
                                # Extract nested ZIP
                                nested_zip_content = outer_zip.read(nested_file)
                                
                                with zipfile.ZipFile(BytesIO(nested_zip_content)) as nested_zip:
                                    csv_files = [name for name in nested_zip.namelist() if name.lower().endswith('.csv')]
                                    
                                    if csv_files:
                                        # Parse the CSV content using existing parser
                                        with nested_zip.open(csv_files[0]) as csv_file:
                                            csv_content = csv_file.read().decode('utf-8')
                                        
                                        # Use existing parsing logic but for single files
                                        lines = csv_content.strip().split('\n')
                                        data_rows = []
                                        
                                        for line in lines:
                                            if line.startswith('D,ROOFTOP,ACTUAL'):
                                                fields = line.split(',')
                                                if len(fields) >= 8:
                                                    interval_datetime = fields[4].strip('"')
                                                    regionid = fields[5].strip('"')
                                                    powermw = fields[6].strip('"')
                                                    
                                                    try:
                                                        powermw = float(powermw) if powermw else 0.0
                                                    except ValueError:
                                                        continue
                                                    
                                                    data_rows.append({
                                                        'settlementdate': interval_datetime,
                                                        'regionid': regionid,
                                                        'powermw': powermw
                                                    })
                                        
                                        if data_rows:
                                            df = pd.DataFrame(data_rows)
                                            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                                            
                                            # Pivot to get regions as columns
                                            pivot_df = df.pivot_table(
                                                index='settlementdate',
                                                columns='regionid',
                                                values='powermw',
                                                aggfunc='first'
                                            ).fillna(0)
                                            
                                            pivot_df = pivot_df.reset_index()
                                            all_historical_data.append(pivot_df)
                                            
                            except Exception as e:
                                logger.warning(f"Error processing nested file {nested_file}: {e}")
                                continue
                                
                except Exception as e:
                    logger.error(f"Error downloading archive {archive_file}: {e}")
                    continue
            
            if all_historical_data:
                # Combine all historical data
                logger.info(f"Combining {len(all_historical_data)} data files")
                combined_df = pd.concat(all_historical_data, ignore_index=True)
                combined_df = combined_df.sort_values('settlementdate').drop_duplicates(subset=['settlementdate'])
                
                # Filter to requested date range
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                combined_df = combined_df[
                    (combined_df['settlementdate'] >= start_dt) & 
                    (combined_df['settlementdate'] <= end_dt)
                ]
                
                logger.info(f"Historical data: {len(combined_df)} records from {combined_df['settlementdate'].min()} to {combined_df['settlementdate'].max()}")
                
                # Convert to 5-minute intervals
                df_5min = self.convert_30min_to_5min(combined_df)
                
                if not df_5min.empty:
                    # Replace existing data with historical data
                    self.rooftop_data = df_5min.sort_values('settlementdate')
                    self.save_rooftop_data()
                    
                    logger.info(f"Successfully backfilled {len(df_5min)} historical rooftop records")
                    return True
                    
            logger.warning("No historical data was successfully processed")
            return False
            
        except Exception as e:
            logger.error(f"Error in historical backfill: {e}")
            return False


def main():
    """Main entry point for running the rooftop updater"""
    updater = RooftopDataUpdater()
    updater.run_continuous_update()


def backfill_historical():
    """Standalone function to backfill historical data"""
    updater = RooftopDataUpdater()
    
    # Backfill from generation data start date
    start_date = "2025-06-18"
    end_date = "2025-07-12"
    
    success = updater.backfill_historical_data(start_date, end_date)
    if success:
        print(f"Historical backfill completed successfully from {start_date} to {end_date}")
    else:
        print("Historical backfill failed")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        backfill_historical()
    else:
        main()