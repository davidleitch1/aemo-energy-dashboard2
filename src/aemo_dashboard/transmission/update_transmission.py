#!/usr/bin/env python3
"""
AEMO Transmission Flow Data Updater
Downloads AEMO DISPATCHINTERCONNECTORRES data and stores in efficient parquet format.
Integrates with shared configuration and logging systems.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime
import re
import zipfile
from io import BytesIO
from pathlib import Path

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

class TransmissionDataUpdater:
    """
    AEMO Transmission Flow Data Updater
    Downloads and processes DISPATCHINTERCONNECTORRES data from AEMO website
    """
    
    def __init__(self):
        """Initialize the updater with configuration from shared config"""
        self.base_url = "http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/"
        self.transmission_output_file = Path(config.transmission_output_file)
        self.transmission_output_backup = self.transmission_output_file.with_suffix('.pkl')  # For migration
        self.last_processed_file = None
        self.update_interval = config.update_interval_minutes * 60  # Convert to seconds
        
        # Ensure the directory exists
        self.transmission_output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize or load existing DataFrame
        self.transmission_output = self.load_or_create_dataframe()
        
        # Define interconnector mapping
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
            'VIC1-TAS1': {'from': 'VIC1', 'to': 'TAS1', 'name': 'VIC-TAS'},
            'TAS1-VIC1': {'from': 'TAS1', 'to': 'VIC1', 'name': 'TAS-VIC'},
            'NSW1-VIC1': {'from': 'NSW1', 'to': 'VIC1', 'name': 'NSW-VIC'},
            'VIC1-NSW1': {'from': 'VIC1', 'to': 'NSW1', 'name': 'VIC-NSW'},
            'SA1-NSW1': {'from': 'SA1', 'to': 'NSW1', 'name': 'SA-NSW'},
            'NSW1-SA1': {'from': 'NSW1', 'to': 'SA1', 'name': 'NSW-SA'},
        }
        
    def load_or_create_dataframe(self):
        """Load existing transmission_output DataFrame or create new one"""
        # Try parquet first
        if self.transmission_output_file.exists():
            try:
                df = pd.read_parquet(self.transmission_output_file)
                logger.info(f"Loaded existing transmission_flows.parquet with {len(df)} records")
                return df
            except Exception as e:
                logger.error(f"Error loading parquet file: {e}")
        
        # Fall back to pickle file if parquet doesn't exist
        if self.transmission_output_backup.exists():
            try:
                df = pd.read_pickle(self.transmission_output_backup)
                logger.info(f"Loaded existing transmission_flows.pkl with {len(df)} records")
                logger.info("Converting to parquet format...")
                
                # Save as parquet and remove pickle
                df.to_parquet(self.transmission_output_file, compression='snappy', index=False)
                
                # Get file sizes for comparison
                pkl_size = self.transmission_output_backup.stat().st_size / (1024*1024)
                parquet_size = self.transmission_output_file.stat().st_size / (1024*1024)
                savings = ((pkl_size - parquet_size) / pkl_size) * 100
                
                logger.info(f"Migration complete: {pkl_size:.2f}MB -> {parquet_size:.2f}MB ({savings:.1f}% savings)")
                
                # Keep backup for safety
                backup_name = self.transmission_output_backup.with_name(self.transmission_output_backup.stem + '_backup.pkl')
                self.transmission_output_backup.rename(backup_name)
                logger.info(f"Backup created: {backup_name}")
                
                return df
            except Exception as e:
                logger.error(f"Error loading existing pickle file: {e}")
                
        # Create new DataFrame with proper structure
        df = pd.DataFrame(columns=['settlementdate', 'interconnectorid', 'meteredmwflow', 'mwflow', 'exportlimit', 'importlimit', 'mwlosses'])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        logger.info("Created new transmission_output DataFrame")
        return df
    
    def get_latest_file_url(self):
        """Get the URL of the most recent DISPATCHIS file"""
        try:
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all ZIP file links
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'DISPATCHIS' in href:
                    zip_files.append(href)
            
            if not zip_files:
                logger.warning("No DISPATCHIS ZIP files found")
                return None
                
            # Sort to get the most recent (AEMO files have timestamp in filename)
            zip_files.sort(reverse=True)
            latest_file = zip_files[0]
            
            # Extract timestamp from filename for logging
            timestamp_match = re.search(r'(\d{12})', latest_file)
            if timestamp_match:
                timestamp = timestamp_match.group(1)
                logger.info(f"Latest file: {latest_file} (timestamp: {timestamp})")
            
            # Construct proper URL - href already contains the full path from root
            if latest_file.startswith('/'):
                file_url = "http://nemweb.com.au" + latest_file
            else:
                file_url = self.base_url + latest_file
                
            return file_url
            
        except Exception as e:
            logger.error(f"Error getting latest file URL: {e}")
            return None
    
    def download_and_parse_file(self, file_url):
        """Download and parse DISPATCHIS ZIP file"""
        try:
            response = requests.get(file_url, timeout=60)
            response.raise_for_status()
            
            # Extract ZIP file
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                # Get the first (and usually only) CSV file in the ZIP
                csv_files = [name for name in zip_file.namelist() if name.endswith('.CSV')]
                
                if not csv_files:
                    logger.error("No CSV files found in ZIP archive")
                    return None
                    
                csv_filename = csv_files[0]
                logger.info(f"Extracting and parsing: {csv_filename}")
                
                # Read CSV content from ZIP
                with zip_file.open(csv_filename) as csv_file:
                    csv_content = csv_file.read().decode('utf-8')
            
            # Parse CSV content
            lines = csv_content.strip().split('\n')
            
            # Find data lines (start with 'D,DISPATCH,INTERCONNECTORRES')
            data_rows = []
            for line in lines:
                if line.startswith('D,DISPATCH,INTERCONNECTORRES'):
                    # Split CSV line and extract required fields
                    fields = line.split(',')
                    if len(fields) >= 17:  # Ensure we have all required fields (including export/import limits)
                        settlementdate = fields[4].strip('"')
                        interconnectorid = fields[6].strip('"')
                        meteredmwflow = fields[9].strip('"')
                        mwflow = fields[10].strip('"')
                        mwlosses = fields[11].strip('"')
                        exportlimit = fields[15].strip('"') if len(fields) > 15 else '0'
                        importlimit = fields[16].strip('"') if len(fields) > 16 else '0'
                        
                        try:
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
                logger.info(f"Parsed {len(df)} transmission flow records from {file_url}")
                logger.info(f"Interconnectors found: {df['interconnectorid'].unique()}")
                return df
            else:
                logger.warning("No valid transmission flow data rows found in file")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading/parsing file {file_url}: {e}")
            return None
    
    def is_new_data(self, new_df):
        """Check if the new data contains records not already in transmission_output"""
        if self.transmission_output.empty:
            return True
            
        if new_df is None or new_df.empty:
            return False
            
        # Get the latest timestamp in existing data
        latest_existing = self.transmission_output['settlementdate'].max()
        
        # Check if new data has more recent timestamps
        latest_new = new_df['settlementdate'].max()
        
        is_new = latest_new > latest_existing
        logger.info(f"Latest existing: {latest_existing}, Latest new: {latest_new}, Is new: {is_new}")
        
        return is_new
    
    def add_new_data(self, new_df):
        """Add new data to transmission_output DataFrame and save"""
        if new_df is None or new_df.empty:
            return
            
        try:
            # If transmission_output is empty, just use the new data
            if self.transmission_output.empty:
                self.transmission_output = new_df.copy()
            else:
                # Filter out any duplicate records based on settlementdate and interconnectorid
                latest_existing = self.transmission_output['settlementdate'].max()
                truly_new = new_df[new_df['settlementdate'] > latest_existing]
                
                if not truly_new.empty:
                    self.transmission_output = pd.concat([self.transmission_output, truly_new], ignore_index=True)
                    logger.info(f"Added {len(truly_new)} new transmission flow records")
                else:
                    logger.info("No truly new transmission flow records to add")
                    return
            
            # Sort by settlement date
            self.transmission_output = self.transmission_output.sort_values('settlementdate').reset_index(drop=True)
            
            # Save to parquet file with compression
            self.transmission_output.to_parquet(self.transmission_output_file, compression='snappy', index=False)
            
            # Get file size for logging
            file_size = self.transmission_output_file.stat().st_size / (1024*1024)
            logger.info(f"Saved transmission_flows.parquet with {len(self.transmission_output)} total records ({file_size:.2f}MB)")
            
        except Exception as e:
            logger.error(f"Error adding new transmission flow data: {e}")
    
    def get_data_summary(self):
        """Get summary statistics of the current transmission flow data"""
        if self.transmission_output.empty:
            return "No transmission flow data available"
        
        total_records = len(self.transmission_output)
        date_range = f"{self.transmission_output['settlementdate'].min()} to {self.transmission_output['settlementdate'].max()}"
        unique_interconnectors = self.transmission_output['interconnectorid'].nunique()
        file_size = self.transmission_output_file.stat().st_size / (1024*1024) if self.transmission_output_file.exists() else 0
        
        return f"Records: {total_records:,}, Interconnectors: {unique_interconnectors}, Date range: {date_range}, File size: {file_size:.2f}MB"
    
    def run_once(self):
        """Run a single update cycle"""
        try:
            # Get latest file URL
            latest_url = self.get_latest_file_url()
            
            if latest_url and latest_url != self.last_processed_file:
                logger.info(f"Processing new transmission flow file: {latest_url}")
                
                # Download and parse
                new_data = self.download_and_parse_file(latest_url)
                
                # Check if it's truly new data
                if self.is_new_data(new_data):
                    self.add_new_data(new_data)
                    self.last_processed_file = latest_url
                    logger.info(f"Updated transmission flow data summary: {self.get_data_summary()}")
                    return True
                else:
                    logger.info("File exists but contains no new transmission flow data")
                    return False
                    
            else:
                logger.info("No new transmission flow files found")
                return False
                
        except Exception as e:
            logger.error(f"Error in transmission flow update cycle: {e}")
            return False
    
    def run_monitor(self):
        """Main monitoring loop"""
        logger.info("Starting AEMO transmission flow data monitoring...")
        logger.info(f"Checking every {self.update_interval/60:.1f} minutes for new files at {self.base_url}")
        logger.info(f"Transmission flow data summary: {self.get_data_summary()}")
        
        while True:
            try:
                self.run_once()
                
            except Exception as e:
                logger.error(f"Error in transmission flow monitoring loop: {e}")
            
            # Wait for next update interval
            logger.info(f"Waiting {self.update_interval/60:.1f} minutes for next transmission flow check...")
            time.sleep(self.update_interval)


def migrate_existing_pickle_to_parquet():
    """
    Standalone function to migrate existing pickle file to parquet
    """
    pkl_file = Path(config.transmission_output_file).with_suffix('.pkl')
    parquet_file = Path(config.transmission_output_file)
    
    if not pkl_file.exists():
        logger.info(f"No transmission flow pickle file found at: {pkl_file}")
        return False
    
    if parquet_file.exists():
        logger.info(f"Transmission flow parquet file already exists at: {parquet_file}")
        return False
    
    try:
        logger.info(f"Loading transmission flow pickle file: {pkl_file}")
        df = pd.read_pickle(pkl_file)
        
        pkl_size = pkl_file.stat().st_size / (1024*1024)
        logger.info(f"Loaded {len(df)} transmission flow records ({pkl_size:.2f}MB)")
        
        logger.info(f"Saving as parquet: {parquet_file}")
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_file, compression='snappy', index=False)
        
        parquet_size = parquet_file.stat().st_size / (1024*1024)
        savings = ((pkl_size - parquet_size) / pkl_size) * 100
        
        logger.info(f"Transmission flow migration complete: {pkl_size:.2f}MB -> {parquet_size:.2f}MB ({savings:.1f}% savings)")
        
        # Create backup
        backup_file = pkl_file.with_name(pkl_file.stem + '_backup.pkl')
        pkl_file.rename(backup_file)
        logger.info(f"Original transmission flow file backed up as: {backup_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Transmission flow migration failed: {e}")
        return False


def main():
    """Main function to run the transmission flow data updater"""
    logger.info("AEMO Transmission Flow Data Updater starting...")
    
    # Create updater instance
    updater = TransmissionDataUpdater()
    
    try:
        updater.run_monitor()
    except KeyboardInterrupt:
        logger.info("Transmission flow monitoring stopped by user")
    except Exception as e:
        logger.error(f"Transmission flow monitor crashed: {e}")


if __name__ == "__main__":
    main()