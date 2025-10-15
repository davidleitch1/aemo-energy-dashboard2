#!/usr/bin/env python3
"""
AEMO Spot Price Updater - Parquet Version
Downloads the latest AEMO dispatch data and updates the historical spot price file.
Runs continuously, checking for updates every 4 minutes.
Includes Twilio price alerts and automatic file cleanup.
Uses parquet format for better performance and compression.
"""

import os
import time
import requests
import zipfile
import pandas as pd
from datetime import datetime
import tempfile
from io import StringIO
import csv

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

try:
    from .twilio_price_alerts import check_price_alerts
except ImportError:
    logger.warning("Twilio price alerts not available - install twilio package")
    def check_price_alerts(data):
        pass

# Configuration from shared config
AEMO_URL = config.aemo_dispatch_url
PARQUET_FILE_PATH = config.spot_hist_file
CHECK_INTERVAL = config.update_interval_minutes * 60  # Convert to seconds


def get_latest_dispatch_file():
    """
    Download the latest dispatch file from AEMO website.
    Returns the CSV content as a string and filename, or None if failed.
    """
    try:
        # Get the main page to find the latest file
        response = requests.get(AEMO_URL, timeout=30)
        response.raise_for_status()
        
        # Look for PUBLIC_DISPATCH files - they follow pattern: PUBLIC_DISPATCH_YYYYMMDDHHMM_*.zip
        import re
        zip_pattern = r'PUBLIC_DISPATCH_\d{12}_\d{14}_LEGACY\.zip'
        matches = re.findall(zip_pattern, response.text)
        
        if not matches:
            logger.error("No dispatch files found on AEMO website")
            return None, None
            
        # Get the latest file (they should be in chronological order)
        latest_file = sorted(matches)[-1]
        file_url = AEMO_URL + latest_file
        
        logger.info(f"Downloading: {latest_file}")
        
        # Download the zip file
        zip_response = requests.get(file_url, timeout=60)
        zip_response.raise_for_status()
        
        # Create a temporary file for the zip
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip_file:
            temp_zip_path = temp_zip_file.name
            temp_zip_file.write(zip_response.content)
            temp_zip_file.flush()
            
            try:
                # Extract the CSV from the zip file
                with zipfile.ZipFile(temp_zip_path, 'r') as zip_file:
                    # Find the CSV file inside (should be similar name but .CSV)
                    csv_name = latest_file.replace('.zip', '.CSV')
                    if csv_name not in zip_file.namelist():
                        # Try without _LEGACY suffix
                        csv_name = latest_file.replace('_LEGACY.zip', '.CSV')
                    
                    if csv_name in zip_file.namelist():
                        csv_content = zip_file.read(csv_name).decode('utf-8')
                        return csv_content, latest_file
                    else:
                        logger.error(f"CSV file not found in zip. Available files: {zip_file.namelist()}")
                        return None, None
                        
            finally:
                # Clean up: delete the temporary zip file
                try:
                    os.unlink(temp_zip_path)
                    logger.debug(f"Deleted temporary file: {temp_zip_path}")
                except OSError as e:
                    logger.warning(f"Could not delete temporary file {temp_zip_path}: {e}")
                    
    except Exception as e:
        logger.error(f"Error downloading dispatch file: {e}")
        return None, None


def parse_dispatch_data(csv_content):
    """
    Parse AEMO dispatch CSV data and extract regional price information.
    Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
    """
    try:
        # AEMO CSV files have variable number of columns per row, so we need to handle this carefully
        lines = csv_content.strip().split('\n')
        
        price_records = []
        
        for line in lines:
            if not line.strip():
                continue
                
            # Split the line by commas, handling quoted fields properly
            reader = csv.reader(StringIO(line))
            try:
                fields = next(reader)
            except:
                continue
            
            # Look for DREGION data rows (marked with 'D' in first column)
            if len(fields) >= 9 and fields[0] == 'D' and fields[1] == 'DREGION':
                try:
                    # Field positions based on actual CSV structure:
                    # 0: D, 1: DREGION, 2: empty, 3: 3, 4: settlement_date, 5: runno, 6: regionid, 7: intervention, 8: rrp
                    settlement_date = pd.to_datetime(fields[4])  # Settlement date field
                    region = fields[6]  # Region field
                    rrp = float(fields[8])  # RRP field
                    
                    price_records.append({
                        'SETTLEMENTDATE': settlement_date,
                        'REGIONID': region,
                        'RRP': rrp
                    })
                except (ValueError, TypeError, IndexError) as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
        
        if not price_records:
            logger.warning("No valid price records extracted")
            return pd.DataFrame()
            
        # Convert to DataFrame and set SETTLEMENTDATE as index
        temp_df = pd.DataFrame(price_records)
        
        # Set SETTLEMENTDATE as index to match the existing parquet file format
        result_df = temp_df.set_index('SETTLEMENTDATE')
        
        settlement_time = result_df.index[0]
        logger.info(f"Extracted {len(result_df)} price records for settlement time: {settlement_time}")
        
        # Show the extracted data for verification
        for settlement_date, row in result_df.iterrows():
            logger.info(f"  Parsed: {row['REGIONID']} = ${row['RRP']:.5f}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error parsing dispatch data: {e}")
        return pd.DataFrame()


def load_historical_data():
    """
    Load the historical spot price data from parquet file.
    Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP or empty DataFrame if file doesn't exist.
    """
    try:
        if os.path.exists(PARQUET_FILE_PATH):
            # Load parquet file
            df = pd.read_parquet(PARQUET_FILE_PATH)
            
            # Debug: check the structure
            logger.info(f"Existing parquet file columns: {list(df.columns)}")
            logger.info(f"Index name: {df.index.name}")
            logger.info(f"Index type: {type(df.index)}")
            
            # The parquet file should already have SETTLEMENTDATE as index from the conversion
            # But let's verify and fix if needed
            if df.index.name != 'SETTLEMENTDATE':
                if 'SETTLEMENTDATE' in df.columns:
                    # Convert SETTLEMENTDATE column to index
                    df = df.set_index('SETTLEMENTDATE')
                    logger.info("Converted SETTLEMENTDATE column to index")
                else:
                    logger.error("Cannot find SETTLEMENTDATE in columns or index")
                    return pd.DataFrame(columns=['REGIONID', 'RRP'])
            
            # Ensure we have the expected columns
            expected_cols = ['REGIONID', 'RRP']
            missing_cols = [col for col in expected_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"Missing expected columns: {missing_cols}")
                return pd.DataFrame(columns=expected_cols)
            
            logger.info(f"Loaded historical data: {len(df)} records, latest: {df.index.max()}")
            return df
        else:
            logger.info("No historical data file found, starting fresh")
            # Create empty DataFrame with SETTLEMENTDATE as index
            return pd.DataFrame(columns=['REGIONID', 'RRP'], 
                              index=pd.DatetimeIndex([], name='SETTLEMENTDATE'))
    except Exception as e:
        logger.error(f"Error loading historical data: {e}")
        return pd.DataFrame(columns=['REGIONID', 'RRP'], 
                          index=pd.DatetimeIndex([], name='SETTLEMENTDATE'))


def save_historical_data(df):
    """
    Save the historical data to parquet file with compression.
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(PARQUET_FILE_PATH), exist_ok=True)
        
        # Save with snappy compression for better performance
        df.to_parquet(PARQUET_FILE_PATH, compression='snappy', index=True)
        logger.info(f"Saved {len(df)} records to {PARQUET_FILE_PATH}")
        
    except Exception as e:
        logger.error(f"Error saving historical data: {e}")


def get_latest_timestamp(df):
    """
    Get the latest settlement date from the historical data.
    """
    if df.empty:
        return pd.Timestamp.min
    return df.index.max()


def update_spot_prices():
    """
    Main function to check for new data and update the historical file.
    """
    logger.info("Checking for new spot price data...")
    
    # Load existing historical data
    historical_df = load_historical_data()
    latest_timestamp = get_latest_timestamp(historical_df)
    
    # Download latest dispatch file
    result = get_latest_dispatch_file()
    if result is None or result[0] is None:
        logger.warning("Failed to download latest dispatch file")
        return False
    
    csv_content, filename = result
    
    # Parse the new data
    new_df = parse_dispatch_data(csv_content)
    
    if new_df.empty:
        logger.warning("No new data to process")
        return False
    
    # Check if new data is more recent than our latest
    new_timestamp = new_df.index.max()
    
    if new_timestamp <= latest_timestamp:
        logger.info("No new prices - latest data is not newer than existing records")
        return False
    
    # Filter for only newer records
    newer_records = new_df[new_df.index > latest_timestamp]
    
    if newer_records.empty:
        logger.info("No new prices - no records newer than existing data")
        return False
    
    # CHECK FOR PRICE ALERTS before logging
    try:
        check_price_alerts(newer_records)
    except Exception as e:
        logger.error(f"Error checking price alerts: {e}")
    
    # Log the new prices found
    settlement_time = newer_records.index[0]
    logger.info(f"New prices found for {settlement_time}:")
    for settlement_date, row in newer_records.iterrows():
        logger.info(f"  {row['REGIONID']}: ${row['RRP']:.2f}")
    
    # Combine historical data with new records
    updated_df = pd.concat([historical_df, newer_records])
    
    # Sort by index (settlement date) and remove duplicate region-timestamp combinations
    # Keep the last occurrence of each unique (settlement_date, region) combination
    updated_df = updated_df.reset_index()
    updated_df = updated_df.drop_duplicates(subset=['SETTLEMENTDATE', 'REGIONID'], keep='last')
    updated_df = updated_df.set_index('SETTLEMENTDATE').sort_index()
    
    # Save updated data
    save_historical_data(updated_df)
    
    logger.info(f"Added {len(newer_records)} new records. Total records: {len(updated_df)}")
    
    return True


def main():
    """
    Main loop - runs continuously checking for updates every 4 minutes.
    """
    logger.info("Starting AEMO spot price updater (Parquet version)...")
    logger.info(f"Check interval: {CHECK_INTERVAL/60} minutes")
    logger.info(f"Data file: {PARQUET_FILE_PATH}")
    
    while True:
        try:
            update_spot_prices()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        
        # Wait for next check
        logger.info(f"Waiting {CHECK_INTERVAL/60} minutes until next check...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()