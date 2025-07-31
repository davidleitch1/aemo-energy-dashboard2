#!/usr/bin/env python3
"""Test extraction of DISPATCH_UNIT_SCADA data from MMSDM archives"""

import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

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
            df['settlementdate'] = pd.to_datetime(df['SETTLEMENTDATE'], format='%Y/%m/%d %H:%M:%S')
        if 'DUID' in df.columns:
            df['duid'] = df['DUID'].str.strip()
        if 'SCADAVALUE' in df.columns:
            df['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
            
        return df[['settlementdate', 'duid', 'scadavalue']]
    
    return pd.DataFrame()

def test_single_day_extraction():
    """Test extracting a single day from MMSDM archive"""
    
    # Test with Dec 15, 2020
    test_date = datetime(2020, 12, 15)
    year = test_date.year
    month = f"{test_date.month:02d}"
    day_str = test_date.strftime("%Y%m%d")
    
    url = f"https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}.zip"
    
    logger.info(f"Testing extraction for {test_date.date()}")
    logger.info(f"Archive URL: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        # We'll use streaming to avoid downloading the entire file
        logger.info("Starting streaming download...")
        
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()
            
            # Read the ZIP file in chunks
            chunks = []
            downloaded_mb = 0
            found_scada_files = []
            
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                chunks.append(chunk)
                downloaded_mb += 1
                
                if downloaded_mb % 100 == 0:
                    logger.info(f"Downloaded {downloaded_mb} MB...")
                
                # Try to read ZIP central directory periodically
                if downloaded_mb % 500 == 0 or downloaded_mb > 4000:
                    combined = b''.join(chunks)
                    
                    try:
                        with zipfile.ZipFile(io.BytesIO(combined), 'r') as zf:
                            # Look for DISPATCH_UNIT_SCADA files for our test date
                            all_files = zf.namelist()
                            scada_pattern = re.compile(f'.*DISPATCH.*UNIT.*SCADA.*{day_str}.*\\.CSV', re.IGNORECASE)
                            
                            day_files = [f for f in all_files if scada_pattern.match(f)]
                            
                            if day_files and not found_scada_files:
                                found_scada_files = day_files
                                logger.info(f"Found {len(day_files)} SCADA files for {test_date.date()}:")
                                for f in day_files[:3]:
                                    logger.info(f"  - {f}")
                                
                                # Try to extract one file
                                test_file = day_files[0]
                                logger.info(f"\nExtracting: {test_file}")
                                
                                with zf.open(test_file) as f:
                                    content = f.read()
                                    df = parse_mms_csv(content)
                                    
                                    if not df.empty:
                                        logger.info(f"✅ Successfully parsed {len(df)} records")
                                        logger.info(f"   Time range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
                                        logger.info(f"   Unique DUIDs: {df['duid'].nunique()}")
                                        logger.info(f"   Sample data:")
                                        print(df.head())
                                        
                                        # Calculate 30-minute aggregates
                                        df['settlementdate_30min'] = df['settlementdate'].dt.floor('30min')
                                        scada30 = df.groupby(['settlementdate_30min', 'duid'])['scadavalue'].mean().reset_index()
                                        scada30.columns = ['settlementdate', 'duid', 'scadavalue']
                                        
                                        logger.info(f"\n30-minute aggregation:")
                                        logger.info(f"   Records: {len(scada30)}")
                                        logger.info(f"   Sample:")
                                        print(scada30.head())
                                        
                                        return True
                                        
                    except zipfile.BadZipFile:
                        # Not enough data yet
                        continue
                    except Exception as e:
                        if 'File is not a zip file' not in str(e):
                            logger.debug(f"ZIP read error: {e}")
                
                # Stop if we found what we need
                if found_scada_files and downloaded_mb > 1000:
                    logger.info("Found required data, stopping download")
                    break
                    
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        return False
    
    return False

# Run test
if __name__ == "__main__":
    logger.info("Testing MMSDM DISPATCH_UNIT_SCADA extraction")
    logger.info("="*60)
    
    success = test_single_day_extraction()
    
    if success:
        logger.info("\n✅ TEST SUCCESSFUL")
        logger.info("We can extract DISPATCH_UNIT_SCADA data from MMSDM archives")
        logger.info("Ready to create full backfill script")
    else:
        logger.info("\n❌ TEST FAILED")
        logger.info("Need to troubleshoot extraction process")