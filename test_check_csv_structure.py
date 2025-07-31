#!/usr/bin/env python3
"""Check structure of CSV in MMSDM archive"""

import requests
import zipfile
import io
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def check_csv_structure():
    """Download archive and check CSV structure"""
    
    url = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2020/MMSDM_2020_12.zip"
    
    logger.info(f"Downloading archive: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=300)
        response.raise_for_status()
        
        logger.info(f"Downloaded {len(response.content) / (1024**3):.1f} GB")
        
        # Open main ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zf:
            # Find nested ZIP
            nested_zip = [f for f in zf.namelist() if 'DISPATCH_UNIT_SCADA' in f and f.endswith('.zip')][0]
            
            logger.info(f"Opening nested ZIP: {nested_zip}")
            
            # Extract nested ZIP
            with zf.open(nested_zip) as nested_file:
                nested_data = nested_file.read()
                
            # Open nested ZIP
            with zipfile.ZipFile(io.BytesIO(nested_data), 'r') as nested_zf:
                csv_files = nested_zf.namelist()
                logger.info(f"CSV files in nested ZIP: {csv_files}")
                
                # Read first few lines of CSV
                csv_name = csv_files[0]
                logger.info(f"\nReading {csv_name}...")
                
                with nested_zf.open(csv_name) as f:
                    # Read first 1000 lines
                    lines = []
                    for i in range(1000):
                        line = f.readline().decode('utf-8', errors='ignore')
                        if not line:
                            break
                        lines.append(line.strip())
                    
                    # Show structure
                    logger.info(f"Total lines read: {len(lines)}")
                    
                    # Find header line
                    header_line = None
                    for i, line in enumerate(lines[:50]):
                        if line.startswith('I') and 'UNIT_SCADA' in line:
                            header_line = i
                            logger.info(f"\nHeader at line {i}: {line}")
                            break
                    
                    # Count data lines
                    data_lines = [l for l in lines if l.startswith('D') and 'UNIT_SCADA' in l]
                    logger.info(f"\nData lines found: {len(data_lines)}")
                    
                    # Show sample data lines
                    logger.info("\nSample data lines:")
                    for line in data_lines[:5]:
                        logger.info(f"  {line}")
                    
                    # Check for Dec 15 data
                    dec15_lines = [l for l in data_lines if '2020/12/15' in l]
                    logger.info(f"\nLines with Dec 15, 2020 data: {len(dec15_lines)}")
                    if dec15_lines:
                        logger.info("Sample Dec 15 lines:")
                        for line in dec15_lines[:3]:
                            logger.info(f"  {line}")
                    
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    check_csv_structure()