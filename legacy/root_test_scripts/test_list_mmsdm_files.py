#!/usr/bin/env python3
"""List files in MMSDM archive to understand structure"""

import requests
import zipfile
import io
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def list_files_in_archive():
    """Download archive and list file structure"""
    
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
        
        # Open ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zf:
            all_files = zf.namelist()
            logger.info(f"Total files in archive: {len(all_files)}")
            
            # Find SCADA files
            scada_files = [f for f in all_files if 'SCADA' in f.upper()]
            logger.info(f"\nFiles containing 'SCADA': {len(scada_files)}")
            
            # Show first 10 SCADA files
            for i, f in enumerate(scada_files[:10]):
                logger.info(f"  {i+1}. {f}")
            
            # Look for pattern variations
            dispatch_files = [f for f in all_files if 'DISPATCH' in f.upper()]
            logger.info(f"\nFiles containing 'DISPATCH': {len(dispatch_files)}")
            
            # Check for unit scada specifically
            unit_scada_files = [f for f in all_files if 'UNIT' in f.upper() and 'SCADA' in f.upper()]
            logger.info(f"\nFiles containing 'UNIT' and 'SCADA': {len(unit_scada_files)}")
            for i, f in enumerate(unit_scada_files[:5]):
                logger.info(f"  {i+1}. {f}")
            
            # Check for files on Dec 15
            dec15_files = [f for f in unit_scada_files if '20201215' in f]
            logger.info(f"\nUnit SCADA files for Dec 15, 2020: {len(dec15_files)}")
            for f in dec15_files:
                logger.info(f"  - {f}")
                
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    list_files_in_archive()