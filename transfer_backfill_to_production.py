#!/usr/bin/env python3
"""
Transfer backfilled SCADA30 data from development to production.
Extracts only the specific date ranges that were backfilled.
"""

import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DEV_DATA_PATH = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2'
PROD_DATA_PATH = '/Volumes/davidleitch/aemo_production/data'

# Backfilled periods
BACKFILLED_PERIODS = [
    ("2020-12-01 00:30:00", "2020-12-31 23:30:00"),
    ("2021-10-01 00:30:00", "2021-10-31 23:30:00"),
    ("2022-06-01 00:30:00", "2022-06-30 23:30:00")
]

def extract_backfilled_data():
    """Extract only the backfilled data from development files"""
    
    logger.info("Extracting backfilled data from development files...")
    
    # Read development scada30
    dev_scada30_path = Path(DEV_DATA_PATH) / "scada30.parquet"
    logger.info(f"Reading from: {dev_scada30_path}")
    
    # Read in chunks to manage memory
    backfilled_data = []
    
    for start_str, end_str in BACKFILLED_PERIODS:
        logger.info(f"Extracting period: {start_str} to {end_str}")
        
        # Read only the specific date range
        df = pd.read_parquet(
            dev_scada30_path,
            filters=[
                ('settlementdate', '>=', pd.Timestamp(start_str)),
                ('settlementdate', '<=', pd.Timestamp(end_str))
            ]
        )
        
        logger.info(f"  Found {len(df)} records")
        backfilled_data.append(df)
    
    # Combine all backfilled data
    all_backfilled = pd.concat(backfilled_data, ignore_index=True)
    logger.info(f"Total backfilled records to transfer: {len(all_backfilled)}")
    
    return all_backfilled

def merge_with_production(backfilled_df):
    """Merge backfilled data with production scada30"""
    
    prod_scada30_path = Path(PROD_DATA_PATH) / "scada30.parquet"
    
    # Check if production path exists
    if not prod_scada30_path.parent.exists():
        logger.error(f"Production data directory not found: {prod_scada30_path.parent}")
        return False
    
    if not prod_scada30_path.exists():
        logger.error(f"Production scada30.parquet not found: {prod_scada30_path}")
        return False
    
    logger.info(f"Reading production scada30 from: {prod_scada30_path}")
    
    # Read production data
    prod_df = pd.read_parquet(prod_scada30_path)
    logger.info(f"Production records: {len(prod_df)}")
    
    # Combine with backfilled data
    logger.info("Merging data...")
    combined_df = pd.concat([prod_df, backfilled_df], ignore_index=True)
    
    # Remove duplicates
    logger.info("Removing duplicates...")
    combined_df = combined_df.drop_duplicates(['settlementdate', 'duid'], keep='first')
    
    # Sort by timestamp
    logger.info("Sorting data...")
    combined_df = combined_df.sort_values(['settlementdate', 'duid'])
    
    logger.info(f"Final record count: {len(combined_df)} (added {len(combined_df) - len(prod_df)})")
    
    # Create backup
    backup_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = prod_scada30_path.parent / f"scada30_backup_{backup_time}.parquet"
    logger.info(f"Creating backup: {backup_path}")
    prod_df.to_parquet(backup_path, engine='pyarrow')
    
    # Save merged data
    logger.info(f"Saving merged data to: {prod_scada30_path}")
    combined_df.to_parquet(prod_scada30_path, engine='pyarrow')
    
    return True

def verify_production_update():
    """Verify the production data was updated correctly"""
    
    prod_scada30_path = Path(PROD_DATA_PATH) / "scada30.parquet"
    
    logger.info("\nVerifying production update...")
    
    for start_str, end_str in BACKFILLED_PERIODS:
        df = pd.read_parquet(
            prod_scada30_path,
            filters=[
                ('settlementdate', '>=', pd.Timestamp(start_str)),
                ('settlementdate', '<=', pd.Timestamp(end_str))
            ]
        )
        
        period_name = pd.Timestamp(start_str).strftime("%B %Y")
        if len(df) > 0:
            logger.info(f"✅ {period_name}: {len(df)} records")
        else:
            logger.error(f"❌ {period_name}: No records found!")

def main():
    """Main function"""
    
    logger.info("Starting transfer of backfilled data to production")
    logger.info("="*60)
    
    # Extract backfilled data
    try:
        backfilled_df = extract_backfilled_data()
    except Exception as e:
        logger.error(f"Failed to extract backfilled data: {e}")
        sys.exit(1)
    
    # Merge with production
    if merge_with_production(backfilled_df):
        logger.info("\n✅ Transfer complete!")
        verify_production_update()
    else:
        logger.error("\n❌ Transfer failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()