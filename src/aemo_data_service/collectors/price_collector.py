#!/usr/bin/env python3
"""
Spot Price Collector for AEMO Data Service
Collects regional spot prices from NEMWEB every 5 minutes.
"""

import pandas as pd
import requests
import re
import zipfile
import tempfile
import os
from io import StringIO
import csv
from pathlib import Path
from typing import Optional, List
import asyncio

from .base_collector import BaseCollector
from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)


class PriceCollector(BaseCollector):
    """
    Collector for AEMO spot price data.
    
    Downloads DISPATCH files from NEMWEB containing 5-minute regional spot prices
    for all regions in the National Electricity Market.
    """
    
    def __init__(self):
        """Initialize the price collector."""
        super().__init__(
            name="Spot Prices",
            output_file=config.spot_hist_file,
            update_interval_minutes=config.update_interval_minutes
        )
        
        self.aemo_url = config.aemo_dispatch_url
        self.last_processed_file = None
    
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with price data schema."""
        # Price data uses SETTLEMENTDATE as index
        df = pd.DataFrame(columns=['REGIONID', 'RRP'])
        df.index.name = 'SETTLEMENTDATE'
        return df
    
    def get_required_columns(self) -> List[str]:
        """Return required columns for price data."""
        return ['REGIONID', 'RRP']
    
    async def fetch_latest_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch the latest spot price data from NEMWEB.
        
        Returns:
            DataFrame with SETTLEMENTDATE index and columns: REGIONID, RRP
        """
        try:
            # Get latest dispatch file
            result = await self._get_latest_dispatch_file()
            
            if not result or not result[0]:
                logger.warning("Failed to download latest dispatch file")
                return None
            
            csv_content, filename = result
            
            # Skip if we've already processed this file
            if filename == self.last_processed_file:
                logger.info("Latest file already processed")
                return None
            
            # Parse the data
            new_data = self._parse_dispatch_data(csv_content)
            
            if new_data is not None and not new_data.empty:
                self.last_processed_file = filename
                logger.info(f"Fetched {len(new_data)} price records from {filename}")
            
            return new_data
            
        except Exception as e:
            logger.error(f"Error fetching latest price data: {e}")
            return None
    
    async def _get_latest_dispatch_file(self):
        """
        Download the latest dispatch file from AEMO website.
        Returns the CSV content as a string and filename, or None if failed.
        """
        try:
            # Use proper headers to avoid 403 errors
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(self.aemo_url, headers=headers, timeout=30)
            )
            response.raise_for_status()
            
            # Look for PUBLIC_DISPATCH files
            zip_pattern = r'PUBLIC_DISPATCH_\d{12}_\d{14}_LEGACY\.zip'
            matches = re.findall(zip_pattern, response.text)
            
            if not matches:
                logger.error("No dispatch files found on AEMO website")
                return None, None
            
            # Get the latest file
            latest_file = sorted(matches)[-1]
            file_url = self.aemo_url + latest_file
            
            logger.info(f"Downloading: {latest_file}")
            
            # Download the zip file with retry logic
            for attempt in range(3):
                try:
                    if attempt > 0:
                        await asyncio.sleep(2 * attempt)
                        logger.info(f"Retry attempt {attempt + 1} for price file {latest_file}")
                    
                    zip_response = await loop.run_in_executor(
                        None,
                        lambda: requests.get(file_url, headers=headers, timeout=60)
                    )
                    zip_response.raise_for_status()
                    break
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403 and attempt < 2:
                        logger.warning(f"403 error downloading price file {latest_file}, will retry...")
                        continue
                    else:
                        raise
            
            # Create a temporary file for the zip
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip_file:
                temp_zip_path = temp_zip_file.name
                temp_zip_file.write(zip_response.content)
                temp_zip_file.flush()
                
                try:
                    # Extract the CSV from the zip file
                    with zipfile.ZipFile(temp_zip_path, 'r') as zip_file:
                        # Find the CSV file inside
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
                    # Clean up temporary file
                    try:
                        os.unlink(temp_zip_path)
                    except OSError as e:
                        logger.warning(f"Could not delete temporary file {temp_zip_path}: {e}")
                        
        except Exception as e:
            logger.error(f"Error downloading dispatch file: {e}")
            return None, None
    
    def _parse_dispatch_data(self, csv_content: str) -> Optional[pd.DataFrame]:
        """
        Parse AEMO dispatch CSV data and extract regional price information.
        Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
        """
        try:
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
                
                # Look for DREGION data rows
                if len(fields) >= 9 and fields[0] == 'D' and fields[1] == 'DREGION':
                    try:
                        # Field positions: 0=D, 1=DREGION, 2=empty, 3=3, 4=settlement_date, 
                        # 5=runno, 6=regionid, 7=intervention, 8=rrp
                        settlement_date = pd.to_datetime(fields[4])
                        region = fields[6]
                        rrp = float(fields[8])
                        
                        price_records.append({
                            'SETTLEMENTDATE': settlement_date,
                            'REGIONID': region,
                            'RRP': rrp
                        })
                    except (ValueError, TypeError, IndexError) as e:
                        logger.debug(f"Error parsing price row: {e}")
                        continue
            
            if not price_records:
                logger.warning("No valid price records extracted")
                return pd.DataFrame()
            
            # Convert to DataFrame and set SETTLEMENTDATE as index
            temp_df = pd.DataFrame(price_records)
            result_df = temp_df.set_index('SETTLEMENTDATE')
            
            settlement_time = result_df.index[0]
            logger.info(f"Extracted {len(result_df)} price records for settlement time: {settlement_time}")
            
            # Log extracted prices for verification
            for settlement_date, row in result_df.iterrows():
                logger.info(f"  Parsed: {row['REGIONID']} = ${row['RRP']:.2f}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error parsing dispatch data: {e}")
            return pd.DataFrame()
    
    def is_new_data(self, new_df: pd.DataFrame) -> bool:
        """Check if the new data contains records not already in storage."""
        if self.data.empty:
            return True
        
        if new_df is None or new_df.empty:
            return False
        
        # Get the latest timestamp in existing data
        latest_existing = self.data.index.max()
        
        # Check if new data has more recent timestamps
        latest_new = new_df.index.max()
        
        is_new = latest_new > latest_existing
        logger.info(f"Latest existing: {latest_existing}, Latest new: {latest_new}, Is new: {is_new}")
        
        return is_new
    
    def merge_data(self, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
        """
        Merge new price data with existing data.
        Handles duplicate region-timestamp combinations by keeping the latest.
        """
        if existing.empty:
            return new.copy()
        
        # Filter for only newer records
        latest_existing = existing.index.max()
        newer_records = new[new.index > latest_existing]
        
        if newer_records.empty:
            logger.info("No truly new price records to merge")
            return existing
        
        # Combine and handle duplicates
        combined = pd.concat([existing, newer_records])
        
        # Remove duplicates, keeping last occurrence of each (settlement_date, region) combination
        combined = combined.reset_index()
        combined = combined.drop_duplicates(subset=['SETTLEMENTDATE', 'REGIONID'], keep='last')
        combined = combined.set_index('SETTLEMENTDATE')
        
        logger.info(f"Merged {len(newer_records)} new price records")
        
        return combined
    
    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort price data by settlement date index."""
        return df.sort_index()
    
    def load_existing_data(self) -> pd.DataFrame:
        """Load existing price data, handling the index structure correctly."""
        if self.output_file.exists():
            try:
                df = pd.read_parquet(self.output_file)
                
                # Ensure SETTLEMENTDATE is the index
                if df.index.name != 'SETTLEMENTDATE':
                    if 'SETTLEMENTDATE' in df.columns:
                        df = df.set_index('SETTLEMENTDATE')
                        logger.info("Converted SETTLEMENTDATE column to index")
                    else:
                        logger.error("Cannot find SETTLEMENTDATE in columns or index")
                        return self.create_empty_dataframe()
                
                # Validate columns
                expected_cols = ['REGIONID', 'RRP']
                missing_cols = [col for col in expected_cols if col not in df.columns]
                
                if missing_cols:
                    logger.error(f"Missing expected columns: {missing_cols}")
                    return self.create_empty_dataframe()
                
                logger.info(f"Loaded {len(df)} existing price records, latest: {df.index.max()}")
                return df
                
            except Exception as e:
                logger.error(f"Error loading existing price data: {e}")
        
        return self.create_empty_dataframe()
    
    def save_data(self) -> bool:
        """Save price data to parquet file with index preserved."""
        try:
            # Ensure directory exists
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with index=True to preserve SETTLEMENTDATE index
            self.data.to_parquet(
                self.output_file, 
                compression='snappy', 
                index=True
            )
            
            # Log file size
            file_size = self.output_file.stat().st_size / (1024*1024)
            logger.info(f"Saved price data to {self.output_file} ({file_size:.2f}MB)")
            return True
            
        except Exception as e:
            logger.error(f"Error saving price data: {e}")
            return False
    
    def get_data_summary(self) -> str:
        """Get a summary of the current price data."""
        if self.data.empty:
            return "No price data available"
        
        total_records = len(self.data)
        unique_regions = self.data['REGIONID'].nunique()
        
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / (1024*1024)
        else:
            file_size = 0
        
        date_range = f"{self.data.index.min()} to {self.data.index.max()}"
        
        return f"Prices: {total_records:,} records, {unique_regions} regions, {date_range}, {file_size:.2f}MB"


# Convenience function for standalone use
async def main():
    """Run the price collector once for testing."""
    collector = PriceCollector()
    
    print("Price Collector Status:")
    print(collector.get_summary())
    
    print("\nRunning single collection cycle...")
    success = await collector.run_once()
    
    if success:
        print("✅ Collection successful")
        print(collector.get_summary())
    else:
        print("❌ Collection failed")


if __name__ == "__main__":
    asyncio.run(main())