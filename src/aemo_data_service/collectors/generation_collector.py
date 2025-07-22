#!/usr/bin/env python3
"""
Generation Data Collector for AEMO Data Service
Collects SCADA generation data from NEMWEB every 5 minutes.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional, List
import asyncio

from .base_collector import BaseCollector
from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)


class GenerationCollector(BaseCollector):
    """
    Collector for AEMO generation SCADA data.
    
    Downloads DISPATCH_SCADA files from NEMWEB containing 5-minute generation data
    for all registered units in the National Electricity Market.
    """
    
    def __init__(self):
        """Initialize the generation collector."""
        super().__init__(
            name="Generation SCADA",
            output_file=config.gen_output_file,
            update_interval_minutes=config.update_interval_minutes
        )
        
        self.base_url = "http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/"
        self.last_processed_file = None
    
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with generation data schema."""
        df = pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df
    
    def get_required_columns(self) -> List[str]:
        """Return required columns for generation data."""
        return ['settlementdate', 'duid', 'scadavalue']
    
    async def fetch_latest_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch the latest SCADA data from NEMWEB.
        
        Returns:
            DataFrame with columns: settlementdate, duid, scadavalue
        """
        try:
            # Get latest file URL
            latest_url = await self._get_latest_file_url()
            
            if not latest_url:
                logger.warning("No SCADA files found")
                return None
            
            # Skip if we've already processed this file
            if latest_url == self.last_processed_file:
                logger.info("Latest file already processed")
                return None
            
            # Download and parse
            new_data = await self._download_and_parse_file(latest_url)
            
            if new_data is not None and not new_data.empty:
                self.last_processed_file = latest_url
                logger.info(f"Fetched {len(new_data)} records from {latest_url}")
            
            return new_data
            
        except Exception as e:
            logger.error(f"Error fetching latest data: {e}")
            return None
    
    async def _get_latest_file_url(self) -> Optional[str]:
        """Get the URL of the most recent SCADA file."""
        try:
            # Use proper headers to avoid 403 errors
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.get(self.base_url, headers=headers, timeout=30)
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all ZIP file links
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'DISPATCHSCADA' in href:
                    zip_files.append(href)
            
            if not zip_files:
                return None
            
            # Sort to get the most recent (AEMO files have timestamp in filename)
            zip_files.sort(reverse=True)
            latest_file = zip_files[0]
            
            # Extract timestamp for logging
            timestamp_match = re.search(r'(\d{12})', latest_file)
            if timestamp_match:
                timestamp = timestamp_match.group(1)
                logger.info(f"Latest file: {latest_file} (timestamp: {timestamp})")
            
            # Construct proper URL
            if latest_file.startswith('/'):
                file_url = "http://nemweb.com.au" + latest_file
            else:
                file_url = self.base_url + latest_file
            
            return file_url
            
        except Exception as e:
            logger.error(f"Error getting latest file URL: {e}")
            return None
    
    async def _download_and_parse_file(self, file_url: str) -> Optional[pd.DataFrame]:
        """Download and parse SCADA ZIP file with retry logic."""
        for attempt in range(3):
            try:
                # Use proper headers to avoid 403 errors
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                # Add small delay to avoid rate limiting
                if attempt > 0:
                    await asyncio.sleep(2 * attempt)
                    logger.info(f"Retry attempt {attempt + 1} for {file_url}")
                
                # Download in thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(file_url, headers=headers, timeout=60)
                )
                response.raise_for_status()
                
                # If successful, break the retry loop
                break
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 and attempt < 2:
                    logger.warning(f"403 error downloading {file_url}, will retry...")
                    continue
                else:
                    logger.error(f"Error downloading file {file_url}: {e}")
                    return None
            except Exception as e:
                logger.error(f"Error downloading file {file_url}: {e}")
                return None
        
        try:
            # Extract ZIP file
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                # Get the first CSV file in the ZIP
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
            
            # Find data lines (start with 'D')
            data_rows = []
            for line in lines:
                if line.startswith('D,DISPATCH,UNIT_SCADA'):
                    # Split CSV line and extract required fields
                    fields = line.split(',')
                    if len(fields) >= 7:  # Ensure we have all required fields
                        settlementdate = fields[4].strip('"')
                        duid = fields[5].strip('"')
                        scadavalue = fields[6].strip('"')
                        
                        try:
                            scadavalue = float(scadavalue)
                        except ValueError:
                            continue  # Skip invalid numeric values
                        
                        data_rows.append({
                            'settlementdate': settlementdate,
                            'duid': duid,
                            'scadavalue': scadavalue
                        })
            
            if data_rows:
                df = pd.DataFrame(data_rows)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                logger.info(f"Parsed {len(df)} records")
                return df
            else:
                logger.warning("No valid data rows found in file")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading/parsing file {file_url}: {e}")
            return None
    
    def is_new_data(self, new_df: pd.DataFrame) -> bool:
        """Check if the new data contains records not already in storage."""
        if self.data.empty:
            return True
        
        if new_df is None or new_df.empty:
            return False
        
        # Get the latest timestamp in existing data
        latest_existing = self.data['settlementdate'].max()
        
        # Check if new data has more recent timestamps
        latest_new = new_df['settlementdate'].max()
        
        is_new = latest_new > latest_existing
        logger.info(f"Latest existing: {latest_existing}, Latest new: {latest_new}, Is new: {is_new}")
        
        return is_new
    
    def merge_data(self, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
        """
        Merge new generation data with existing data.
        Only adds truly new records based on timestamp.
        """
        if existing.empty:
            return new.copy()
        
        # Filter out any records that aren't truly new
        latest_existing = existing['settlementdate'].max()
        truly_new = new[new['settlementdate'] > latest_existing]
        
        if truly_new.empty:
            logger.info("No truly new records to merge")
            return existing
        
        # Concatenate and return
        combined = pd.concat([existing, truly_new], ignore_index=True)
        logger.info(f"Merged {len(truly_new)} new records")
        
        return combined
    
    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort generation data by settlement date."""
        return df.sort_values('settlementdate').reset_index(drop=True)
    
    def get_data_summary(self) -> str:
        """Get a summary of the current generation data."""
        if self.data.empty:
            return "No generation data available"
        
        total_records = len(self.data)
        date_range = f"{self.data['settlementdate'].min()} to {self.data['settlementdate'].max()}"
        unique_duids = self.data['duid'].nunique()
        
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / (1024*1024)
        else:
            file_size = 0
        
        return f"Generation: {total_records:,} records, {unique_duids} DUIDs, {date_range}, {file_size:.2f}MB"


# Convenience function for standalone use
async def main():
    """Run the generation collector once for testing."""
    collector = GenerationCollector()
    
    print("Generation Collector Status:")
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