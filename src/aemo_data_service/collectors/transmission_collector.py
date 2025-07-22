#!/usr/bin/env python3
"""
Transmission Flow Collector for AEMO Data Service
Collects 5-minute transmission interconnector flow data from NEMWEB.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional, List, Dict
import asyncio

from .base_collector import BaseCollector
from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)


class TransmissionCollector(BaseCollector):
    """
    Collector for AEMO transmission interconnector flow data.
    
    Downloads DISPATCHINTERCONNECTORRES files from NEMWEB containing 5-minute 
    transmission flow data between NEM regions.
    """
    
    def __init__(self):
        """Initialize the transmission collector."""
        super().__init__(
            name="Transmission Flows",
            output_file=config.transmission_file,
            update_interval_minutes=config.update_interval_minutes
        )
        
        self.base_url = "http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/"
        self.last_processed_file = None
        
        # Define interconnector mapping for reference
        self.interconnector_mapping = {
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
    
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with transmission flow schema."""
        df = pd.DataFrame(columns=[
            'settlementdate', 'interconnectorid', 'meteredmwflow', 
            'mwflow', 'exportlimit', 'importlimit', 'mwlosses'
        ])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df
    
    def get_required_columns(self) -> List[str]:
        """Return required columns for transmission data."""
        return [
            'settlementdate', 'interconnectorid', 'meteredmwflow', 
            'mwflow', 'exportlimit', 'importlimit', 'mwlosses'
        ]
    
    async def fetch_latest_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch the latest transmission flow data from NEMWEB.
        
        Returns:
            DataFrame with transmission flow data
        """
        try:
            # Get latest file URL
            latest_url = await self._get_latest_file_url()
            
            if not latest_url:
                logger.warning("No DISPATCHIS files found")
                return None
            
            # Skip if we've already processed this file
            if latest_url == self.last_processed_file:
                logger.info("Latest file already processed")
                return None
            
            # Download and parse
            new_data = await self._download_and_parse_file(latest_url)
            
            if new_data is not None and not new_data.empty:
                self.last_processed_file = latest_url
                logger.info(f"Fetched {len(new_data)} transmission records from {latest_url}")
                logger.info(f"Interconnectors: {new_data['interconnectorid'].unique()}")
            
            return new_data
            
        except Exception as e:
            logger.error(f"Error fetching transmission data: {e}")
            return None
    
    async def _get_latest_file_url(self) -> Optional[str]:
        """Get the URL of the most recent DISPATCHIS file."""
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
                if href.endswith('.zip') and 'DISPATCHIS' in href:
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
        """Download and parse DISPATCHIS ZIP file with retry logic."""
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
                logger.info(f"Parsed {len(df)} transmission flow records")
                return df
            else:
                logger.warning("No valid transmission flow data rows found")
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
        Merge new transmission data with existing data.
        Only adds truly new records based on timestamp.
        """
        if existing.empty:
            return new.copy()
        
        # Filter for only newer records
        latest_existing = existing['settlementdate'].max()
        truly_new = new[new['settlementdate'] > latest_existing]
        
        if truly_new.empty:
            logger.info("No truly new transmission records to merge")
            return existing
        
        # Concatenate and return
        combined = pd.concat([existing, truly_new], ignore_index=True)
        logger.info(f"Merged {len(truly_new)} new transmission records")
        
        return combined
    
    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort transmission data by settlement date."""
        return df.sort_values('settlementdate').reset_index(drop=True)
    
    def get_data_summary(self) -> str:
        """Get a summary of the current transmission data."""
        if self.data.empty:
            return "No transmission data available"
        
        total_records = len(self.data)
        date_range = f"{self.data['settlementdate'].min()} to {self.data['settlementdate'].max()}"
        unique_interconnectors = self.data['interconnectorid'].nunique()
        
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / (1024*1024)
        else:
            file_size = 0
        
        return f"Transmission: {total_records:,} records, {unique_interconnectors} interconnectors, {date_range}, {file_size:.2f}MB"


# Convenience function for standalone use
async def main():
    """Run the transmission collector once for testing."""
    collector = TransmissionCollector()
    
    print("Transmission Collector Status:")
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