#!/usr/bin/env python3
"""
Rooftop Solar Collector for AEMO Data Service
Collects 30-minute rooftop solar data and converts to 5-minute intervals.
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional, List
import asyncio
from datetime import datetime, timedelta

from .base_collector import BaseCollector
from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)


class RooftopCollector(BaseCollector):
    """
    Collector for AEMO rooftop solar data.
    
    Downloads ROOFTOP_PV_ACTUAL_MEASUREMENT files from NEMWEB containing 30-minute 
    rooftop solar generation data and converts to 5-minute intervals using weighted averaging.
    """
    
    def __init__(self):
        """Initialize the rooftop collector."""
        super().__init__(
            name="Rooftop Solar",
            output_file=config.rooftop_file,
            update_interval_minutes=config.update_interval_minutes
        )
        
        self.base_url = "http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"
        self.regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        self.last_processed_files = set()
    
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with rooftop solar schema."""
        columns = ['settlementdate'] + self.regions
        df = pd.DataFrame(columns=columns)
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        return df
    
    def get_required_columns(self) -> List[str]:
        """Return required columns for rooftop data."""
        return ['settlementdate'] + self.regions
    
    async def fetch_latest_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch the latest rooftop solar data from NEMWEB.
        
        Returns:
            DataFrame with columns: settlementdate, NSW1, QLD1, SA1, TAS1, VIC1
        """
        try:
            # Get latest files
            recent_files = await self._get_latest_rooftop_files()
            
            if not recent_files:
                logger.warning("No rooftop PV files found")
                return None
            
            # Process only new files
            new_files = [f for f in recent_files if f not in self.last_processed_files]
            
            if not new_files:
                logger.info("No new rooftop files to process")
                return None
            
            # Download and process files
            new_data_list = []
            for filename in new_files[:3]:  # Process max 3 files per cycle
                logger.info(f"Processing new rooftop file: {filename}")
                
                zip_content = await self._download_rooftop_zip(filename)
                if zip_content is None:
                    continue
                
                # Parse 30-minute data
                df_30min = self._parse_rooftop_zip(zip_content)
                if df_30min.empty:
                    continue
                
                # Convert to 5-minute intervals
                df_5min = self._convert_30min_to_5min(df_30min)
                if not df_5min.empty:
                    new_data_list.append(df_5min)
                    self.last_processed_files.add(filename)
            
            if not new_data_list:
                logger.info("No valid rooftop data processed")
                return None
            
            # Combine all new data
            combined_data = pd.concat(new_data_list, ignore_index=True)
            combined_data = combined_data.sort_values('settlementdate').drop_duplicates(subset=['settlementdate'])
            
            logger.info(f"Fetched {len(combined_data)} rooftop records from {len(new_files)} files")
            return combined_data
            
        except Exception as e:
            logger.error(f"Error fetching rooftop data: {e}")
            return None
    
    async def _get_latest_rooftop_files(self) -> List[str]:
        """Get list of recent rooftop PV files from NEMWEB."""
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
                if href.endswith('.zip') and 'ROOFTOP_PV_ACTUAL_MEASUREMENT' in href:
                    zip_files.append(href)
            
            if not zip_files:
                return []
            
            # Sort to get most recent files
            zip_files.sort(reverse=True)
            
            # Return last 5 files for processing
            recent_files = zip_files[:5]
            logger.info(f"Found {len(recent_files)} recent rooftop PV files")
            
            return recent_files
            
        except Exception as e:
            logger.error(f"Error getting rooftop file list: {e}")
            return []
    
    async def _download_rooftop_zip(self, filename: str) -> Optional[bytes]:
        """Download a specific rooftop PV ZIP file with retry logic."""
        # Construct URL
        if filename.startswith('/'):
            file_url = "http://nemweb.com.au" + filename
        else:
            file_url = self.base_url + filename
        
        for attempt in range(3):
            try:
                # Use proper headers to avoid 403 errors
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                # Add small delay to avoid rate limiting
                if attempt > 0:
                    await asyncio.sleep(2 * attempt)
                    logger.info(f"Retry attempt {attempt + 1} for rooftop file {filename}")
                
                # Download in thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(file_url, headers=headers, timeout=30)
                )
                response.raise_for_status()
                
                return response.content
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 and attempt < 2:
                    logger.warning(f"403 error downloading rooftop file {filename}, will retry...")
                    continue
                else:
                    logger.error(f"Failed to download rooftop file {filename}: {e}")
                    return None
            except Exception as e:
                logger.error(f"Failed to download rooftop file {filename}: {e}")
                return None
        
        return None
    
    def _parse_rooftop_zip(self, zip_content: bytes) -> pd.DataFrame:
        """Parse rooftop PV ZIP content into 30-minute DataFrame."""
        try:
            # Extract ZIP file
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                # Look for CSV files
                csv_files = [name for name in zip_file.namelist() if name.lower().endswith('.csv')]
                
                if not csv_files:
                    logger.error("No CSV files found in rooftop ZIP")
                    return pd.DataFrame()
                
                csv_filename = csv_files[0]
                
                # Read CSV content
                with zip_file.open(csv_filename) as csv_file:
                    csv_content = csv_file.read().decode('utf-8')
            
            # Parse CSV lines
            lines = csv_content.strip().split('\n')
            data_rows = []
            
            for line in lines:
                if line.startswith('D,ROOFTOP,ACTUAL'):
                    fields = line.split(',')
                    if len(fields) >= 8:
                        # Extract fields: interval_datetime, regionid, powermw
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
            
            if not data_rows:
                logger.warning("No valid rooftop data rows found")
                return pd.DataFrame()
            
            # Create DataFrame and pivot
            df = pd.DataFrame(data_rows)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            # Pivot to get regions as columns
            pivot_df = df.pivot_table(
                index='settlementdate',
                columns='regionid',
                values='powermw',
                aggfunc='first'
            ).fillna(0)
            
            # Reset index and ensure all regions are present
            pivot_df = pivot_df.reset_index()
            
            # Add missing regions as zero columns
            for region in self.regions:
                if region not in pivot_df.columns:
                    pivot_df[region] = 0.0
            
            # Reorder columns
            columns = ['settlementdate'] + self.regions
            pivot_df = pivot_df[columns]
            
            logger.info(f"Parsed {len(pivot_df)} 30-minute rooftop records")
            logger.info(f"Date range: {pivot_df['settlementdate'].min()} to {pivot_df['settlementdate'].max()}")
            
            return pivot_df
            
        except Exception as e:
            logger.error(f"Error parsing rooftop ZIP: {e}")
            return pd.DataFrame()
    
    def _convert_30min_to_5min(self, df_30min: pd.DataFrame) -> pd.DataFrame:
        """
        Convert 30-minute data to 5-minute intervals using weighted averaging.
        
        Algorithm:
        - Each 30-min period creates 6 x 5-min periods
        - Uses weighted transition between consecutive 30-min values
        - Formula: ((6-j)*current + j*next) / 6 for periods j=0..5
        """
        if df_30min.empty:
            return pd.DataFrame()
        
        # Sort by time
        df_30min = df_30min.sort_values('settlementdate')
        
        # Create list for 5-minute records
        five_min_records = []
        
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
                for region in self.regions:
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
                        # No next value - use current value
                        value = current_value
                    
                    record[region] = value
                
                five_min_records.append(record)
        
        # Create DataFrame
        df_5min = pd.DataFrame(five_min_records)
        
        logger.info(f"Converted {len(df_30min)} 30-min records to {len(df_5min)} 5-min records")
        
        return df_5min
    
    def is_new_data(self, new_df: pd.DataFrame) -> bool:
        """Check if the new data contains records not already in storage."""
        if self.data.empty:
            return True
        
        if new_df is None or new_df.empty:
            return False
        
        # Get latest timestamp in existing data
        latest_existing = self.data['settlementdate'].max()
        
        # Check if new data has more recent timestamps
        latest_new = new_df['settlementdate'].max()
        
        is_new = latest_new > latest_existing
        logger.info(f"Latest existing: {latest_existing}, Latest new: {latest_new}, Is new: {is_new}")
        
        return is_new
    
    def merge_data(self, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
        """
        Merge new rooftop data with existing data.
        Only adds truly new records based on timestamp.
        """
        if existing.empty:
            return new.copy()
        
        # Filter for only newer records
        latest_existing = existing['settlementdate'].max()
        truly_new = new[new['settlementdate'] > latest_existing]
        
        if truly_new.empty:
            logger.info("No truly new rooftop records to merge")
            return existing
        
        # Concatenate and return
        combined = pd.concat([existing, truly_new], ignore_index=True)
        logger.info(f"Merged {len(truly_new)} new rooftop records")
        
        return combined
    
    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort rooftop data by settlement date."""
        return df.sort_values('settlementdate').reset_index(drop=True)
    
    def get_data_summary(self) -> str:
        """Get a summary of the current rooftop data."""
        if self.data.empty:
            return "No rooftop data available"
        
        total_records = len(self.data)
        date_range = f"{self.data['settlementdate'].min()} to {self.data['settlementdate'].max()}"
        
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / (1024*1024)
        else:
            file_size = 0
        
        # Calculate total generation across all regions
        region_totals = {}
        for region in self.regions:
            if region in self.data.columns:
                region_totals[region] = self.data[region].sum()
        
        return f"Rooftop: {total_records:,} records, {date_range}, {file_size:.2f}MB, Regions: {len(region_totals)}"


# Convenience function for standalone use
async def main():
    """Run the rooftop collector once for testing."""
    collector = RooftopCollector()
    
    print("Rooftop Collector Status:")
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