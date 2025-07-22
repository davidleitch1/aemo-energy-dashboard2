#!/usr/bin/env python3
"""
Base Collector for AEMO Data Service
Abstract base class for all data collectors.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from typing import Optional, Dict, Any, List
import logging

from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)


class BaseCollector(ABC):
    """
    Abstract base class for all AEMO data collectors.
    
    Provides common functionality:
    - File management and parquet operations
    - Update scheduling and timing
    - Error handling and retries
    - Data validation
    - Status reporting
    """
    
    def __init__(self, name: str, output_file: Path, update_interval_minutes: int = None):
        """
        Initialize the base collector.
        
        Args:
            name: Human-readable name for the collector
            output_file: Path to output parquet file
            update_interval_minutes: How often to check for new data (default from config)
        """
        self.name = name
        self.output_file = Path(output_file)
        self.update_interval = (update_interval_minutes or config.update_interval_minutes) * 60
        self.last_update = None
        self.error_count = 0
        self.max_retries = 3
        
        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize data
        self.data = self.load_existing_data()
        
        logger.info(f"Initialized {self.name} collector")
        logger.info(f"Output file: {self.output_file}")
        logger.info(f"Update interval: {self.update_interval/60:.1f} minutes")
    
    def load_existing_data(self) -> pd.DataFrame:
        """Load existing data from parquet file or create empty DataFrame."""
        if self.output_file.exists():
            try:
                df = pd.read_parquet(self.output_file)
                logger.info(f"{self.name}: Loaded {len(df)} existing records")
                return df
            except Exception as e:
                logger.error(f"{self.name}: Error loading existing data: {e}")
        
        # Create empty DataFrame with proper structure
        df = self.create_empty_dataframe()
        logger.info(f"{self.name}: Created new empty DataFrame")
        return df
    
    @abstractmethod
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct schema for this collector."""
        pass
    
    @abstractmethod
    async def fetch_latest_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data from the source.
        
        Returns:
            DataFrame with new data, or None if no new data available
        """
        pass
    
    @abstractmethod
    def is_new_data(self, new_df: pd.DataFrame) -> bool:
        """
        Check if the fetched data contains new records.
        
        Args:
            new_df: The freshly fetched data
            
        Returns:
            True if the data contains new records
        """
        pass
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate the fetched data before adding to storage.
        
        Args:
            df: Data to validate
            
        Returns:
            True if data is valid
        """
        if df is None or df.empty:
            return False
        
        # Check for required columns - subclasses can override
        required_columns = self.get_required_columns()
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            logger.error(f"{self.name}: Missing required columns: {missing_columns}")
            return False
        
        return True
    
    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """Return list of required column names for this collector."""
        pass
    
    def add_new_data(self, new_df: pd.DataFrame) -> bool:
        """
        Add new data to the existing dataset and save to parquet.
        
        Args:
            new_df: New data to add
            
        Returns:
            True if data was successfully added
        """
        try:
            if not self.validate_data(new_df):
                logger.warning(f"{self.name}: Data validation failed")
                return False
            
            # If existing data is empty, just use the new data
            if self.data.empty:
                self.data = new_df.copy()
                added_count = len(new_df)
            else:
                # Merge with existing data - subclasses can override merge logic
                initial_count = len(self.data)
                self.data = self.merge_data(self.data, new_df)
                added_count = len(self.data) - initial_count
            
            if added_count > 0:
                # Sort and save
                self.data = self.sort_data(self.data)
                self.save_data()
                
                logger.info(f"{self.name}: Added {added_count} new records")
                logger.info(f"{self.name}: Total records: {len(self.data):,}")
                return True
            else:
                logger.info(f"{self.name}: No new records to add")
                return False
                
        except Exception as e:
            logger.error(f"{self.name}: Error adding new data: {e}")
            return False
    
    def merge_data(self, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
        """
        Merge new data with existing data.
        Default implementation concatenates and removes duplicates.
        Subclasses can override for specific merge logic.
        """
        combined = pd.concat([existing, new], ignore_index=True)
        # Remove duplicates based on all columns
        return combined.drop_duplicates().reset_index(drop=True)
    
    @abstractmethod
    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort the data appropriately for this collector."""
        pass
    
    def save_data(self) -> bool:
        """Save current data to parquet file."""
        try:
            self.data.to_parquet(
                self.output_file, 
                compression='snappy', 
                index=False
            )
            
            # Log file size
            file_size = self.output_file.stat().st_size / (1024*1024)
            logger.info(f"{self.name}: Saved to {self.output_file} ({file_size:.2f}MB)")
            return True
            
        except Exception as e:
            logger.error(f"{self.name}: Error saving data: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the collector."""
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / (1024*1024)
        else:
            file_size = 0
        
        status = {
            'name': self.name,
            'last_update': self.last_update,
            'error_count': self.error_count,
            'total_records': len(self.data) if not self.data.empty else 0,
            'file_size_mb': round(file_size, 2),
            'output_file': str(self.output_file)
        }
        
        # Add data range if available
        if not self.data.empty and hasattr(self.data, 'settlementdate'):
            try:
                status['date_range'] = {
                    'start': self.data['settlementdate'].min().isoformat(),
                    'end': self.data['settlementdate'].max().isoformat()
                }
            except:
                pass
        
        return status
    
    async def run_once(self) -> bool:
        """
        Run a single collection cycle.
        
        Returns:
            True if new data was collected successfully
        """
        try:
            logger.info(f"{self.name}: Starting collection cycle")
            
            # Fetch new data
            new_data = await self.fetch_latest_data()
            
            if new_data is None:
                logger.info(f"{self.name}: No data available from source")
                return False
            
            # Check if it's actually new
            if not self.is_new_data(new_data):
                logger.info(f"{self.name}: Data available but not new")
                return False
            
            # Add to storage
            success = self.add_new_data(new_data)
            
            if success:
                self.last_update = datetime.now()
                self.error_count = 0
                logger.info(f"{self.name}: Collection cycle completed successfully")
                return True
            else:
                self.error_count += 1
                logger.warning(f"{self.name}: Collection cycle failed")
                return False
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"{self.name}: Error in collection cycle: {e}")
            return False
    
    # Note: Continuous collection is now handled by the unified service
    # Individual collectors only implement run_once() method
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the collector status."""
        status = self.get_status()
        
        summary = f"{status['name']}:\n"
        summary += f"  Records: {status['total_records']:,}\n"
        summary += f"  File size: {status['file_size_mb']} MB\n"
        summary += f"  Errors: {status['error_count']}\n"
        
        if status.get('date_range'):
            summary += f"  Date range: {status['date_range']['start']} to {status['date_range']['end']}\n"
        
        if status['last_update']:
            summary += f"  Last update: {status['last_update']}\n"
        
        return summary