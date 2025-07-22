#!/usr/bin/env python3
"""
AEMO Combined Data Updater - Phase 1 Wrapper
Downloads both generation and transmission data in sequence.
Safe wrapper approach that calls existing updaters without modifying them.
"""

import time
from datetime import datetime

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger
from ..generation.update_generation import GenerationDataUpdater
from ..transmission.update_transmission import TransmissionDataUpdater

# Set up logging
setup_logging()
logger = get_logger(__name__)

class CombinedDataUpdater:
    """
    Combined updater that runs both generation and transmission updates
    Phase 1: Safe wrapper approach - no changes to existing updaters
    """
    
    def __init__(self):
        """Initialize the combined updater"""
        self.update_interval = config.update_interval_minutes * 60  # Convert to seconds
        
        # Initialize both updaters
        logger.info("Initializing generation updater...")
        self.gen_updater = GenerationDataUpdater()
        
        logger.info("Initializing transmission updater...")
        self.trans_updater = TransmissionDataUpdater()
        
        logger.info("Combined updater initialized successfully")
    
    def run_once(self):
        """Run a single update cycle for both data types"""
        logger.info("=== Starting combined update cycle ===")
        start_time = datetime.now()
        
        gen_success = False
        trans_success = False
        
        try:
            # Run generation update
            logger.info("Running generation data update...")
            gen_success = self.gen_updater.run_once()
            logger.info(f"Generation update {'succeeded' if gen_success else 'completed (no new data)'}")
            
        except Exception as e:
            logger.error(f"Error in generation update: {e}")
        
        try:
            # Run transmission update
            logger.info("Running transmission data update...")
            trans_success = self.trans_updater.run_once()
            logger.info(f"Transmission update {'succeeded' if trans_success else 'completed (no new data)'}")
            
        except Exception as e:
            logger.error(f"Error in transmission update: {e}")
        
        # Log cycle summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"=== Combined update cycle complete in {duration:.1f}s ===")
        logger.info(f"Generation: {'✓' if gen_success else '○'}, Transmission: {'✓' if trans_success else '○'}")
        
        return gen_success or trans_success
    
    def get_data_summary(self):
        """Get summary of both datasets"""
        try:
            gen_summary = self.gen_updater.get_data_summary()
            trans_summary = self.trans_updater.get_data_summary()
            
            return f"Generation: {gen_summary} | Transmission: {trans_summary}"
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return "Error getting data summary"
    
    def run_monitor(self):
        """Main monitoring loop for combined updates"""
        logger.info("Starting AEMO combined data monitoring...")
        logger.info(f"Checking every {self.update_interval/60:.1f} minutes for new data")
        logger.info(f"Data summary: {self.get_data_summary()}")
        
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                logger.info(f"--- Cycle {cycle_count} ---")
                
                # Run combined update
                any_success = self.run_once()
                
                if any_success:
                    logger.info(f"Updated data summary: {self.get_data_summary()}")
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
            
            # Wait for next update interval
            logger.info(f"Waiting {self.update_interval/60:.1f} minutes for next check...")
            time.sleep(self.update_interval)


def main():
    """Main function to run the combined data updater"""
    logger.info("AEMO Combined Data Updater (Phase 1) starting...")
    
    # Create combined updater instance
    updater = CombinedDataUpdater()
    
    try:
        updater.run_monitor()
    except KeyboardInterrupt:
        logger.info("Combined monitoring stopped by user")
    except Exception as e:
        logger.error(f"Combined monitor crashed: {e}")


if __name__ == "__main__":
    main()