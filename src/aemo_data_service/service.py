#!/usr/bin/env python3
"""
AEMO Data Service - Main Orchestrator
Manages all data collectors and provides unified service interface.
"""

import asyncio
import signal
import sys
from typing import Dict, List, Optional
from datetime import datetime
import json
from pathlib import Path

from .shared.config import config
from .shared.logging_config import configure_service_logging, get_logger
from .collectors.generation_collector import GenerationCollector
from .collectors.price_collector import PriceCollector
from .collectors.rooftop_collector import RooftopCollector
from .collectors.transmission_collector import TransmissionCollector

# Set up logging
configure_service_logging()
logger = get_logger(__name__)


class AEMODataService:
    """
    Main data service that orchestrates all AEMO data collectors.
    
    Uses unified timing: all collectors checked in single 4.5-minute loop.
    
    Provides:
    - Unified start/stop control
    - Synchronized data collection
    - Status monitoring
    - Error handling and recovery
    - Performance metrics
    """
    
    def __init__(self):
        """Initialize the data service."""
        self.collectors = {}
        self.is_running = False
        self.start_time = None
        self.collection_task = None
        self.cycle_count = 0
        self.last_cycle_time = None
        self.update_interval = config.update_interval_minutes * 60  # Convert to seconds
        
        # Initialize collectors
        self._initialize_collectors()
        
        # Set up signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        logger.info("AEMO Data Service initialized")
        logger.info(f"Unified collection cycle: {config.update_interval_minutes} minutes")
    
    def _initialize_collectors(self):
        """Initialize all data collectors."""
        try:
            # All collectors implemented
            self.collectors['generation'] = GenerationCollector()
            self.collectors['prices'] = PriceCollector()
            self.collectors['rooftop'] = RooftopCollector()
            self.collectors['transmission'] = TransmissionCollector()
            
            logger.info(f"Initialized {len(self.collectors)} collectors")
            
        except Exception as e:
            logger.error(f"Error initializing collectors: {e}")
            raise
    
    def _setup_signal_handlers(self):
        """Set up graceful shutdown on SIGINT/SIGTERM."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def start(self) -> None:
        """Start the unified data collection service."""
        if self.is_running:
            logger.warning("Service is already running")
            return
        
        logger.info("Starting AEMO Data Service with unified timing...")
        logger.info(config.get_summary())
        
        self.is_running = True
        self.start_time = datetime.now()
        self.cycle_count = 0
        
        # Start the unified collection loop
        self.collection_task = asyncio.create_task(
            self._unified_collection_loop(),
            name="unified_collection"
        )
        
        logger.info(f"Started unified collection loop (every {self.update_interval/60:.1f} minutes)")
        
        # Wait for the task to complete (or be cancelled)
        try:
            await self.collection_task
        except asyncio.CancelledError:
            logger.info("Collection task cancelled")
        except Exception as e:
            logger.error(f"Error in unified collection: {e}")
        finally:
            self.is_running = False
            logger.info("AEMO Data Service stopped")
    
    async def stop(self) -> None:
        """Stop the data collection service gracefully."""
        if not self.is_running:
            logger.warning("Service is not running")
            return
        
        logger.info("Stopping AEMO Data Service...")
        
        # Cancel the collection task
        if self.collection_task and not self.collection_task.done():
            self.collection_task.cancel()
            
            # Wait for graceful shutdown
            try:
                await asyncio.wait_for(self.collection_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.info("Collection task stopped")
        
        self.is_running = False
        logger.info("Data service stopped")
    
    async def _unified_collection_loop(self) -> None:
        """
        Main unified collection loop.
        Checks all data sources every update_interval seconds.
        """
        logger.info("Starting unified collection loop")
        
        while self.is_running:
            try:
                cycle_start = datetime.now()
                self.cycle_count += 1
                
                logger.info(f"=== Collection Cycle #{self.cycle_count} Started ===")
                
                # Collect results from all sources in parallel
                results = await self._run_collection_cycle()
                
                # Log cycle results
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                success_count = sum(1 for success in results.values() if success)
                
                logger.info(f"=== Cycle #{self.cycle_count} Complete in {cycle_duration:.1f}s ===")
                logger.info(f"Successful collections: {success_count}/{len(results)}")
                
                for name, success in results.items():
                    status = "✅" if success else "❌"
                    logger.info(f"  {status} {name}")
                
                self.last_cycle_time = cycle_start
                
                # Wait for next cycle (unless stopping)
                if self.is_running:
                    logger.info(f"Next collection cycle in {self.update_interval/60:.1f} minutes...")
                    await asyncio.sleep(self.update_interval)
                    
            except asyncio.CancelledError:
                logger.info("Collection loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in collection cycle #{self.cycle_count}: {e}")
                # Wait a bit before retrying on error
                if self.is_running:
                    await asyncio.sleep(60)  # 1-minute backoff on error
    
    async def _run_collection_cycle(self) -> Dict[str, bool]:
        """
        Run a single collection cycle for all collectors.
        Returns dict mapping collector names to success status.
        """
        # Run all collectors concurrently
        tasks = []
        for name, collector in self.collectors.items():
            task = asyncio.create_task(
                collector.run_once(),
                name=f"cycle_{name}"
            )
            tasks.append((name, task))
        
        # Wait for all to complete
        results = {}
        for name, task in tasks:
            try:
                success = await task
                results[name] = success
            except Exception as e:
                logger.error(f"Error in {name} collector: {e}")
                results[name] = False
        
        return results
    
    async def run_once_all(self) -> Dict[str, bool]:
        """
        Run all collectors once (for testing/manual execution).
        
        Returns:
            Dict mapping collector names to success status
        """
        logger.info("Running single collection cycle...")
        
        # Use the same collection cycle method for consistency
        results = await self._run_collection_cycle()
        
        # Log results
        success_count = sum(1 for success in results.values() if success)
        logger.info(f"Single cycle complete: {success_count}/{len(results)} successful")
        
        for name, success in results.items():
            status = "✅" if success else "❌"
            logger.info(f"{status} {name}: {'success' if success else 'failed'}")
        
        return results
    
    def get_status(self) -> Dict:
        """Get comprehensive service status."""
        status = {
            'service': {
                'running': self.is_running,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'uptime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
                'cycle_count': self.cycle_count,
                'last_cycle': self.last_cycle_time.isoformat() if self.last_cycle_time else None,
                'update_interval_minutes': self.update_interval / 60,
                'collection_task_running': self.collection_task and not self.collection_task.done() if self.collection_task else False
            },
            'collectors': {}
        }
        
        # Get status from each collector
        for name, collector in self.collectors.items():
            status['collectors'][name] = collector.get_status()
        
        return status
    
    def get_summary(self) -> str:
        """Get human-readable service summary."""
        status = self.get_status()
        
        summary = "AEMO Data Service Status (Unified Timing):\n"
        summary += f"  Running: {status['service']['running']}\n"
        summary += f"  Update interval: {status['service']['update_interval_minutes']:.1f} minutes\n"
        summary += f"  Cycle count: {status['service']['cycle_count']}\n"
        
        if status['service']['start_time']:
            summary += f"  Started: {status['service']['start_time']}\n"
            summary += f"  Uptime: {status['service']['uptime_seconds']:.0f} seconds\n"
        
        if status['service']['last_cycle']:
            summary += f"  Last cycle: {status['service']['last_cycle']}\n"
        
        summary += f"  Collection task: {'Running' if status['service']['collection_task_running'] else 'Stopped'}\n"
        summary += "\nCollectors:\n"
        
        for name, collector_status in status['collectors'].items():
            summary += f"  {name}:\n"
            summary += f"    Records: {collector_status['total_records']:,}\n"
            summary += f"    File size: {collector_status['file_size_mb']} MB\n"
            summary += f"    Errors: {collector_status['error_count']}\n"
            
            if collector_status.get('date_range'):
                summary += f"    Date range: {collector_status['date_range']['start']} to {collector_status['date_range']['end']}\n"
        
        return summary
    
    async def save_status_report(self, filepath: Optional[Path] = None) -> None:
        """Save detailed status report to file."""
        if filepath is None:
            filepath = config.BASE_PATH / 'service_status.json'
        
        status = self.get_status()
        
        try:
            with open(filepath, 'w') as f:
                json.dump(status, f, indent=2, default=str)
            
            logger.info(f"Status report saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving status report: {e}")


async def main():
    """Main entry point for the data service."""
    service = AEMODataService()
    
    try:
        # Check if we should run once or continuously
        if len(sys.argv) > 1 and sys.argv[1] == '--once':
            print("Running all collectors once...")
            results = await service.run_once_all()
            
            print("\nResults:")
            for name, success in results.items():
                status = "✅ Success" if success else "❌ Failed"
                print(f"  {name}: {status}")
            
            print(f"\nService Status:")
            print(service.get_summary())
            
        else:
            # Run continuously
            print("Starting AEMO Data Service (Ctrl+C to stop)...")
            await service.start()
    
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Service error: {e}")
        raise
    finally:
        if service.is_running:
            await service.stop()


if __name__ == "__main__":
    # Run the service
    asyncio.run(main())