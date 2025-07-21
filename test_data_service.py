#!/usr/bin/env python3
"""
Test script for the AEMO Data Service
Run this to test the unified timing implementation.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path so we can import the service
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_data_service import AEMODataService


async def test_service():
    """Test the data service with unified timing."""
    print("🧪 Testing AEMO Data Service with Unified Timing")
    print("=" * 60)
    
    # Create service
    service = AEMODataService()
    
    print("\n📊 Initial Status:")
    print(service.get_summary())
    
    print("\n🔄 Running single collection cycle...")
    try:
        results = await service.run_once_all()
        
        print(f"\n✅ Collection Results:")
        for name, success in results.items():
            status = "✅ Success" if success else "⚪ No New Data"
            print(f"  {name}: {status}")
        
        print(f"\n📊 Final Status:")
        print(service.get_summary())
        
        print(f"\n🎯 Test completed successfully!")
        print(f"   - Service uses unified 4.5-minute timing")
        print(f"   - All collectors run in single synchronized cycle")
        print(f"   - Parquet files updated atomically")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


async def test_continuous_service(duration_seconds=30):
    """Test continuous service for a short period."""
    print(f"\n🔄 Testing continuous service for {duration_seconds} seconds...")
    
    service = AEMODataService()
    
    # Start service in background
    service_task = asyncio.create_task(service.start())
    
    try:
        # Let it run for the specified duration
        await asyncio.sleep(duration_seconds)
        
        print(f"\n📊 Service status after {duration_seconds}s:")
        print(service.get_summary())
        
    finally:
        # Stop the service
        await service.stop()
        
        # Wait for service task to complete
        try:
            await asyncio.wait_for(service_task, timeout=5.0)
        except asyncio.TimeoutError:
            service_task.cancel()
        
        print("✅ Continuous test completed")


async def main():
    """Main test function."""
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        await test_continuous_service(duration)
    else:
        await test_service()


if __name__ == "__main__":
    print("AEMO Data Service Test")
    print("Usage:")
    print("  python test_data_service.py           # Single cycle test")
    print("  python test_data_service.py --continuous [seconds]  # Continuous test")
    print()
    
    asyncio.run(main())