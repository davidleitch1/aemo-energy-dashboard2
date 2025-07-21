#!/usr/bin/env python3
"""
Test script to compare memory usage between original and optimized API services
"""

import requests
import subprocess
import time
import os
import signal
import sys

def start_server(script_name, port):
    """Start a FastAPI server and return the process"""
    env = os.environ.copy()
    env['FASTAPI_PORT'] = str(port)
    
    process = subprocess.Popen(
        [sys.executable, script_name],
        cwd='/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/src',
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for server to start
    time.sleep(10)
    
    return process

def test_server(port, server_name):
    """Test a server and get memory usage"""
    base_url = f"http://localhost:{port}"
    
    print(f"\n{'='*60}")
    print(f"Testing {server_name} on port {port}")
    print('='*60)
    
    try:
        # Test health endpoint
        response = requests.get(f"{base_url}/api/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            memory_mb = data['memory_usage_mb']
            print(f"✓ Memory usage: {memory_mb:.1f} MB")
            print(f"✓ Data loaded: {data['data_loaded']}")
            
            # Test metadata endpoint
            response = requests.get(f"{base_url}/api/metadata", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Date ranges available:")
                for dtype, info in data['date_ranges'].items():
                    print(f"  - {dtype}: {info['records']:,} records")
            
            # Test a generation query
            response = requests.get(
                f"{base_url}/api/generation/by-fuel",
                params={
                    "start_date": "2025-07-01T00:00:00",
                    "end_date": "2025-07-02T00:00:00",
                    "resolution": "30min"
                },
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Generation query returned {data['count']} records")
            
            return memory_mb
            
    except requests.ConnectionError:
        print(f"✗ Could not connect to {server_name}")
    except Exception as e:
        print(f"✗ Error testing {server_name}: {e}")
    
    return None

def main():
    """Main test function"""
    print("=== AEMO Dashboard Memory Usage Comparison ===")
    
    # Test original service
    print("\nStarting original service...")
    original_process = start_server('main_fastapi.py', 8000)
    
    try:
        original_memory = test_server(8000, "Original Service")
        
        # Stop original service
        original_process.terminate()
        original_process.wait()
        time.sleep(2)
        
        # Test optimized service
        print("\nStarting optimized service...")
        optimized_process = start_server('main_fastapi_optimized.py', 8001)
        
        try:
            optimized_memory = test_server(8001, "Optimized Service")
            
            # Summary
            print(f"\n{'='*60}")
            print("MEMORY USAGE COMPARISON")
            print('='*60)
            
            if original_memory and optimized_memory:
                print(f"Original service:  {original_memory:8.1f} MB")
                print(f"Optimized service: {optimized_memory:8.1f} MB")
                print(f"Memory reduction:  {original_memory - optimized_memory:8.1f} MB ({(1 - optimized_memory/original_memory)*100:.1f}%)")
            else:
                print("Could not compare memory usage - one or both services failed")
            
            print('='*60)
            
        finally:
            # Stop optimized service
            optimized_process.terminate()
            optimized_process.wait()
            
    finally:
        # Ensure original service is stopped
        try:
            original_process.terminate()
        except:
            pass

if __name__ == "__main__":
    main()