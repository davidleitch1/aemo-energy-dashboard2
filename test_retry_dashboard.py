#!/usr/bin/env python3
"""
Test dashboard with retry logic multiple times
"""

import subprocess
import time
import sys
from datetime import datetime
import os

NUM_TESTS = 10  # More tests to catch intermittent issues
STARTUP_TIMEOUT = 30

def test_dashboard_startup(test_num):
    """Test a single dashboard startup"""
    print(f"\n{'='*60}")
    print(f"Test {test_num}: Starting dashboard at {datetime.now()}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Use the patched version
    process = subprocess.Popen(
        [sys.executable, 'run_dashboard_with_retry.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    success = False
    hang_detected = False
    retry_count = 0
    output_lines = []
    
    while True:
        if process.poll() is not None:
            print(f"Process exited with code: {process.returncode}")
            break
        
        line = process.stdout.readline()
        if line:
            output_lines.append(line.strip())
            elapsed = time.time() - start_time
            print(f"[{elapsed:.2f}s] {line.strip()}")
            
            # Track retry attempts
            if "retry" in line.lower() or "waiting" in line.lower():
                retry_count += 1
                print(f"   ⚠️  Retry detected (count: {retry_count})")
            
            # Check for success
            if "Navigate to: http://localhost:" in line or "Press Ctrl+C to stop" in line:
                success = True
                print(f"✅ Dashboard started successfully in {elapsed:.2f} seconds")
                if retry_count > 0:
                    print(f"   Note: {retry_count} retries were needed")
                break
        
        # Check timeout
        if time.time() - start_time > STARTUP_TIMEOUT:
            hang_detected = True
            print(f"❌ TIMEOUT after {STARTUP_TIMEOUT} seconds")
            break
        
        time.sleep(0.1)
    
    # Cleanup
    if process.poll() is None:
        process.terminate()
        time.sleep(1)
        if process.poll() is None:
            process.kill()
    
    return {
        'success': success,
        'hang': hang_detected,
        'duration': time.time() - start_time,
        'retry_count': retry_count
    }

def main():
    """Run multiple tests"""
    print("Testing Dashboard with Retry Logic")
    print(f"Running {NUM_TESTS} tests...")
    
    results = []
    
    for i in range(1, NUM_TESTS + 1):
        result = test_dashboard_startup(i)
        results.append(result)
        
        if i < NUM_TESTS:
            print("\nWaiting 3 seconds before next test...")
            time.sleep(3)
    
    # Summary
    print(f"\n{'='*70}")
    print("TEST SUMMARY")
    print(f"{'='*70}")
    
    successful = sum(1 for r in results if r['success'])
    hangs = sum(1 for r in results if r['hang'])
    total_retries = sum(r['retry_count'] for r in results)
    
    print(f"Total tests: {NUM_TESTS}")
    print(f"Successful: {successful} ({successful/NUM_TESTS*100:.0f}%)")
    print(f"Hangs: {hangs}")
    print(f"Total retries across all tests: {total_retries}")
    
    if successful > 0:
        avg_time = sum(r['duration'] for r in results if r['success']) / successful
        print(f"Average successful startup time: {avg_time:.2f}s")
    
    # Analyze retry patterns
    tests_with_retries = sum(1 for r in results if r['retry_count'] > 0)
    if tests_with_retries > 0:
        print(f"\nTests that needed retries: {tests_with_retries}/{NUM_TESTS}")
        print("This indicates concurrent file access is occurring")
    
    if hangs == 0 and successful == NUM_TESTS:
        print("\n✅ SUCCESS! All tests passed with retry logic")
        print("The retry mechanism successfully handles concurrent file access")
    elif hangs < NUM_TESTS / 2:
        print("\n⚠️  PARTIAL SUCCESS: Retry logic reduced but didn't eliminate all hangs")
    else:
        print("\n❌ Retry logic did not resolve the issue")

if __name__ == "__main__":
    main()