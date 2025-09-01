#!/usr/bin/env python3
"""
Test script to diagnose intermittent dashboard startup hangs
"""

import sys
import time
import subprocess
import signal
import os
from datetime import datetime
import psutil

# Number of test runs
NUM_TESTS = 5
STARTUP_TIMEOUT = 30  # seconds

def test_dashboard_startup(test_num):
    """Test a single dashboard startup"""
    print(f"\n{'='*60}")
    print(f"Test {test_num}: Starting dashboard at {datetime.now()}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Set environment variable to use diagnostic mode
    env = os.environ.copy()
    env['DASHBOARD_DEBUG'] = 'true'
    env['USE_DUCKDB'] = 'true'
    
    # Start the dashboard process
    try:
        process = subprocess.Popen(
            [sys.executable, 'run_dashboard_duckdb.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env
        )
        
        # Monitor output for success or hang
        success = False
        hang_detected = False
        last_output_time = time.time()
        output_lines = []
        
        while True:
            # Check if process is still running
            if process.poll() is not None:
                print(f"Process exited with code: {process.returncode}")
                break
            
            # Try to read output (non-blocking)
            try:
                line = process.stdout.readline()
                if line:
                    output_lines.append(line.strip())
                    print(f"[{time.time() - start_time:.2f}s] {line.strip()}")
                    last_output_time = time.time()
                    
                    # Check for success indicators
                    if "Navigate to: http://localhost:" in line:
                        success = True
                        print(f"✅ Dashboard started successfully in {time.time() - start_time:.2f} seconds")
                        break
                    
                    # Check for hang indicator
                    if "Initializing dashboard components..." in line:
                        print("⚠️  Found initialization message - monitoring for hang...")
            except:
                pass
            
            # Check for timeout
            elapsed = time.time() - start_time
            if elapsed > STARTUP_TIMEOUT:
                hang_detected = True
                print(f"❌ TIMEOUT: Dashboard failed to start within {STARTUP_TIMEOUT} seconds")
                break
            
            # Check for output stall (no output for 10 seconds after initialization)
            if output_lines and (time.time() - last_output_time) > 10:
                if any("Initializing dashboard components" in line for line in output_lines[-5:]):
                    hang_detected = True
                    print(f"❌ HANG DETECTED: No output for 10 seconds after initialization message")
                    break
            
            time.sleep(0.1)
        
        # Get process info before terminating
        if hang_detected:
            try:
                p = psutil.Process(process.pid)
                print(f"\nProcess info at hang:")
                print(f"  CPU: {p.cpu_percent(interval=1)}%")
                print(f"  Memory: {p.memory_info().rss / 1024 / 1024:.1f} MB")
                print(f"  Threads: {p.num_threads()}")
                
                # Get thread info if possible
                print("\nThread info:")
                for thread in p.threads():
                    print(f"  Thread {thread.id}: CPU time = {thread.user_time + thread.system_time:.2f}s")
            except:
                pass
        
        # Terminate the process
        if process.poll() is None:
            print("\nTerminating dashboard process...")
            process.terminate()
            time.sleep(2)
            if process.poll() is None:
                process.kill()
        
        return {
            'test_num': test_num,
            'success': success,
            'hang_detected': hang_detected,
            'duration': time.time() - start_time,
            'last_output': output_lines[-5:] if output_lines else []
        }
        
    except Exception as e:
        print(f"Error during test: {e}")
        return {
            'test_num': test_num,
            'success': False,
            'hang_detected': False,
            'duration': time.time() - start_time,
            'error': str(e)
        }

def main():
    """Run multiple startup tests"""
    print("Dashboard Startup Diagnostic Test")
    print(f"Running {NUM_TESTS} startup tests...")
    print(f"Timeout per test: {STARTUP_TIMEOUT} seconds")
    
    results = []
    
    for i in range(1, NUM_TESTS + 1):
        result = test_dashboard_startup(i)
        results.append(result)
        
        # Wait between tests
        if i < NUM_TESTS:
            print("\nWaiting 3 seconds before next test...")
            time.sleep(3)
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    successful = sum(1 for r in results if r['success'])
    hangs = sum(1 for r in results if r['hang_detected'])
    failures = NUM_TESTS - successful
    
    print(f"Total tests: {NUM_TESTS}")
    print(f"Successful: {successful} ({successful/NUM_TESTS*100:.0f}%)")
    print(f"Hangs detected: {hangs}")
    print(f"Other failures: {failures - hangs}")
    
    if hangs > 0:
        print(f"\n⚠️  INTERMITTENT HANG DETECTED in {hangs}/{NUM_TESTS} tests!")
        print("\nHang details:")
        for r in results:
            if r['hang_detected']:
                print(f"\nTest {r['test_num']}:")
                print(f"  Duration before timeout: {r['duration']:.2f}s")
                print(f"  Last output:")
                for line in r['last_output']:
                    print(f"    {line}")
    
    # Save detailed results
    with open('dashboard_startup_test_results.txt', 'w') as f:
        f.write(f"Dashboard Startup Test Results - {datetime.now()}\n")
        f.write(f"{'='*60}\n\n")
        for r in results:
            f.write(f"Test {r['test_num']}:\n")
            f.write(f"  Success: {r['success']}\n")
            f.write(f"  Hang: {r['hang_detected']}\n")
            f.write(f"  Duration: {r['duration']:.2f}s\n")
            if 'error' in r:
                f.write(f"  Error: {r['error']}\n")
            f.write("\n")

if __name__ == "__main__":
    main()