#!/usr/bin/env python3
"""
Test the safe dashboard implementation multiple times
"""

import subprocess
import time
import sys
from datetime import datetime

NUM_TESTS = 5
STARTUP_TIMEOUT = 30

def test_dashboard_startup(test_num, script_name):
    """Test a single dashboard startup"""
    print(f"\n{'='*60}")
    print(f"Test {test_num}: Starting {script_name} at {datetime.now()}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        process = subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        success = False
        hang_detected = False
        output_lines = []
        last_line_time = time.time()
        
        while True:
            if process.poll() is not None:
                print(f"Process exited with code: {process.returncode}")
                break
            
            # Read output
            line = process.stdout.readline()
            if line:
                output_lines.append(line.strip())
                print(f"[{time.time() - start_time:.2f}s] {line.strip()}")
                last_line_time = time.time()
                
                # Check for success
                if "Navigate to: http://localhost:" in line or "Dashboard will be available at" in line:
                    success = True
                    print(f"✅ Dashboard started successfully in {time.time() - start_time:.2f} seconds")
                    break
                
                # Check for retry messages
                if "retry" in line.lower() or "waiting" in line.lower():
                    print("⚠️  Retry logic activated - handling concurrent access")
            
            # Check timeout
            if time.time() - start_time > STARTUP_TIMEOUT:
                hang_detected = True
                print(f"❌ TIMEOUT after {STARTUP_TIMEOUT} seconds")
                break
            
            # Check for hang after initialization message
            if output_lines and (time.time() - last_line_time) > 10:
                if any("Initializing" in line for line in output_lines[-5:]):
                    hang_detected = True
                    print(f"❌ HANG DETECTED: No output for 10 seconds")
                    break
            
            time.sleep(0.1)
        
        # Terminate process
        if process.poll() is None:
            process.terminate()
            time.sleep(1)
            if process.poll() is None:
                process.kill()
        
        return {
            'success': success,
            'hang': hang_detected,
            'duration': time.time() - start_time
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            'success': False,
            'hang': False,
            'duration': time.time() - start_time,
            'error': str(e)
        }

def main():
    """Test both regular and safe versions"""
    print("Dashboard Startup Comparison Test")
    print("Testing regular vs safe implementation")
    
    # Test safe version
    print(f"\n{'='*70}")
    print("TESTING SAFE VERSION (with retry logic)")
    print(f"{'='*70}")
    
    safe_results = []
    for i in range(1, NUM_TESTS + 1):
        result = test_dashboard_startup(i, 'run_dashboard_duckdb_safe.py')
        safe_results.append(result)
        if i < NUM_TESTS:
            time.sleep(3)
    
    # Summary
    print(f"\n{'='*70}")
    print("TEST SUMMARY - SAFE VERSION")
    print(f"{'='*70}")
    
    safe_success = sum(1 for r in safe_results if r['success'])
    safe_hangs = sum(1 for r in safe_results if r['hang'])
    
    print(f"Total tests: {NUM_TESTS}")
    print(f"Successful: {safe_success} ({safe_success/NUM_TESTS*100:.0f}%)")
    print(f"Hangs: {safe_hangs}")
    print(f"Average startup time: {sum(r['duration'] for r in safe_results if r['success'])/max(1, safe_success):.2f}s")
    
    if safe_hangs == 0 and safe_success == NUM_TESTS:
        print("\n✅ SAFE VERSION RESOLVED THE INTERMITTENT HANG ISSUE!")
    elif safe_hangs < NUM_TESTS:
        print(f"\n⚠️  Safe version reduced hangs but didn't eliminate them completely")

if __name__ == "__main__":
    main()