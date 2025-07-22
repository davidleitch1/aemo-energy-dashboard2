#!/usr/bin/env python3
"""
Test the fast startup implementation
"""
import time
import subprocess
import sys
import os
from pathlib import Path

def test_startup_time(script_name, description):
    """Test startup time for a given script"""
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"Script: {script_name}")
    print(f"{'='*60}")
    
    # Measure startup time
    start_time = time.time()
    
    # Run the script and capture output
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(__file__).parent)
    
    try:
        # Run for 5 seconds then kill
        proc = subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        
        # Wait for startup messages
        startup_complete = False
        timeout = 10
        start = time.time()
        
        while time.time() - start < timeout:
            if proc.poll() is not None:
                break
                
            # Check output for startup completion
            try:
                line = proc.stdout.readline()
                if line:
                    print(f"  > {line.strip()}")
                    if "Dashboard will be available" in line or "Startup time:" in line:
                        startup_complete = True
                        startup_duration = time.time() - start_time
                        print(f"\n✅ Startup completed in {startup_duration:.2f} seconds")
                        break
            except:
                pass
        
        # Kill the process
        proc.terminate()
        proc.wait()
        
        if not startup_complete:
            print(f"\n❌ Startup did not complete within {timeout} seconds")
            # Print any error output
            stderr = proc.stderr.read()
            if stderr:
                print(f"Errors:\n{stderr}")
        
        return startup_duration if startup_complete else None
        
    except Exception as e:
        print(f"\n❌ Error running script: {e}")
        return None

# Test different startup methods
print("AEMO Dashboard Startup Performance Test")
print("="*60)

results = []

# Test 1: Original DuckDB startup
result1 = test_startup_time(
    "run_dashboard_duckdb.py",
    "Original DuckDB Dashboard"
)
if result1:
    results.append(("Original DuckDB", result1))

# Test 2: Fast startup version
result2 = test_startup_time(
    "run_dashboard_fast.py",
    "Fast Startup Dashboard"
)
if result2:
    results.append(("Fast Startup", result2))

# Summary
print(f"\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")

if results:
    for name, duration in results:
        print(f"{name}: {duration:.2f} seconds")
    
    if len(results) == 2:
        improvement = (results[0][1] - results[1][1]) / results[0][1] * 100
        speedup = results[0][1] / results[1][1]
        print(f"\nImprovement: {improvement:.1f}% faster")
        print(f"Speedup: {speedup:.1f}x")
        
        if results[1][1] <= 3.0:
            print("\n✅ TARGET ACHIEVED: Startup time <= 3 seconds")
        else:
            print(f"\n⚠️  Target not met. Need to reduce by {results[1][1] - 3.0:.1f} more seconds")
else:
    print("No successful tests completed")