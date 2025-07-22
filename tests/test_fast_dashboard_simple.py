#!/usr/bin/env python3
"""
Simple test of fast dashboard - run it and check basic functionality
"""
import os
import sys
import time
import subprocess
from pathlib import Path

print("Testing Fast Dashboard Launch and Basic Functionality")
print("=" * 60)

# Set up environment
env = os.environ.copy()
env['USE_DUCKDB'] = 'true'
env['DUCKDB_LAZY_VIEWS'] = 'true'
env['PYTHONPATH'] = str(Path(__file__).parent)

# Launch the dashboard
print("\n1. Launching fast dashboard...")
proc = subprocess.Popen(
    [sys.executable, 'run_dashboard_fast.py'],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    env=env
)

# Monitor output for 10 seconds
print("2. Monitoring startup output...")
start_time = time.time()
startup_complete = False
errors = []

while time.time() - start_time < 10:
    if proc.poll() is not None:
        break
    
    # Check stdout
    line = proc.stdout.readline()
    if line:
        print(f"   STDOUT: {line.strip()}")
        if "Dashboard will be available" in line or "localhost:5006" in line:
            startup_complete = True
    
    # Check stderr
    err_line = proc.stderr.readline()
    if err_line:
        print(f"   STDERR: {err_line.strip()}")
        errors.append(err_line.strip())

# Give it a moment to stabilize
time.sleep(2)

# Summary
print("\n" + "=" * 60)
print("TEST RESULTS")
print("=" * 60)

if startup_complete:
    print("✅ Dashboard started successfully")
    print(f"   Startup time: ~{time.time() - start_time:.1f} seconds")
else:
    print("❌ Dashboard failed to start properly")

if errors:
    print(f"\n⚠️  {len(errors)} errors/warnings detected:")
    for err in errors[:5]:  # Show first 5 errors
        print(f"   - {err}")
else:
    print("\n✅ No errors detected during startup")

print("\n" + "=" * 60)
print("MANUAL TESTING REQUIRED")
print("=" * 60)
print("\nThe dashboard should now be running at http://localhost:5006")
print("\nPlease manually test:")
print("1. ✓ Open http://localhost:5006 in your browser")
print("2. ✓ Check that the NEM Dashboard tab loads")
print("3. ✓ Click on each tab to verify they load")
print("4. ✓ Check that charts and tables display correctly")
print("5. ✓ Test time range selection")
print("6. ✓ Verify data updates when changing selections")

print("\nPress Ctrl+C when done testing...")

try:
    # Keep the dashboard running
    proc.wait()
except KeyboardInterrupt:
    print("\n\nShutting down dashboard...")
    proc.terminate()
    proc.wait()
    print("Dashboard stopped.")

# Clean up
if proc.poll() is None:
    proc.terminate()
    proc.wait()