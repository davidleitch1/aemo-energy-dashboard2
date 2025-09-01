#!/usr/bin/env python3
"""
Check production parquet files for corruption or concurrent access issues
"""

import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
import time
import fcntl
import subprocess

PRODUCTION_DATA_PATH = "/Volumes/davidleitch/aemo_production/data"

def check_file_lock(file_path):
    """Check if file is currently being written to"""
    try:
        # Try to get exclusive lock (non-blocking)
        with open(file_path, 'rb') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            return False  # File is not locked
    except (IOError, OSError):
        return True  # File is locked (being written)

def check_collector_status():
    """Check if the data collector is currently running"""
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        processes = result.stdout
        collector_running = 'unified_collector.py' in processes or 'run_unified_collector.py' in processes
        return collector_running
    except:
        return None

def check_parquet_file_detailed(file_path, description):
    """Detailed check of a parquet file including multiple read attempts"""
    print(f"\n{'='*70}")
    print(f"Checking: {description}")
    print(f"Path: {file_path}")
    print(f"{'='*70}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ FILE NOT FOUND")
        return {'status': 'missing', 'error': 'File not found'}
    
    # Get file stats
    file_stats = os.stat(file_path)
    file_size_mb = file_stats.st_size / (1024 * 1024)
    mod_time = datetime.fromtimestamp(file_stats.st_mtime)
    
    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Last modified: {mod_time}")
    print(f"Age: {(datetime.now() - mod_time).total_seconds() / 60:.1f} minutes")
    
    # Check if file is locked
    is_locked = check_file_lock(file_path)
    if is_locked:
        print("⚠️  File appears to be locked (possibly being written)")
    
    results = {
        'status': 'unknown',
        'size_mb': file_size_mb,
        'mod_time': mod_time,
        'locked': is_locked,
        'attempts': []
    }
    
    # Try reading the file multiple times
    for attempt in range(3):
        if attempt > 0:
            print(f"\nAttempt {attempt + 1}/3 (waiting 2 seconds)...")
            time.sleep(2)
        
        try:
            start_time = time.time()
            
            # First try reading metadata only
            parquet_file = pq.ParquetFile(file_path)
            num_rows = parquet_file.metadata.num_rows
            num_columns = len(parquet_file.schema)
            
            print(f"✅ Metadata read successful")
            print(f"   Rows: {num_rows:,}")
            print(f"   Columns: {num_columns}")
            
            # Try reading first few rows
            df_sample = pd.read_parquet(file_path, engine='pyarrow').head(5)
            read_time = time.time() - start_time
            
            print(f"✅ Data read successful (time: {read_time:.2f}s)")
            
            # Check for recent data
            if 'settlementdate' in df_sample.columns:
                latest = pd.read_parquet(file_path, columns=['settlementdate']).max()['settlementdate']
                print(f"   Latest data: {latest}")
                data_age_mins = (datetime.now() - pd.to_datetime(latest)).total_seconds() / 60
                print(f"   Data age: {data_age_mins:.1f} minutes")
                
                if data_age_mins > 30:
                    print(f"⚠️  WARNING: Data is more than 30 minutes old")
                
                results['latest_data'] = latest
                results['data_age_mins'] = data_age_mins
            
            results['status'] = 'ok'
            results['attempts'].append({'attempt': attempt + 1, 'success': True, 'time': read_time})
            break
            
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            print(f"❌ ERROR: {error_msg}")
            
            results['attempts'].append({
                'attempt': attempt + 1,
                'success': False,
                'error': error_msg
            })
            
            # Check if file size changed (indicating active write)
            new_size = os.path.getsize(file_path) / (1024 * 1024)
            if new_size != file_size_mb:
                print(f"📝 File size changed: {file_size_mb:.2f} MB → {new_size:.2f} MB")
                results['size_changed'] = True
            
            if attempt == 2:  # Last attempt
                results['status'] = 'error'
                results['final_error'] = error_msg
    
    return results

def main():
    """Check all production parquet files"""
    print("AEMO Production Parquet File Integrity Check")
    print(f"Timestamp: {datetime.now()}")
    print(f"Production path: {PRODUCTION_DATA_PATH}")
    
    # Check if collector is running
    collector_running = check_collector_status()
    if collector_running is not None:
        print(f"\nData collector status: {'RUNNING ⚠️' if collector_running else 'Not running'}")
        if collector_running:
            print("Note: Files may be actively updated during this check")
    
    # List of files to check
    files_to_check = [
        "scada5.parquet",
        "scada30.parquet",
        "prices5.parquet",
        "prices30.parquet",
        "transmission5.parquet",
        "transmission30.parquet",
        "rooftop30.parquet"
    ]
    
    all_results = {}
    
    for filename in files_to_check:
        file_path = os.path.join(PRODUCTION_DATA_PATH, filename)
        result = check_parquet_file_detailed(file_path, filename)
        all_results[filename] = result
    
    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    
    # Count statuses
    ok_count = sum(1 for r in all_results.values() if r['status'] == 'ok')
    error_count = sum(1 for r in all_results.values() if r['status'] == 'error')
    missing_count = sum(1 for r in all_results.values() if r['status'] == 'missing')
    
    print(f"Files checked: {len(all_results)}")
    print(f"OK: {ok_count}")
    print(f"Errors: {error_count}")
    print(f"Missing: {missing_count}")
    
    # Detailed issues
    if error_count > 0:
        print("\n❌ FILES WITH ERRORS:")
        for filename, result in all_results.items():
            if result['status'] == 'error':
                print(f"\n{filename}:")
                print(f"  Final error: {result.get('final_error', 'Unknown')}")
                print(f"  Locked: {result.get('locked', False)}")
                print(f"  Size changed: {result.get('size_changed', False)}")
                
                # Show attempt history
                print("  Attempts:")
                for attempt in result['attempts']:
                    if attempt['success']:
                        print(f"    {attempt['attempt']}: Success ({attempt['time']:.2f}s)")
                    else:
                        print(f"    {attempt['attempt']}: Failed - {attempt['error']}")
    
    # Files with warnings
    old_data_files = []
    locked_files = []
    
    for filename, result in all_results.items():
        if result['status'] == 'ok':
            if result.get('data_age_mins', 0) > 30:
                old_data_files.append((filename, result['data_age_mins']))
            if result.get('locked', False):
                locked_files.append(filename)
    
    if old_data_files:
        print("\n⚠️  FILES WITH OLD DATA:")
        for filename, age in old_data_files:
            print(f"  {filename}: {age:.0f} minutes old")
    
    if locked_files:
        print("\n⚠️  LOCKED FILES:")
        for filename in locked_files:
            print(f"  {filename}")
    
    # Diagnosis
    print(f"\n{'='*70}")
    print("DIAGNOSIS")
    print(f"{'='*70}")
    
    if error_count > 0:
        print("🔍 Intermittent startup hangs are likely caused by:")
        print("   1. Concurrent file access - dashboard reading while updater is writing")
        print("   2. Corrupted files from interrupted writes")
        print("\nRECOMMENDED FIXES:")
        print("   1. Implement file locking or atomic writes in the data updater")
        print("   2. Add retry logic with exponential backoff in dashboard")
        print("   3. Use temporary files for writes, then atomic rename")
        print("   4. Consider using DuckDB's ability to read from multiple files")
    else:
        print("✅ All files appear healthy")
        print("   If hangs still occur, they may be due to:")
        print("   - Network/disk latency")
        print("   - Memory pressure")
        print("   - Other system resources")

if __name__ == "__main__":
    main()