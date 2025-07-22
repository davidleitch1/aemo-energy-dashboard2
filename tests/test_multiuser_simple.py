#!/usr/bin/env python3
"""
Simple Multi-User Dashboard Test
Tests concurrent access to dashboard backend
"""

import threading
import time
import psutil
import requests
from datetime import datetime
import pandas as pd
import os

# Configuration
DASHBOARD_PORT = 5008
NUM_USERS = 4
TEST_DURATION = 60  # 1 minute for quick test
REQUESTS_PER_USER = 20

class TestResults:
    """Collect test results"""
    def __init__(self):
        self.start_time = datetime.now()
        self.errors = []
        self.response_times = []
        self.memory_samples = []
        self.lock = threading.Lock()
        
    def add_error(self, user_id, error):
        with self.lock:
            self.errors.append({
                'timestamp': datetime.now(),
                'user_id': user_id,
                'error': str(error)
            })
            
    def add_response_time(self, user_id, endpoint, response_time):
        with self.lock:
            self.response_times.append({
                'timestamp': datetime.now(),
                'user_id': user_id,
                'endpoint': endpoint,
                'response_time_ms': response_time * 1000
            })
            
    def add_memory_sample(self, memory_mb):
        with self.lock:
            self.memory_samples.append({
                'timestamp': datetime.now(),
                'memory_mb': memory_mb
            })

def monitor_memory(results, stop_event):
    """Monitor memory usage in background"""
    # Find the dashboard process
    dashboard_pid = None
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline', [])
            if cmdline and any('gen_dash.py' in arg for arg in cmdline):
                dashboard_pid = proc.info['pid']
                break
        except:
            continue
            
    if not dashboard_pid:
        print("WARNING: Could not find dashboard process for memory monitoring")
        return
        
    process = psutil.Process(dashboard_pid)
    print(f"Monitoring dashboard process PID: {dashboard_pid}")
    
    while not stop_event.is_set():
        try:
            memory_mb = process.memory_info().rss / 1024 / 1024
            results.add_memory_sample(memory_mb)
        except:
            pass
        time.sleep(1)

def simulate_user(user_id, results, duration):
    """Simulate a single user making requests"""
    print(f"User {user_id}: Starting requests")
    
    endpoints = [
        # Simulate different dashboard operations
        f"http://localhost:{DASHBOARD_PORT}/",  # Main page
        # Add more endpoints if your dashboard exposes them
    ]
    
    start_time = time.time()
    request_count = 0
    
    while time.time() - start_time < duration:
        for endpoint in endpoints:
            try:
                req_start = time.time()
                response = requests.get(endpoint, timeout=30)
                req_time = time.time() - req_start
                
                if response.status_code == 200:
                    results.add_response_time(user_id, endpoint, req_time)
                else:
                    results.add_error(user_id, f"HTTP {response.status_code}")
                    
                request_count += 1
                
                # Simulate user think time
                time.sleep(1)
                
            except Exception as e:
                results.add_error(user_id, str(e))
                
        if request_count >= REQUESTS_PER_USER:
            break
            
    print(f"User {user_id}: Completed {request_count} requests")

def run_test():
    """Run the multi-user test"""
    print(f"\n{'='*60}")
    print(f"Multi-User Dashboard Test (Simple)")
    print(f"{'='*60}")
    print(f"Users: {NUM_USERS}")
    print(f"Duration: up to {TEST_DURATION} seconds")
    print(f"Requests per user: up to {REQUESTS_PER_USER}")
    print(f"{'='*60}\n")
    
    # Check dashboard is running
    try:
        response = requests.get(f"http://localhost:{DASHBOARD_PORT}", timeout=5)
        print(f"Dashboard status: {response.status_code}")
    except Exception as e:
        print(f"ERROR: Cannot connect to dashboard on port {DASHBOARD_PORT}: {e}")
        print("Please ensure the dashboard is running with: .venv/bin/python run_dashboard_duckdb.py")
        return
        
    # Initialize results collector
    results = TestResults()
    
    # Start memory monitoring
    stop_event = threading.Event()
    memory_thread = threading.Thread(
        target=monitor_memory, 
        args=(results, stop_event)
    )
    memory_thread.start()
    
    # Create user threads
    threads = []
    test_start = time.time()
    
    for i in range(NUM_USERS):
        thread = threading.Thread(
            target=simulate_user,
            args=(i + 1, results, TEST_DURATION)
        )
        threads.append(thread)
        thread.start()
        # Stagger starts slightly
        time.sleep(0.5)
    
    # Wait for all users to complete
    for thread in threads:
        thread.join()
        
    test_end = time.time()
    
    # Stop memory monitoring
    stop_event.set()
    memory_thread.join()
    
    # Generate report
    print(f"\n{'='*60}")
    print(f"Test Results")
    print(f"{'='*60}")
    print(f"Test duration: {test_end - test_start:.1f} seconds")
    
    # Error summary
    print(f"\nErrors: {len(results.errors)}")
    if results.errors:
        for error in results.errors[:5]:  # Show first 5 errors
            print(f"  User {error['user_id']}: {error['error']}")
            
    # Response time summary
    if results.response_times:
        df_resp = pd.DataFrame(results.response_times)
        print(f"\nResponse Times:")
        print(f"  Total requests: {len(df_resp)}")
        print(f"  Average: {df_resp['response_time_ms'].mean():.1f} ms")
        print(f"  Median: {df_resp['response_time_ms'].median():.1f} ms")
        print(f"  95th percentile: {df_resp['response_time_ms'].quantile(0.95):.1f} ms")
        print(f"  Max: {df_resp['response_time_ms'].max():.1f} ms")
        
        # Per-user summary
        print(f"\nPer-user request counts:")
        user_counts = df_resp.groupby('user_id').size()
        for user_id, count in user_counts.items():
            print(f"  User {user_id}: {count} requests")
            
    # Memory summary
    if results.memory_samples:
        df_mem = pd.DataFrame(results.memory_samples)
        print(f"\nMemory Usage:")
        print(f"  Start: {df_mem['memory_mb'].iloc[0]:.1f} MB")
        print(f"  Peak: {df_mem['memory_mb'].max():.1f} MB")
        print(f"  Final: {df_mem['memory_mb'].iloc[-1]:.1f} MB")
        print(f"  Increase: {df_mem['memory_mb'].iloc[-1] - df_mem['memory_mb'].iloc[0]:.1f} MB")
        
        # Save memory history
        df_mem.to_csv('multiuser_memory_simple.csv', index=False)
        print(f"\nMemory history saved to: multiuser_memory_simple.csv")
        
    # Overall assessment
    print(f"\nAssessment:")
    if len(results.errors) == 0:
        print("  ✓ No errors - concurrent access working well")
    else:
        print(f"  ⚠ {len(results.errors)} errors detected")
        
    if results.memory_samples:
        memory_increase = df_mem['memory_mb'].iloc[-1] - df_mem['memory_mb'].iloc[0]
        if memory_increase < 100:
            print("  ✓ Memory usage stable")
        else:
            print(f"  ⚠ Memory increased by {memory_increase:.1f} MB")
            
    if results.response_times:
        avg_response = df_resp['response_time_ms'].mean()
        if avg_response < 1000:
            print(f"  ✓ Good response times (avg {avg_response:.0f} ms)")
        else:
            print(f"  ⚠ Slow response times (avg {avg_response:.0f} ms)")
            
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    run_test()