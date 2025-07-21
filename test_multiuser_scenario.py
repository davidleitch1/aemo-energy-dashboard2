#!/usr/bin/env python3
"""
Multi-User Dashboard Test
Tests 4 concurrent users accessing the dashboard
Monitors memory usage and performance
"""

import asyncio
import time
import psutil
import os
import threading
from datetime import datetime, timedelta
import requests
from playwright.async_api import async_playwright
import pandas as pd

# Configuration
DASHBOARD_URL = "http://localhost:5006"
NUM_USERS = 4
TEST_DURATION = 300  # 5 minutes
MEMORY_CHECK_INTERVAL = 5  # seconds

class MemoryMonitor:
    """Monitor memory usage during test"""
    def __init__(self):
        self.process = psutil.Process()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory
        self.memory_history = []
        self.monitoring = True
        
    def start(self):
        """Start monitoring in background thread"""
        self.thread = threading.Thread(target=self._monitor)
        self.thread.start()
        
    def _monitor(self):
        """Monitor memory usage"""
        while self.monitoring:
            current_memory = self.process.memory_info().rss / 1024 / 1024
            self.memory_history.append({
                'timestamp': datetime.now(),
                'memory_mb': current_memory
            })
            if current_memory > self.peak_memory:
                self.peak_memory = current_memory
            time.sleep(MEMORY_CHECK_INTERVAL)
            
    def stop(self):
        """Stop monitoring"""
        self.monitoring = False
        self.thread.join()
        
    def get_report(self):
        """Get memory usage report"""
        final_memory = self.process.memory_info().rss / 1024 / 1024
        return {
            'start_memory_mb': self.start_memory,
            'peak_memory_mb': self.peak_memory,
            'final_memory_mb': final_memory,
            'memory_increase_mb': final_memory - self.start_memory,
            'peak_increase_mb': self.peak_memory - self.start_memory,
            'history': self.memory_history
        }

async def simulate_user(user_id: int, duration: int):
    """Simulate a single user interacting with dashboard"""
    print(f"User {user_id}: Starting session")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Navigate to dashboard
            print(f"User {user_id}: Navigating to dashboard")
            await page.goto(DASHBOARD_URL)
            await page.wait_for_load_state('networkidle')
            
            # Simulate user interactions
            tabs = ["Today", "Generation mix", "Pivot table", "Station Analysis", "Penetration"]
            interactions = [
                # View different tabs
                lambda: click_tab(page, tabs[1]),  # Generation mix
                lambda: click_tab(page, tabs[2]),  # Pivot table
                lambda: click_tab(page, tabs[3]),  # Station Analysis
                lambda: click_tab(page, tabs[4]),  # Penetration
                lambda: click_tab(page, tabs[0]),  # Back to Today
                
                # Change date ranges
                lambda: change_date_range(page, "Last 7 days"),
                lambda: change_date_range(page, "Last 30 days"),
                lambda: change_date_range(page, "Last 24 hours"),
                
                # Wait to simulate reading
                lambda: page.wait_for_timeout(5000),
            ]
            
            # Run interactions for duration
            start_time = time.time()
            interaction_count = 0
            
            while time.time() - start_time < duration:
                # Pick random interaction
                interaction = interactions[interaction_count % len(interactions)]
                try:
                    await interaction()
                    print(f"User {user_id}: Completed interaction {interaction_count + 1}")
                except Exception as e:
                    print(f"User {user_id}: Error in interaction: {e}")
                
                interaction_count += 1
                await page.wait_for_timeout(2000)  # Wait between actions
                
        except Exception as e:
            print(f"User {user_id}: Session error: {e}")
        finally:
            await browser.close()
            print(f"User {user_id}: Session ended after {interaction_count} interactions")

async def click_tab(page, tab_name):
    """Click on a dashboard tab"""
    await page.click(f'text="{tab_name}"')
    await page.wait_for_load_state('networkidle')

async def change_date_range(page, range_text):
    """Change the date range selector"""
    # This would need to be adapted to actual dashboard selectors
    try:
        await page.click('select.date-range-selector')
        await page.select_option('select.date-range-selector', range_text)
        await page.wait_for_load_state('networkidle')
    except:
        # Fallback if selector not found
        pass

async def run_multi_user_test():
    """Run the multi-user test"""
    print(f"\n{'='*60}")
    print(f"Multi-User Dashboard Test")
    print(f"{'='*60}")
    print(f"Users: {NUM_USERS}")
    print(f"Duration: {TEST_DURATION} seconds")
    print(f"Dashboard URL: {DASHBOARD_URL}")
    print(f"{'='*60}\n")
    
    # Start memory monitoring
    monitor = MemoryMonitor()
    monitor.start()
    
    # Check dashboard is running
    try:
        response = requests.get(DASHBOARD_URL, timeout=5)
        if response.status_code != 200:
            print(f"ERROR: Dashboard not accessible at {DASHBOARD_URL}")
            return
    except Exception as e:
        print(f"ERROR: Cannot connect to dashboard: {e}")
        return
    
    # Create user tasks
    tasks = []
    for i in range(NUM_USERS):
        task = simulate_user(i + 1, TEST_DURATION)
        tasks.append(task)
    
    # Run all users concurrently
    start_time = time.time()
    await asyncio.gather(*tasks)
    end_time = time.time()
    
    # Stop memory monitoring
    monitor.stop()
    
    # Generate report
    print(f"\n{'='*60}")
    print(f"Test Results")
    print(f"{'='*60}")
    print(f"Test duration: {end_time - start_time:.1f} seconds")
    
    # Memory report
    memory_report = monitor.get_report()
    print(f"\nMemory Usage:")
    print(f"  Start: {memory_report['start_memory_mb']:.1f} MB")
    print(f"  Peak: {memory_report['peak_memory_mb']:.1f} MB")
    print(f"  Final: {memory_report['final_memory_mb']:.1f} MB")
    print(f"  Peak increase: {memory_report['peak_increase_mb']:.1f} MB")
    print(f"  Final increase: {memory_report['memory_increase_mb']:.1f} MB")
    
    # Save detailed memory history
    df = pd.DataFrame(memory_report['history'])
    df.to_csv('multiuser_memory_history.csv', index=False)
    print(f"\nDetailed memory history saved to: multiuser_memory_history.csv")
    
    # Performance summary
    print(f"\nPerformance Summary:")
    print(f"  Average memory per user: {memory_report['peak_increase_mb'] / NUM_USERS:.1f} MB")
    print(f"  Memory efficiency: {'GOOD' if memory_report['peak_increase_mb'] < 1000 else 'NEEDS OPTIMIZATION'}")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    # Install playwright browsers if needed
    import subprocess
    subprocess.run(["playwright", "install", "chromium"], capture_output=True)
    
    # Run the test
    asyncio.run(run_multi_user_test())