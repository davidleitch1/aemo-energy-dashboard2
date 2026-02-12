#!/usr/bin/env python3
"""
Test script to verify auto-refresh and state preservation
"""

import os
import sys
import time
import threading
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def start_dashboard():
    """Start the dashboard in a separate thread"""
    from aemo_dashboard.generation.gen_dash import main
    main()

def test_state_preservation():
    """Test that dashboard state is preserved across refresh"""
    print("Starting dashboard state preservation test...")
    
    # Start dashboard in background thread
    dashboard_thread = threading.Thread(target=start_dashboard, daemon=True)
    dashboard_thread.start()
    
    # Wait for dashboard to start
    print("Waiting for dashboard to start...")
    time.sleep(10)
    
    # Create webdriver (requires Chrome/Chromium)
    try:
        from selenium.webdriver.chrome.options import Options
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in headless mode
        driver = webdriver.Chrome(options=chrome_options)
    except:
        print("Chrome driver not available, trying Firefox...")
        from selenium.webdriver.firefox.options import Options
        firefox_options = Options()
        firefox_options.add_argument('--headless')
        driver = webdriver.Firefox(options=firefox_options)
    
    try:
        # Navigate to dashboard
        driver.get("http://localhost:5008")
        
        # Wait for dashboard to load
        wait = WebDriverWait(driver, 30)
        
        # Wait for tabs to be present
        print("Waiting for dashboard tabs to load...")
        tabs = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "bk-tabs")))
        
        # Click on the "Prices" tab (index 2)
        print("Clicking on Prices tab...")
        tab_headers = driver.find_elements(By.CLASS_NAME, "bk-tab")
        if len(tab_headers) > 2:
            tab_headers[2].click()
            time.sleep(2)
            
            # Check if tab is active
            active_tab = driver.find_element(By.CLASS_NAME, "bk-tab-active")
            active_index = active_tab.get_attribute("data-index")
            print(f"Active tab index: {active_index}")
            
            # Execute JavaScript to save state
            print("Manually triggering state save...")
            driver.execute_script("saveDashboardState();")
            
            # Check localStorage
            saved_state = driver.execute_script("return localStorage.getItem('aemo_dashboard_state');")
            if saved_state:
                import json
                state = json.loads(saved_state)
                print(f"Saved state: {json.dumps(state, indent=2)}")
                
                # Verify tab index was saved
                if state.get('activeTab') == '2':
                    print("✓ Tab state saved correctly!")
                else:
                    print(f"✗ Tab state incorrect: expected '2', got '{state.get('activeTab')}'")
            else:
                print("✗ No state saved in localStorage")
        else:
            print(f"✗ Not enough tabs found: {len(tab_headers)}")
            
        # Test the restore function
        print("\nTesting state restoration...")
        driver.execute_script("""
            // Simulate clicking on first tab
            document.querySelectorAll('.bk-tab')[0].click();
        """)
        time.sleep(1)
        
        # Now restore state
        driver.execute_script("restoreDashboardState();")
        time.sleep(2)
        
        # Check if we're back on Prices tab
        active_tab = driver.find_element(By.CLASS_NAME, "bk-tab-active")
        final_index = active_tab.get_attribute("data-index")
        
        if final_index == '2':
            print("✓ State restoration successful!")
        else:
            print(f"✗ State restoration failed: on tab {final_index} instead of 2")
            
    except TimeoutException:
        print("✗ Timeout waiting for dashboard to load")
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()
        print("\nTest complete.")

def test_without_selenium():
    """Test the dashboard runs with new refresh settings"""
    print("Testing dashboard with new auto-refresh settings...")
    print("This will start the dashboard and you can manually verify:")
    print("1. Auto-refresh indicator shows '9min'")
    print("2. Console shows '9 minutes (2 data collector cycles)'")
    print("3. Navigate to different tabs and wait for refresh")
    print("\nStarting dashboard...")
    
    from aemo_dashboard.generation.gen_dash import main
    main()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Test auto-refresh state preservation')
    parser.add_argument('--selenium', action='store_true', help='Run automated Selenium tests')
    args = parser.parse_args()
    
    if args.selenium:
        test_state_preservation()
    else:
        test_without_selenium()