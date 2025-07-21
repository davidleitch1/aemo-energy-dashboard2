#!/usr/bin/env python3
"""
Test script to capture penetration tab screenshot.
"""
import time
from playwright.sync_api import sync_playwright

def capture_penetration_tab():
    """Capture screenshot of the penetration tab."""
    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(headless=False)  # Set to False to see what's happening
        page = browser.new_page()
        
        # Navigate to dashboard
        print("Navigating to dashboard...")
        page.goto("http://localhost:5009")
        
        # Wait for dashboard to load
        print("Waiting for dashboard to load...")
        page.wait_for_load_state("networkidle")
        time.sleep(5)  # Extra wait for components to render
        
        # Click on Penetration tab
        print("Clicking on Penetration tab...")
        try:
            # Look for the Penetration tab
            page.click('text="Penetration"')
            time.sleep(3)  # Wait for tab content to load
            
            # Take screenshot
            print("Taking screenshot...")
            page.screenshot(path="penetration_tab_screenshot.png", full_page=True)
            print("Screenshot saved as penetration_tab_screenshot.png")
            
        except Exception as e:
            print(f"Error: {e}")
            # Take screenshot anyway to see what's on screen
            page.screenshot(path="error_screenshot.png", full_page=True)
            print("Error screenshot saved as error_screenshot.png")
        
        # Keep browser open for a moment to see the result
        time.sleep(2)
        browser.close()

if __name__ == "__main__":
    capture_penetration_tab()