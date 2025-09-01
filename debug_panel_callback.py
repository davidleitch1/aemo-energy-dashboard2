#!/usr/bin/env python3
"""
Debug Panel callback issues
"""

import panel as pn
import time

pn.extension()

def test_callbacks():
    """Test different callback mechanisms"""
    print(f"Panel version: {pn.__version__}")
    print(f"Panel state: {pn.state}")
    
    # Test 1: Simple callback
    container = pn.Column("Initial content")
    
    def update_content():
        print("Callback fired!")
        container[0] = "Updated content"
    
    # Test different callback methods
    print("\nTesting add_periodic_callback...")
    try:
        pn.state.add_periodic_callback(update_content, period=100, count=1)
        print("add_periodic_callback registered successfully")
    except Exception as e:
        print(f"add_periodic_callback failed: {e}")
    
    # Test onload
    print("\nTesting onload...")
    try:
        if hasattr(pn.state, 'onload'):
            pn.state.onload(lambda: print("onload fired!"))
            print("onload registered successfully")
        else:
            print("onload not available in this context")
    except Exception as e:
        print(f"onload failed: {e}")
    
    return container

# Test in different contexts
print("Testing Panel callbacks...")

# Test 1: Direct execution
print("\n1. Direct execution test:")
app = test_callbacks()

# Test 2: Server context
print("\n2. Server context test:")
def create_test_app():
    return test_callbacks()

if __name__ == "__main__":
    print("\nStarting test server on port 5009...")
    pn.serve(create_test_app, port=5009, show=False)