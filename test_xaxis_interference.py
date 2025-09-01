#!/usr/bin/env python3
"""
Test to verify the x-axis interference issue between Prices and Batteries tabs.

The issue: When using the Prices tab, the link_x_ranges_hook function stores
x_range objects as function attributes that persist across tab switches.
This causes the Batteries tab plots to inherit the x-axis range from the Prices tab.
"""

def test_function_attribute_persistence():
    """Demonstrate how function attributes persist across calls"""
    
    def link_hook():
        """Simulates the problematic hook function"""
        if not hasattr(link_hook, 'x_ranges'):
            link_hook.x_ranges = []
        link_hook.x_ranges.append(len(link_hook.x_ranges))
        return link_hook.x_ranges
    
    # First call (simulating Prices tab)
    print("First call (Prices tab):", link_hook())
    print("Second call (Prices tab):", link_hook())
    
    # Third call (simulating switching to Batteries tab)
    print("Third call (Batteries tab - should be empty but isn't):", link_hook())
    
    # Show that the attribute persists
    print("\nFunction attribute state:", link_hook.x_ranges)
    print("This persistence causes x_ranges from Prices tab to affect Batteries tab!")


def test_proper_scoping():
    """Demonstrate the fix using proper scoping"""
    
    class TabContext:
        """Each tab gets its own context to avoid interference"""
        def __init__(self):
            self.x_ranges = []
        
        def link_hook(self, plot):
            """Hook that uses instance state instead of function attributes"""
            self.x_ranges.append(plot)
            return self.x_ranges
    
    # Create separate contexts for each tab
    prices_context = TabContext()
    batteries_context = TabContext()
    
    # Prices tab operations
    print("\nPrices tab context:")
    print("First call:", prices_context.link_hook("price_plot1"))
    print("Second call:", prices_context.link_hook("price_plot2"))
    
    # Batteries tab operations (completely independent)
    print("\nBatteries tab context (independent):")
    print("First call:", batteries_context.link_hook("battery_plot1"))
    print("Second call:", batteries_context.link_hook("battery_plot2"))
    
    print("\nNo interference between tabs!")


if __name__ == "__main__":
    print("="*60)
    print("Demonstrating x-axis interference issue")
    print("="*60)
    
    test_function_attribute_persistence()
    
    print("\n" + "="*60)
    print("Demonstrating the fix")
    print("="*60)
    
    test_proper_scoping()