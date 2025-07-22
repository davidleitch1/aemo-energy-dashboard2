#!/usr/bin/env python3
"""
Test the tab loading fix
"""

import panel as pn
import time

pn.extension()

# Simulate the dashboard tab structure
class TabTest:
    def __init__(self):
        self._loaded_tabs = {0}
        self._tab_creators = {
            1: lambda: pn.pane.Markdown("# Tab 1 Content"),
            2: lambda: pn.pane.Markdown("# Tab 2 Content"),
            3: lambda: pn.pane.Markdown("# Tab 3 Content"),
        }
        
        # Create tabs
        self.tabs = pn.Tabs(
            ("Tab 0", pn.pane.Markdown("# Tab 0 (Already loaded)")),
            ("Tab 1", pn.pane.HTML("<h3>Click to load</h3>")),
            ("Tab 2", pn.pane.HTML("<h3>Click to load</h3>")),
            ("Tab 3", pn.pane.HTML("<h3>Click to load</h3>")),
            dynamic=True
        )
        
        # Watch for changes
        self.tabs.param.watch(self._on_tab_change, 'active')
        
        # Add periodic check
        def check_tab_loading():
            current_tab = self.tabs.active
            if current_tab not in self._loaded_tabs and current_tab in self._tab_creators:
                print(f"Periodic check: Tab {current_tab} needs loading")
                self._on_tab_change(type('Event', (), {'new': current_tab, 'old': -1})())
        
        # Simulate the periodic callback
        self.check_tab_loading = check_tab_loading
        
    def _on_tab_change(self, event):
        tab_index = event.new
        print(f"Tab change event: {event.old} -> {event.new}")
        
        if tab_index in self._loaded_tabs:
            print(f"Tab {tab_index} already loaded")
            return
            
        print(f"Loading tab {tab_index}...")
        content = self._tab_creators[tab_index]()
        self.tabs.objects[tab_index] = content
        self._loaded_tabs.add(tab_index)
        print(f"Tab {tab_index} loaded!")

# Test the implementation
print("Testing tab loading mechanism...")
test = TabTest()

# Simulate clicking tabs
print("\nSimulating tab clicks:")
for i in range(1, 4):
    print(f"\nClicking tab {i}...")
    test.tabs.active = i
    time.sleep(0.1)
    
    # Check if loaded
    if i not in test._loaded_tabs:
        print(f"Tab {i} didn't load on first click - triggering periodic check")
        test.check_tab_loading()

print("\nAll tabs should now be loaded!")
print(f"Loaded tabs: {test._loaded_tabs}")