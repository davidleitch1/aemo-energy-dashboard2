#!/usr/bin/env python3
"""
Test how Panel Tabs update mechanism works
"""

import panel as pn
import time

pn.extension()

# Test 1: Initial setup
print("Test 1: Creating tabs with initial content")
tabs = pn.Tabs(
    ("Tab 1", pn.pane.Markdown("Initial content 1")),
    ("Tab 2", pn.pane.Markdown("Initial content 2")),
    ("Tab 3", pn.pane.Markdown("Initial content 3")),
)

print(f"Initial setup:")
print(f"  tabs._names: {tabs._names}")
print(f"  tabs.objects: {tabs.objects}")
print(f"  Type of tabs.objects[0]: {type(tabs.objects[0])}")

# Test 2: Update using objects attribute
print("\nTest 2: Updating tab content using tabs.objects[index]")
new_content = pn.pane.Markdown("**Updated content for Tab 2**")
tabs.objects[1] = new_content

print(f"After update:")
print(f"  tabs._names: {tabs._names}")
print(f"  tabs.objects[1]: {tabs.objects[1]}")

# Test 3: Try to trigger updates
print("\nTest 3: Checking if we need to trigger updates")
print(f"  tabs.param.objects: {tabs.param.objects}")

# Test 4: Alternative update method
print("\nTest 4: Testing alternative update methods")

# Method A: Direct assignment with tuple (the old way that lost names)
# This is what we DON'T want to do
# tabs[1] = ("Tab 2", pn.pane.Markdown("Content via tuple"))

# Method B: Using remove and insert
print("Method B: Using remove() and insert()")
tabs.remove(tabs.objects[2])
tabs.insert(2, ("Tab 3", pn.pane.Markdown("Content via insert")))
print(f"  After remove/insert - tabs._names: {tabs._names}")

# Test 5: Check if objects list is mutable
print("\nTest 5: Direct list operations")
tabs.objects[0] = pn.pane.Markdown("Direct list update")
print(f"  Direct update worked: {tabs.objects[0]}")

# Summary
print("\n" + "="*60)
print("FINDINGS:")
print("1. tabs.objects[index] = content SHOULD work for updating content")
print("2. Tab names are preserved in tabs._names")
print("3. The objects list is directly mutable")
print("4. Alternative: use remove() + insert() for complete control")
print("="*60)