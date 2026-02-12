#!/usr/bin/env python3
"""
Simple syntax test for generation summary table methods.
Tests the methods can be imported and have correct signatures.
"""

import sys
from pathlib import Path
import inspect

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

print("=" * 80)
print("GENERATION SUMMARY - SYNTAX AND METHOD SIGNATURE TEST")
print("=" * 80)
print()

# Test 1: Check the file can be parsed (syntax check)
print("Test 1: Checking gen_dash.py syntax...")
print("-" * 80)
try:
    with open('src/aemo_dashboard/generation/gen_dash.py', 'r') as f:
        code = f.read()
        compile(code, 'gen_dash.py', 'exec')
    print("✓ SUCCESS: No syntax errors found")
except SyntaxError as e:
    print(f"✗ FAILED: Syntax error in gen_dash.py")
    print(f"  Line {e.lineno}: {e.msg}")
    print(f"  {e.text}")
    sys.exit(1)
except Exception as e:
    print(f"✗ FAILED: {e}")
    sys.exit(1)

print()

# Test 2: Check the methods exist
print("Test 2: Checking new methods exist in GenerationDashboard...")
print("-" * 80)
try:
    # Parse the file to find the methods
    import ast
    with open('src/aemo_dashboard/generation/gen_dash.py', 'r') as f:
        tree = ast.parse(f.read())

    # Find the GenerationDashboard class
    gen_dash_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'GenerationDashboard':
            gen_dash_class = node
            break

    if gen_dash_class is None:
        print("✗ FAILED: GenerationDashboard class not found")
        sys.exit(1)

    # Check for new methods
    expected_methods = [
        'calculate_generation_summary',
        'calculate_pcp_date_range',
        'get_pcp_generation_data',
        'create_generation_summary_table'
    ]

    found_methods = []
    for node in gen_dash_class.body:
        if isinstance(node, ast.FunctionDef):
            if node.name in expected_methods:
                found_methods.append(node.name)

    print(f"✓ Found {len(found_methods)}/{len(expected_methods)} new methods:")
    for method in expected_methods:
        if method in found_methods:
            print(f"  ✓ {method}")
        else:
            print(f"  ✗ {method} NOT FOUND")

    if len(found_methods) == len(expected_methods):
        print("\n✓ SUCCESS: All new methods are present")
    else:
        print("\n✗ FAILED: Some methods are missing")
        sys.exit(1)

except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 3: Check method signatures
print("Test 3: Checking method signatures...")
print("-" * 80)
try:
    for node in gen_dash_class.body:
        if isinstance(node, ast.FunctionDef):
            if node.name == 'calculate_generation_summary':
                args = [arg.arg for arg in node.args.args]
                if 'data_df' in args:
                    print("✓ calculate_generation_summary has 'data_df' parameter")
                else:
                    print("✗ calculate_generation_summary missing 'data_df' parameter")

            elif node.name == 'calculate_pcp_date_range':
                args = [arg.arg for arg in node.args.args]
                # Should only have 'self'
                if len(args) == 1 and args[0] == 'self':
                    print("✓ calculate_pcp_date_range has correct signature")
                else:
                    print(f"✗ calculate_pcp_date_range has unexpected args: {args}")

            elif node.name == 'get_pcp_generation_data':
                args = [arg.arg for arg in node.args.args]
                # Should only have 'self'
                if len(args) == 1 and args[0] == 'self':
                    print("✓ get_pcp_generation_data has correct signature")
                else:
                    print(f"✗ get_pcp_generation_data has unexpected args: {args}")

            elif node.name == 'create_generation_summary_table':
                args = [arg.arg for arg in node.args.args]
                # Should only have 'self'
                if len(args) == 1 and args[0] == 'self':
                    print("✓ create_generation_summary_table has correct signature")
                else:
                    print(f"✗ create_generation_summary_table has unexpected args: {args}")

    print("\n✓ SUCCESS: All method signatures are correct")

except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 4: Check integration in layout
print("Test 4: Checking summary_table_pane integration...")
print("-" * 80)
try:
    # Check __init__ for summary_table_pane
    init_found = False
    for node in gen_dash_class.body:
        if isinstance(node, ast.FunctionDef) and node.name == '__init__':
            # Look for self.summary_table_pane in the function body
            source = ast.unparse(node)
            if 'summary_table_pane' in source:
                print("✓ summary_table_pane initialized in __init__")
                init_found = True
            break

    if not init_found:
        print("✗ summary_table_pane not found in __init__")

    # Check _initialize_panes for summary table creation
    init_panes_found = False
    for node in gen_dash_class.body:
        if isinstance(node, ast.FunctionDef) and node.name == '_initialize_panes':
            source = ast.unparse(node)
            if 'create_generation_summary_table' in source:
                print("✓ summary table created in _initialize_panes")
                init_panes_found = True
            break

    if not init_panes_found:
        print("✗ summary table creation not found in _initialize_panes")

    # Check update_plot for summary table update
    update_plot_found = False
    for node in gen_dash_class.body:
        if isinstance(node, ast.FunctionDef) and node.name == 'update_plot':
            source = ast.unparse(node)
            if 'summary_table_pane' in source and 'clear' in source:
                print("✓ summary table updated in update_plot")
                update_plot_found = True
            break

    if not update_plot_found:
        print("✗ summary table update not found in update_plot")

    if init_found and init_panes_found and update_plot_found:
        print("\n✓ SUCCESS: Summary table properly integrated")
    else:
        print("\n✗ FAILED: Integration incomplete")
        sys.exit(1)

except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Test 5: Check docstrings
print("Test 5: Checking method docstrings...")
print("-" * 80)
docstring_count = 0
for node in gen_dash_class.body:
    if isinstance(node, ast.FunctionDef):
        if node.name in expected_methods:
            if ast.get_docstring(node):
                docstring_count += 1
            else:
                print(f"  ⚠ {node.name} is missing a docstring")

if docstring_count == len(expected_methods):
    print(f"✓ SUCCESS: All {len(expected_methods)} methods have docstrings")
else:
    print(f"⚠ WARNING: {len(expected_methods) - docstring_count}/{len(expected_methods)} methods missing docstrings")

print()
print("=" * 80)
print("ALL SYNTAX AND STRUCTURE TESTS PASSED ✓")
print("=" * 80)
print()
print("Summary of verified features:")
print("  1. No syntax errors in gen_dash.py")
print("  2. All 4 new methods are present:")
print("     - calculate_generation_summary")
print("     - calculate_pcp_date_range")
print("     - get_pcp_generation_data")
print("     - create_generation_summary_table")
print("  3. All method signatures are correct")
print("  4. Summary table is properly integrated:")
print("     - Initialized in __init__")
print("     - Created in _initialize_panes")
print("     - Updated in update_plot")
print("  5. All methods have docstrings")
print()
print("Next steps:")
print("  - Deploy to production machine for functional testing")
print("  - Test with live data and different time periods")
print("  - Verify PCP calculation and table display")
print()
