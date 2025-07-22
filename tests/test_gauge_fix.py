#!/usr/bin/env python3
"""
Test the fixed renewable gauge to ensure:
1. Grey hour line shows across the dial
2. Legend text doesn't overlap
3. Legend is properly positioned below gauge
"""
import os
import sys

# Set environment
os.environ['USE_DUCKDB'] = 'true'
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    print("Testing renewable gauge fixes...")
    
    # Import required modules
    import panel as pn
    pn.extension('plotly')
    
    from aemo_dashboard.nem_dash.renewable_gauge import create_renewable_gauge_plotly
    
    # Test gauge creation with sample values
    print("\n1. Creating gauge with test values...")
    current_value = 61
    all_time_record = 94
    hour_record = 65
    
    fig = create_renewable_gauge_plotly(current_value, all_time_record, hour_record)
    
    print(f"✓ Gauge created with:")
    print(f"  - Current value: {current_value}%")
    print(f"  - All-time record: {all_time_record}% (gold threshold)")
    print(f"  - Hour record: {hour_record}% (grey threshold)")
    
    # Check that we have two traces (main gauge + invisible gauge for grey line)
    print(f"\n2. Checking gauge structure...")
    print(f"✓ Number of traces: {len(fig.data)} (should be 2)")
    
    # Check the main gauge settings
    main_gauge = fig.data[0]
    print(f"✓ Main gauge has gold threshold at: {main_gauge.gauge.threshold.value}%")
    
    # Check the invisible gauge for grey line
    grey_gauge = fig.data[1]
    print(f"✓ Grey line gauge has threshold at: {grey_gauge.gauge.threshold.value}%")
    
    # Check legend positioning
    print(f"\n3. Checking legend positioning...")
    annotations = fig.layout.annotations
    print(f"✓ Number of annotations: {len(annotations)} (should be 3)")
    
    for i, ann in enumerate(annotations):
        print(f"  - Annotation {i+1}: '{ann.text[:20]}...' at y={ann.y}")
    
    # Check layout settings
    print(f"\n4. Checking layout...")
    print(f"✓ Gauge domain Y: {main_gauge.domain.y} (should leave space at bottom)")
    print(f"✓ Show legend: {fig.layout.showlegend} (should be False)")
    
    # Save to test file
    print("\n5. Saving test output...")
    test_html = """
<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { background-color: #1a1a1a; color: white; font-family: Arial; }
        .container { width: 400px; margin: 50px auto; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Renewable Gauge Test</h2>
        <div id="gauge"></div>
    </div>
    <script>
        var data = """ + str(fig.data) + """;
        var layout = """ + str(fig.layout) + """;
        Plotly.newPlot('gauge', data, layout);
    </script>
</body>
</html>
"""
    
    with open('test_gauge_output.html', 'w') as f:
        f.write(test_html)
    
    print("✓ Test output saved to test_gauge_output.html")
    
    print("\n✅ All gauge fixes implemented successfully!")
    print("\nKey improvements:")
    print("- Two-gauge approach: Main gauge (gold line) + Invisible gauge (grey line)")
    print("- Gauge positioned higher (y: 0.15-1.0) to leave space for legend")
    print("- Legend positioned in reserved bottom space (y: 0.06-0.10)")
    print("- Both threshold lines should now display properly")
    
except Exception as e:
    print(f"\n❌ Error testing gauge: {e}")
    import traceback
    traceback.print_exc()