#!/usr/bin/env python3
"""Test complete daily summary with generation data"""

import sys
sys.path.insert(0, 'src')

from datetime import datetime, timedelta
from aemo_dashboard.nem_dash.daily_summary import calculate_daily_metrics, create_daily_summary_component

print("Testing Complete Daily Summary...")
print("=" * 60)

# Calculate metrics
now = datetime.now()
metrics = calculate_daily_metrics(now - timedelta(hours=24), now)

print("\nPrice Data:")
for region, prices in metrics['prices'].items():
    print(f"  {region}: Avg ${prices['avg']}, High ${prices['high']}, Low ${prices['low']}")

print(f"\nNEM Average: ${metrics['nem_avg_price']}")

print("\nGeneration Data:")
if metrics['generation']:
    for region, gen in metrics['generation'].items():
        print(f"  {region}: {gen['total_gwh']} GWh, Renewable {gen['renewable_pct']}%, "
              f"Gas {gen['gas_pct']}%, Coal {gen['coal_pct']}%")
else:
    print("  No generation data loaded")

# Create the component
print("\nCreating daily summary component...")
component = create_daily_summary_component()

print("\nTest complete!")