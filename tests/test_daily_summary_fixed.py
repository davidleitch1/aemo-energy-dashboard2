#!/usr/bin/env python3
"""Test the fixed daily summary component"""

import sys
sys.path.insert(0, 'src')

from datetime import datetime, timedelta
from aemo_dashboard.nem_dash.daily_summary import (
    calculate_daily_metrics,
    generate_comparison_insights,
    create_summary_table
)

# Test metrics calculation
print("Testing Daily Summary Fixes...")
print("=" * 60)

now = datetime.now()
today_metrics = calculate_daily_metrics(now - timedelta(hours=24), now)

print("\nToday's Metrics:")
print(f"NEM Average Price: ${today_metrics['nem_avg_price']}/MWh")
print(f"NEM High: ${today_metrics['nem_high_price']}/MWh")
print(f"NEM Low: ${today_metrics['nem_low_price']}/MWh")

# Create dummy yesterday metrics with some change
yesterday_metrics = {
    'prices': {},
    'generation': {},
    'nem_avg_price': today_metrics['nem_avg_price'] * 0.85,  # 15% lower yesterday
    'nem_high_price': today_metrics['nem_high_price'] * 0.9,
    'nem_low_price': today_metrics['nem_low_price'] * 1.1
}

# Create dummy last year metrics
last_year_metrics = {
    'prices': {},
    'generation': {},
    'nem_avg_price': today_metrics['nem_avg_price'] * 0.7,  # 30% lower last year
    'nem_high_price': today_metrics['nem_high_price'] * 0.8,
    'nem_low_price': today_metrics['nem_low_price'] * 1.2
}

# Generate insights
insights = generate_comparison_insights(today_metrics, yesterday_metrics, last_year_metrics)

print("\nGenerated Insights:")
for i, insight in enumerate(insights, 1):
    print(f"{i}. {insight}")

# Create table HTML
table_html = create_summary_table(today_metrics, insights)

# Check if table has content
print("\nTable Generation:")
print(f"Table created: {'Yes' if table_html else 'No'}")
print(f"Has NEM average: {'Yes' if str(today_metrics['nem_avg_price']) in table_html else 'No'}")
print(f"Has insights: {'Yes' if insights and insights[0] in table_html else 'No'}")

print("\n" + "=" * 60)
print("Test complete!")