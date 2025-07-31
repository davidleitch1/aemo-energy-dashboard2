#!/usr/bin/env python3
"""Test the daily summary component"""

import sys
sys.path.insert(0, 'src')

import panel as pn
from datetime import datetime, timedelta
from aemo_dashboard.nem_dash.daily_summary import (
    calculate_daily_metrics,
    generate_comparison_insights,
    create_summary_table,
    create_daily_summary_component
)
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

def test_daily_summary():
    """Test the daily summary functionality"""
    print("Testing Daily Summary Component...")
    print("=" * 60)
    
    # Test 1: Calculate metrics for today
    print("\n1. Testing metrics calculation for last 24 hours...")
    now = datetime.now()
    today_metrics = calculate_daily_metrics(
        now - timedelta(hours=24),
        now
    )
    
    print("\nToday's Metrics:")
    print(f"NEM Average Price: ${today_metrics['nem_avg_price']}/MWh")
    print(f"NEM High Price: ${today_metrics['nem_high_price']}/MWh")
    print(f"NEM Low Price: ${today_metrics['nem_low_price']}/MWh")
    
    if today_metrics['prices']:
        print("\nRegional Prices:")
        for region, prices in today_metrics['prices'].items():
            print(f"  {region}: Avg ${prices['avg']}, High ${prices['high']}, Low ${prices['low']}")
    
    if today_metrics['generation']:
        print("\nGeneration Summary:")
        for region, gen in today_metrics['generation'].items():
            print(f"  {region}: {gen['total_gwh']} GWh, "
                  f"Renewable {gen['renewable_pct']}%, "
                  f"Gas {gen['gas_pct']}%, "
                  f"Coal {gen['coal_pct']}%")
    
    # Test 2: Calculate yesterday's metrics
    print("\n2. Testing metrics calculation for yesterday...")
    yesterday_metrics = calculate_daily_metrics(
        now - timedelta(hours=48),
        now - timedelta(hours=24)
    )
    
    if yesterday_metrics['nem_avg_price'] > 0:
        price_change = ((today_metrics['nem_avg_price'] - yesterday_metrics['nem_avg_price']) 
                       / yesterday_metrics['nem_avg_price'] * 100)
        print(f"Price change from yesterday: {price_change:+.1f}%")
    
    # Test 3: Generate insights
    print("\n3. Testing insight generation...")
    
    # Create dummy last year metrics for testing
    last_year_metrics = {
        'prices': {},
        'generation': {},
        'nem_avg_price': 75,
        'nem_high_price': 200,
        'nem_low_price': 30
    }
    
    # Copy today's structure with slightly different values
    for region in today_metrics['generation']:
        last_year_metrics['generation'][region] = {
            'total_gwh': today_metrics['generation'][region]['total_gwh'] * 0.95,
            'renewable_pct': today_metrics['generation'][region]['renewable_pct'] * 0.8,
            'gas_pct': today_metrics['generation'][region]['gas_pct'] * 1.1,
            'coal_pct': today_metrics['generation'][region]['coal_pct'] * 1.2
        }
    
    insights = generate_comparison_insights(
        today_metrics,
        yesterday_metrics,
        last_year_metrics
    )
    
    print("\nGenerated Insights:")
    for i, insight in enumerate(insights, 1):
        print(f"{i}. {insight}")
    
    # Test 4: Create HTML table
    print("\n4. Testing HTML table generation...")
    table_html = create_summary_table(today_metrics, insights)
    print("HTML table generated successfully")
    print(f"Table length: {len(table_html)} characters")
    
    # Test 5: Create full component
    print("\n5. Testing full component creation...")
    component = create_daily_summary_component()
    print(f"Component type: {type(component)}")
    print(f"Component width: {component.width}")
    print(f"Component height: {component.height}")
    
    return component

if __name__ == "__main__":
    pn.extension()
    
    component = test_daily_summary()
    
    print("\n" + "=" * 60)
    print("Test complete!")
    print("\nTo see the component in a browser, uncomment the lines below:")
    
    # Uncomment to view in browser
    # app = pn.template.MaterialTemplate(
    #     title="Daily Summary Test",
    #     main=[component]
    # )
    # app.show()
    
    print("\nTo integrate with dashboard, run:")
    print(".venv/bin/python src/aemo_dashboard/generation/gen_dash.py")