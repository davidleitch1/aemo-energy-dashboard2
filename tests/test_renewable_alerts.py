#!/usr/bin/env python3
"""
Test script for renewable energy alerts
Tests the alert logic without actually sending SMS messages
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add the src directory to path
sys.path.insert(0, 'src')

from dotenv import load_dotenv
load_dotenv()

def test_alert_formatting():
    """Test the alert message formatting"""
    print("="*60)
    print("Testing Alert Message Formatting")
    print("="*60)
    
    # Test renewable percentage alert
    print("\n1. Renewable Percentage Alert:")
    new_time = datetime.now()
    old_time = datetime(2024, 11, 6, 13, 0)
    
    message = f"""🎉 NEW RENEWABLE RECORD!
All-time: 79.2% (was 78.7%)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: {old_time.strftime('%Y-%m-%d %H:%M')}"""
    
    print(message)
    print(f"Message length: {len(message)} characters")
    
    # Test wind alert
    print("\n2. Wind Record Alert:")
    message = f"""🌬️ NEW WIND RECORD!
9,825 MW (was 9,757 MW)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: 2025-07-10 23:10"""
    
    print(message)
    print(f"Message length: {len(message)} characters")
    
    # Test solar alert
    print("\n3. Solar Record Alert:")
    message = f"""☀️ NEW SOLAR RECORD!
7,501 MW (was 7,459 MW)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: 2025-02-27 14:30"""
    
    print(message)
    print(f"Message length: {len(message)} characters")

def test_record_detection():
    """Test the record detection logic"""
    print("\n" + "="*60)
    print("Testing Record Detection Logic")
    print("="*60)
    
    # Load current records
    data_dir = os.getenv('DATA_DIR', '/tmp')
    records_file = Path(data_dir) / 'renewable_records_calculated.json'
    
    if records_file.exists():
        with open(records_file, 'r') as f:
            records = json.load(f)
        
        print("\nCurrent Records:")
        print(f"Renewable %: {records['all_time']['renewable_pct']['value']:.1f}%")
        print(f"Wind: {records['all_time']['wind_mw']['value']:,.0f} MW")
        print(f"Solar: {records['all_time']['solar_mw']['value']:,.0f} MW")
        print(f"Rooftop: {records['all_time']['rooftop_mw']['value']:,.0f} MW")
        print(f"Hydro: {records['all_time']['water_mw']['value']:,.0f} MW")
    else:
        print("No records file found")
        records = {
            'all_time': {
                'renewable_pct': {'value': 78.7, 'timestamp': '2024-11-06T13:00:00'},
                'wind_mw': {'value': 9757, 'timestamp': '2025-07-10T23:10:00'},
                'solar_mw': {'value': 7459, 'timestamp': '2025-02-27T14:30:00'},
                'rooftop_mw': {'value': 19297, 'timestamp': '2024-12-27T12:30:00'},
                'water_mw': {'value': 6494, 'timestamp': '2023-06-20T18:00:00'}
            }
        }
    
    # Test with values that would break records
    print("\n\nTesting with record-breaking values:")
    test_values = [
        ('renewable_pct', 79.2, records['all_time']['renewable_pct']['value']),
        ('wind_mw', 9800, records['all_time']['wind_mw']['value']),
        ('solar_mw', 7500, records['all_time']['solar_mw']['value']),
        ('rooftop_mw', 19350, records['all_time']['rooftop_mw']['value']),
        ('water_mw', 6500, records['all_time']['water_mw']['value'])
    ]
    
    alerts_needed = []
    for metric, new_value, old_value in test_values:
        if new_value > old_value:
            print(f"✓ {metric}: {new_value:,.1f} > {old_value:,.1f} - ALERT NEEDED")
            alerts_needed.append(metric)
        else:
            print(f"  {metric}: {new_value:,.1f} <= {old_value:,.1f} - No alert")
    
    print(f"\nTotal alerts needed: {len(alerts_needed)}")
    
    # Test with values that wouldn't break records
    print("\n\nTesting with non-record values:")
    test_values = [
        ('renewable_pct', 75.0, records['all_time']['renewable_pct']['value']),
        ('wind_mw', 9000, records['all_time']['wind_mw']['value']),
        ('solar_mw', 7000, records['all_time']['solar_mw']['value'])
    ]
    
    alerts_needed = []
    for metric, new_value, old_value in test_values:
        if new_value > old_value:
            print(f"✓ {metric}: {new_value:,.1f} > {old_value:,.1f} - ALERT NEEDED")
            alerts_needed.append(metric)
        else:
            print(f"  {metric}: {new_value:,.1f} <= {old_value:,.1f} - No alert")
    
    print(f"\nTotal alerts needed: {len(alerts_needed)}")

def test_twilio_config():
    """Test Twilio configuration"""
    print("\n" + "="*60)
    print("Testing Twilio Configuration")
    print("="*60)
    
    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    from_number = os.getenv('TWILIO_FROM_NUMBER')
    to_number = os.getenv('ALERT_PHONE_NUMBER')
    
    print(f"TWILIO_ACCOUNT_SID: {'✓ Set' if account_sid else '✗ Not set'}")
    print(f"TWILIO_AUTH_TOKEN: {'✓ Set' if auth_token else '✗ Not set'}")
    print(f"TWILIO_FROM_NUMBER: {from_number if from_number else '✗ Not set'}")
    print(f"ALERT_PHONE_NUMBER: {to_number if to_number else '✗ Not set'}")
    
    if all([account_sid, auth_token, from_number, to_number]):
        print("\n✓ All Twilio configuration present")
        
        # Try to initialize Twilio client
        try:
            from twilio.rest import Client
            client = Client(account_sid, auth_token)
            print("✓ Twilio client initialized successfully")
        except Exception as e:
            print(f"✗ Error initializing Twilio: {e}")
    else:
        print("\n✗ Missing Twilio configuration")

def test_time_comparisons():
    """Test time comparison formatting"""
    print("\n" + "="*60)
    print("Testing Time Comparisons")
    print("="*60)
    
    current = datetime.now()
    
    # Various time differences
    times = [
        ("Just now", current - timedelta(minutes=5)),
        ("1 hour ago", current - timedelta(hours=1)),
        ("Yesterday", current - timedelta(days=1)),
        ("Last week", current - timedelta(days=7)),
        ("Last month", current - timedelta(days=30)),
        ("Last year", current - timedelta(days=365))
    ]
    
    for label, old_time in times:
        time_diff = current - old_time
        print(f"\n{label}:")
        print(f"  Current: {current.strftime('%Y-%m-%d %H:%M')}")
        print(f"  Previous: {old_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"  Difference: {time_diff}")

if __name__ == "__main__":
    print("Renewable Energy Alert System Test")
    print("="*60)
    
    # Run all tests
    test_alert_formatting()
    test_record_detection()
    test_twilio_config()
    test_time_comparisons()
    
    print("\n" + "="*60)
    print("Test complete!")
    print("\nTo test the full system with mock alerts:")
    print("python standalone_renewable_gauge_with_alerts.py --test-alerts")
    print("\nTo test without sending real SMS:")
    print("python standalone_renewable_gauge_with_alerts.py --test")