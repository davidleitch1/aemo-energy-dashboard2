#!/usr/bin/env python3
"""
Test script for AEMO Dashboard FastAPI service
"""

import requests
import json
from datetime import datetime, timedelta

# API base URL
BASE_URL = "http://localhost:8000"

def test_health():
    """Test health endpoint"""
    print("1. Testing health endpoint...")
    response = requests.get(f"{BASE_URL}/api/health")
    if response.status_code == 200:
        data = response.json()
        print(f"   ✓ Service is healthy")
        print(f"   ✓ Memory usage: {data['memory_usage_mb']:.1f} MB")
        print(f"   ✓ Data loaded: {data['data_loaded']}")
    else:
        print(f"   ✗ Error: {response.status_code}")

def test_metadata():
    """Test metadata endpoint"""
    print("\n2. Testing metadata endpoint...")
    response = requests.get(f"{BASE_URL}/api/metadata")
    if response.status_code == 200:
        data = response.json()
        print(f"   ✓ Available regions: {len(data['regions'])}")
        print(f"   ✓ Available fuel types: {len(data['fuel_types'])}")
        print(f"   ✓ Date ranges:")
        for dtype, info in data['date_ranges'].items():
            print(f"     - {dtype}: {info['records']:,} records")
    else:
        print(f"   ✗ Error: {response.status_code}")

def test_generation():
    """Test generation endpoint"""
    print("\n3. Testing generation by fuel endpoint...")
    
    # Test with 1 day of data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "regions": ["NSW1", "QLD1"],
        "resolution": "hourly"
    }
    
    response = requests.get(f"{BASE_URL}/api/generation/by-fuel", params=params)
    if response.status_code == 200:
        data = response.json()
        print(f"   ✓ Received {data['count']} records")
        print(f"   ✓ Resolution: {data['resolution']}")
        if data['count'] > 0:
            print(f"   ✓ Sample record: {json.dumps(data['data'][0], indent=2)}")
    else:
        print(f"   ✗ Error: {response.status_code}")
        print(f"   ✗ Message: {response.text}")

def test_prices():
    """Test prices endpoint"""
    print("\n4. Testing regional prices endpoint...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=6)
    
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "regions": ["NSW1", "VIC1"]
    }
    
    response = requests.get(f"{BASE_URL}/api/prices/regional", params=params)
    if response.status_code == 200:
        data = response.json()
        print(f"   ✓ Received {data['count']} price records")
        print(f"   ✓ Regions: {data['regions']}")
    else:
        print(f"   ✗ Error: {response.status_code}")

def test_api_docs():
    """Test API documentation"""
    print("\n5. Testing API documentation...")
    response = requests.get(f"{BASE_URL}/docs")
    if response.status_code == 200:
        print(f"   ✓ API docs available at: {BASE_URL}/docs")
    else:
        print(f"   ✗ API docs not accessible")

if __name__ == "__main__":
    print("=== AEMO Dashboard API Test ===\n")
    
    try:
        # Test connection
        response = requests.get(BASE_URL, timeout=2)
        print(f"✓ Connected to server at {BASE_URL}\n")
        
        # Run tests
        test_health()
        test_metadata()
        test_generation()
        test_prices()
        test_api_docs()
        
        print("\n=== All tests completed ===")
        
    except requests.ConnectionError:
        print(f"✗ Could not connect to server at {BASE_URL}")
        print("  Make sure the FastAPI server is running:")
        print("  ./run_fastapi.sh")
    except Exception as e:
        print(f"✗ Error: {e}")