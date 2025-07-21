#!/usr/bin/env python3
"""
Debug Script for Station Analysis Data Flow Issues

This script systematically tests each step of the station analysis data loading
to identify where the data flow breaks that's causing no data to display.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from datetime import datetime, timedelta

# Import individual modules to avoid dependency issues
try:
    from src.aemo_dashboard.shared.generation_adapter import load_generation_data
    from src.aemo_dashboard.shared.price_adapter import load_price_data
    from src.aemo_dashboard.shared.resolution_manager import resolution_manager
    from src.aemo_dashboard.station.station_analysis import StationAnalysisMotor
    IMPORTS_OK = True
except Exception as e:
    print(f"⚠️  Import error: {e}")
    print("Proceeding with basic file tests only...")
    IMPORTS_OK = False

def test_basic_data_loading():
    """Test basic data loading functions"""
    print("=== STEP 1: Testing Basic Data Loading ===")
    
    if not IMPORTS_OK:
        print("❌ Skipping due to import issues")
        return
    
    # Test 1-day range (should show no data issue)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print(f"Testing date range: {start_date.date()} to {end_date.date()}")
    
    # Test generation data loading
    print("\n--- Testing Generation Data ---")
    try:
        gen_data = load_generation_data(
            start_date=start_date,
            end_date=end_date,
            resolution='auto'
        )
        print(f"✅ Generation data loaded: {len(gen_data):,} records")
        if not gen_data.empty:
            print(f"   Columns: {list(gen_data.columns)}")
            print(f"   Date range: {gen_data['settlementdate'].min()} to {gen_data['settlementdate'].max()}")
            print(f"   Unique DUIDs: {gen_data['duid'].nunique()}")
            print(f"   Sample DUIDs: {gen_data['duid'].unique()[:5].tolist()}")
        else:
            print("❌ No generation data returned")
    except Exception as e:
        print(f"❌ Generation data loading failed: {e}")
    
    # Test price data loading
    print("\n--- Testing Price Data ---")
    try:
        price_data = load_price_data(
            start_date=start_date,
            end_date=end_date,
            resolution='auto'
        )
        print(f"✅ Price data loaded: {len(price_data):,} records")
        if not price_data.empty:
            print(f"   Columns: {list(price_data.columns)}")
            if 'SETTLEMENTDATE' in price_data.columns:
                print(f"   Date range: {price_data['SETTLEMENTDATE'].min()} to {price_data['SETTLEMENTDATE'].max()}")
            elif 'settlementdate' in price_data.columns:
                print(f"   Date range: {price_data['settlementdate'].min()} to {price_data['settlementdate'].max()}")
        else:
            print("❌ No price data returned")
    except Exception as e:
        print(f"❌ Price data loading failed: {e}")

def test_resolution_manager():
    """Test resolution manager file path resolution"""
    print("\n=== STEP 2: Testing Resolution Manager ===")
    
    if not IMPORTS_OK:
        print("❌ Skipping due to import issues")
        return
    
    try:
        # Test generation file paths
        gen_5min_path = resolution_manager.get_file_path('generation', '5min')
        gen_30min_path = resolution_manager.get_file_path('generation', '30min')
        
        print(f"Generation 5min path: {gen_5min_path}")
        print(f"   Exists: {os.path.exists(gen_5min_path)}")
        
        print(f"Generation 30min path: {gen_30min_path}")
        print(f"   Exists: {os.path.exists(gen_30min_path)}")
        
        # Test price file paths
        price_5min_path = resolution_manager.get_file_path('price', '5min')
        price_30min_path = resolution_manager.get_file_path('price', '30min')
        
        print(f"Price 5min path: {price_5min_path}")
        print(f"   Exists: {os.path.exists(price_5min_path)}")
        
        print(f"Price 30min path: {price_30min_path}")
        print(f"   Exists: {os.path.exists(price_30min_path)}")
        
        # Test resolution selection
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        gen_resolution = resolution_manager.get_optimal_resolution(
            start_date, end_date, 'generation'
        )
        price_resolution = resolution_manager.get_optimal_resolution(
            start_date, end_date, 'price'
        )
        
        print(f"\nOptimal resolution for 1-day range:")
        print(f"   Generation: {gen_resolution}")
        print(f"   Price: {price_resolution}")
        
    except Exception as e:
        print(f"❌ Resolution manager test failed: {e}")

def test_station_analysis_motor():
    """Test station analysis motor step by step"""
    print("\n=== STEP 3: Testing Station Analysis Motor ===")
    
    if not IMPORTS_OK:
        print("❌ Skipping due to import issues")
        return
    
    try:
        # Initialize motor
        motor = StationAnalysisMotor()
        
        # Test DUID mapping loading
        print("--- Loading DUID mapping ---")
        if motor.load_data():
            print(f"✅ DUID mapping loaded: {len(motor.duid_mapping):,} entries")
            # Show sample DUIDs
            sample_duids = list(motor.duid_mapping.keys())[:5]
            print(f"   Sample DUIDs: {sample_duids}")
        else:
            print("❌ DUID mapping loading failed")
            return
        
        # Test data loading for specific date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        print(f"\n--- Loading data for {start_date.date()} to {end_date.date()} ---")
        if motor.load_data_for_date_range(start_date, end_date):
            print(f"✅ Data loading successful")
            print(f"   Generation data: {len(motor.gen_data) if motor.gen_data is not None else 0:,} records")
            print(f"   Price data: {len(motor.price_data) if motor.price_data is not None else 0:,} records")
        else:
            print("❌ Data loading failed")
            return
        
        # Test data integration
        print("\n--- Testing data integration ---")
        if motor.integrate_data():
            print(f"✅ Data integration successful")
            print(f"   Integrated data: {len(motor.integrated_data):,} records")
            if not motor.integrated_data.empty:
                print(f"   Columns: {list(motor.integrated_data.columns)}")
                print(f"   Date range: {motor.integrated_data['settlementdate'].min()} to {motor.integrated_data['settlementdate'].max()}")
                print(f"   Unique DUIDs: {motor.integrated_data['duid'].nunique()}")
        else:
            print("❌ Data integration failed")
            return
        
        # Test station filtering with a specific DUID
        print("\n--- Testing station filtering ---")
        if motor.integrated_data is not None and not motor.integrated_data.empty:
            # Get a DUID that has recent data
            sample_duid = motor.integrated_data['duid'].iloc[0]
            print(f"Testing with DUID: {sample_duid}")
            
            if motor.filter_station_data(sample_duid, start_date, end_date):
                print(f"✅ Station filtering successful")
                print(f"   Station data: {len(motor.station_data):,} records")
                if not motor.station_data.empty:
                    print(f"   Date range: {motor.station_data['settlementdate'].min()} to {motor.station_data['settlementdate'].max()}")
                    print(f"   Generation range: {motor.station_data['scadavalue'].min():.1f} to {motor.station_data['scadavalue'].max():.1f} MW")
            else:
                print("❌ Station filtering failed")
        
    except Exception as e:
        print(f"❌ Station analysis motor test failed: {e}")
        import traceback
        traceback.print_exc()

def test_file_access():
    """Test direct file access"""
    print("\n=== STEP 4: Testing Direct File Access ===")
    
    files_to_check = [
        '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet',
        '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada30.parquet',
        '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices5.parquet',
        '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices30.parquet'
    ]
    
    for file_path in files_to_check:
        try:
            print(f"\n--- Testing {os.path.basename(file_path)} ---")
            print(f"Path: {file_path}")
            print(f"Exists: {os.path.exists(file_path)}")
            
            if os.path.exists(file_path):
                df = pd.read_parquet(file_path)
                print(f"Records: {len(df):,}")
                print(f"Columns: {list(df.columns)}")
                
                # Find date column
                date_col = None
                for col in ['settlementdate', 'SETTLEMENTDATE']:
                    if col in df.columns:
                        date_col = col
                        break
                
                if date_col:
                    print(f"Date range: {df[date_col].min()} to {df[date_col].max()}")
                    
                    # Check recent data
                    recent_cutoff = datetime.now() - timedelta(days=1)
                    recent_data = df[df[date_col] >= recent_cutoff]
                    print(f"Recent data (last 24h): {len(recent_data):,} records")
                
        except Exception as e:
            print(f"❌ Error accessing {file_path}: {e}")

def main():
    """Run all debug tests"""
    print("🔍 Station Analysis Data Flow Debug")
    print("=" * 50)
    
    test_basic_data_loading()
    test_resolution_manager()
    test_station_analysis_motor()
    test_file_access()
    
    print("\n" + "=" * 50)
    print("🔍 Debug Complete - Check output above for issues")

if __name__ == "__main__":
    main()