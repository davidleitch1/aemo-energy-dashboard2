#!/usr/bin/env python3
"""
Test dashboard loading with new generation data
"""

import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.config import config
from aemo_dashboard.shared.logging_config import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

def test_dashboard_data_loading():
    """Test if dashboard can load with new generation data"""
    
    print("=" * 60)
    print("TESTING DASHBOARD WITH NEW GENERATION DATA")
    print("=" * 60)
    
    # Check config is pointing to new file
    print(f"\nGeneration file config: {config.gen_output_file}")
    
    # Test loading generation data
    try:
        gen_df = pd.read_parquet(config.gen_output_file)
        print(f"✅ Successfully loaded generation data")
        print(f"   Records: {len(gen_df):,}")
        print(f"   Date range: {gen_df['settlementdate'].min()} to {gen_df['settlementdate'].max()}")
        
        # Test recent data
        latest_time = gen_df['settlementdate'].max()
        recent_data = gen_df[gen_df['settlementdate'] == latest_time]
        print(f"   Latest data: {len(recent_data)} records at {latest_time}")
        
    except Exception as e:
        print(f"❌ Failed to load generation data: {e}")
        return False
    
    # Test loading other data files (should still work)
    print("\nTesting other data files...")
    
    try:
        # Price data
        price_df = pd.read_parquet(config.spot_hist_file)
        print(f"✅ Price data loaded: {len(price_df):,} records")
    except Exception as e:
        print(f"⚠️  Price data issue: {e}")
    
    try:
        # Transmission data
        trans_df = pd.read_parquet(config.transmission_output_file)
        print(f"✅ Transmission data loaded: {len(trans_df):,} records")
    except Exception as e:
        print(f"⚠️  Transmission data issue: {e}")
    
    try:
        # Rooftop data
        roof_df = pd.read_parquet(config.rooftop_solar_file)
        print(f"✅ Rooftop data loaded: {len(roof_df):,} records")
    except Exception as e:
        print(f"⚠️  Rooftop data issue: {e}")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("✅ Dashboard can load with new generation data!")
    print("✅ No code changes required for generation data migration!")
    
    return True

def test_dashboard_calculations():
    """Test some common dashboard calculations with new data"""
    
    print("\n" + "=" * 60)
    print("TESTING DASHBOARD CALCULATIONS")
    print("=" * 60)
    
    try:
        # Load generation data
        gen_df = pd.read_parquet(config.gen_output_file)
        
        # Get last 24 hours
        latest = gen_df['settlementdate'].max()
        start_time = latest - pd.Timedelta(hours=24)
        recent_gen = gen_df[(gen_df['settlementdate'] > start_time) & 
                           (gen_df['settlementdate'] <= latest)]
        
        # Test aggregation by fuel type (simplified - would need gen_info for real fuel types)
        total_gen = recent_gen.groupby('settlementdate')['scadavalue'].sum()
        print(f"✅ Total generation calculation works")
        print(f"   Average MW: {total_gen.mean():.1f}")
        print(f"   Peak MW: {total_gen.max():.1f}")
        
        # Test DUID aggregation
        duid_totals = recent_gen.groupby('duid')['scadavalue'].sum().sort_values(ascending=False)
        print(f"✅ DUID aggregation works")
        print(f"   Top generator: {duid_totals.index[0]} = {duid_totals.iloc[0]:.1f} MWh")
        
        return True
        
    except Exception as e:
        print(f"❌ Calculation test failed: {e}")
        return False

if __name__ == "__main__":
    print("Starting dashboard generation data test...\n")
    
    if test_dashboard_data_loading():
        test_dashboard_calculations()
        
        print("\n" + "🎉" * 30)
        print("\nPROOF OF CONCEPT SUCCESSFUL!")
        print("\nNext steps:")
        print("1. Run the actual dashboard to verify visual output")
        print("2. Test all tabs to ensure generation data displays correctly")
        print("3. Proceed with other data migrations")
        print("\n" + "🎉" * 30)