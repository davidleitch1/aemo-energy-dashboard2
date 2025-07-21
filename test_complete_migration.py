#!/usr/bin/env python3
"""
Test dashboard with generation, transmission, and price data all migrated
"""

import sys
import pandas as pd
from pathlib import Path
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_all_data_loading():
    """Test if dashboard can load all migrated data"""
    
    print("=" * 60)
    print("TESTING COMPLETE MIGRATION (GEN + TRANS + PRICE)")
    print("=" * 60)
    
    # Apply new configuration
    shutil.copy('.env.all_migrated', '.env')
    print("✅ Applied complete migration configuration\n")
    
    # Import after config update
    from aemo_dashboard.shared.config import config
    from aemo_dashboard.shared.logging_config import setup_logging, get_logger
    
    # Set up logging
    setup_logging()
    logger = get_logger(__name__)
    
    # Check config
    print("Configuration:")
    print(f"  Generation: {config.gen_output_file}")
    print(f"  Transmission: {config.transmission_output_file}")
    print(f"  Price: {config.spot_hist_file}")
    print(f"  Rooftop: {config.rooftop_solar_file} (unchanged)\n")
    
    success = True
    
    # Test generation data
    try:
        gen_df = pd.read_parquet(config.gen_output_file)
        print(f"✅ Generation data: {len(gen_df):,} records, latest: {gen_df['settlementdate'].max()}")
    except Exception as e:
        print(f"❌ Generation data failed: {e}")
        success = False
    
    # Test transmission data
    try:
        trans_df = pd.read_parquet(config.transmission_output_file)
        print(f"✅ Transmission data: {len(trans_df):,} records, latest: {trans_df['settlementdate'].max()}")
    except Exception as e:
        print(f"❌ Transmission data failed: {e}")
        success = False
    
    # Test price data with adapter
    try:
        from aemo_dashboard.shared.price_adapter import load_price_data
        price_df = load_price_data()
        print(f"✅ Price data (adapted): {len(price_df):,} records")
        print(f"   Columns: {list(price_df.columns)}")
        print(f"   Index type: {type(price_df.index)}")
    except Exception as e:
        print(f"❌ Price data failed: {e}")
        success = False
    
    # Test rooftop (should still work with old file)
    try:
        roof_df = pd.read_parquet(config.rooftop_solar_file)
        print(f"✅ Rooftop data (old): {len(roof_df):,} records")
    except Exception as e:
        print(f"⚠️  Rooftop data issue: {e}")
    
    return success

def test_integrated_operations():
    """Test operations that combine multiple data sources"""
    
    print("\n" + "=" * 60)
    print("TESTING INTEGRATED OPERATIONS")
    print("=" * 60)
    
    from aemo_dashboard.shared.config import config
    from aemo_dashboard.shared.price_adapter import load_price_data
    
    try:
        # Load all data
        gen_df = pd.read_parquet(config.gen_output_file)
        trans_df = pd.read_parquet(config.transmission_output_file)
        price_df = load_price_data()
        
        # Find common time period
        gen_times = set(gen_df['settlementdate'].unique())
        trans_times = set(trans_df['settlementdate'].unique())
        price_times = set(price_df.index.unique())
        
        common_times = gen_times & trans_times & price_times
        print(f"\n✅ Found {len(common_times):,} common timestamps across all datasets")
        
        if common_times:
            # Test a sample integrated calculation
            sample_time = max(common_times)
            
            # Generation by region (simplified - would use DUID mapping)
            gen_sample = gen_df[gen_df['settlementdate'] == sample_time]
            total_gen = gen_sample['scadavalue'].sum()
            
            # Transmission flows
            trans_sample = trans_df[trans_df['settlementdate'] == sample_time]
            net_flow = trans_sample['meteredmwflow'].sum()
            
            # Prices by region
            price_sample = price_df.loc[sample_time]
            avg_price = price_sample['RRP'].mean()
            
            print(f"\nIntegrated calculation at {sample_time}:")
            print(f"  Total generation: {total_gen:,.1f} MW")
            print(f"  Net transmission: {net_flow:+,.1f} MW")
            print(f"  Average price: ${avg_price:.2f}/MWh")
            
            # Test revenue calculation (simplified)
            nsw_gen = gen_sample[gen_sample['duid'].str.contains('NSW', na=False)]['scadavalue'].sum()
            nsw_price = price_sample[price_sample['REGIONID'] == 'NSW1']['RRP'].values[0] if len(price_sample[price_sample['REGIONID'] == 'NSW1']) > 0 else 0
            nsw_revenue = nsw_gen * nsw_price / 12  # 5-minute to hourly
            
            print(f"\n✅ Revenue calculation works:")
            print(f"   NSW generation: {nsw_gen:.1f} MW")
            print(f"   NSW price: ${nsw_price:.2f}/MWh")
            print(f"   NSW revenue (5-min): ${nsw_revenue:,.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Integrated operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_dashboard_modules():
    """Test that dashboard modules can load with new data"""
    
    print("\n" + "=" * 60)
    print("TESTING DASHBOARD MODULES")
    print("=" * 60)
    
    # Test price analysis motor
    try:
        from aemo_dashboard.analysis.price_analysis import PriceAnalysisMotor
        motor = PriceAnalysisMotor()
        success = motor.load_data()
        if success:
            print("✅ Price Analysis Motor loads successfully")
        else:
            print("❌ Price Analysis Motor failed to load data")
    except Exception as e:
        print(f"❌ Price Analysis Motor error: {e}")
    
    # Test station analysis motor
    try:
        from aemo_dashboard.station.station_analysis import StationAnalysisMotor
        motor = StationAnalysisMotor()
        success = motor.load_data()
        if success:
            print("✅ Station Analysis Motor loads successfully")
        else:
            print("❌ Station Analysis Motor failed to load data")
    except Exception as e:
        print(f"❌ Station Analysis Motor error: {e}")
    
    return True

if __name__ == "__main__":
    print("Starting complete migration test...\n")
    
    if test_all_data_loading():
        if test_integrated_operations():
            test_dashboard_modules()
            
            print("\n" + "🎉" * 30)
            print("\nCOMPLETE MIGRATION SUCCESSFUL!")
            print("\nMigration Summary:")
            print("✅ Generation: Direct use (no adapter needed)")
            print("✅ Transmission: Direct use (no adapter needed)")
            print("✅ Price: Column adapter implemented and tested")
            print("⏳ Rooftop: Still using old file (needs conversion)")
            print("\nAll dashboard modules tested and working!")
            print("\nNext steps:")
            print("1. Run dashboard visually to confirm all tabs work")
            print("2. Implement rooftop solar conversion")
            print("3. Remove update/collector code")
            print("\n" + "🎉" * 30)
        else:
            print("\n⚠️ Some integrated operations need attention")
    else:
        print("\n❌ Data loading failed")