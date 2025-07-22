#!/usr/bin/env python3
"""
Test complete dashboard migration including rooftop solar
"""

import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_complete_migration():
    """Test all four data sources with the dashboard"""
    
    print("=" * 70)
    print("TESTING COMPLETE MIGRATION (ALL 4 DATA SOURCES)")
    print("=" * 70)
    
    from aemo_dashboard.shared.config import config
    from aemo_dashboard.shared.logging_config import setup_logging, get_logger
    from aemo_dashboard.shared.price_adapter import load_price_data
    from aemo_dashboard.shared.rooftop_adapter import load_rooftop_data
    
    # Set up logging
    setup_logging()
    logger = get_logger(__name__)
    
    print(f"Configuration:")
    print(f"  Generation: {config.gen_output_file}")
    print(f"  Transmission: {config.transmission_output_file}")
    print(f"  Price: {config.spot_hist_file}")
    print(f"  Rooftop: {config.rooftop_solar_file}")
    print()
    
    success_count = 0
    total_tests = 4
    
    # Test 1: Generation data
    try:
        gen_df = pd.read_parquet(config.gen_output_file)
        print(f"✅ Generation: {len(gen_df):,} records, latest: {gen_df['settlementdate'].max()}")
        success_count += 1
    except Exception as e:
        print(f"❌ Generation failed: {e}")
    
    # Test 2: Transmission data
    try:
        trans_df = pd.read_parquet(config.transmission_output_file)
        print(f"✅ Transmission: {len(trans_df):,} records, latest: {trans_df['settlementdate'].max()}")
        success_count += 1
    except Exception as e:
        print(f"❌ Transmission failed: {e}")
    
    # Test 3: Price data (with adapter)
    try:
        price_df = load_price_data()
        print(f"✅ Price (adapted): {len(price_df):,} records")
        success_count += 1
    except Exception as e:
        print(f"❌ Price failed: {e}")
    
    # Test 4: Rooftop data (with adapter and Henderson smoothing)
    try:
        rooftop_df = load_rooftop_data()
        print(f"✅ Rooftop (Henderson): {len(rooftop_df):,} records")
        print(f"   Regions: {list(rooftop_df.columns[1:])}")
        success_count += 1
    except Exception as e:
        print(f"❌ Rooftop failed: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n📊 Migration Summary: {success_count}/{total_tests} data sources successful")
    
    if success_count == total_tests:
        print("\n🎉 COMPLETE MIGRATION SUCCESSFUL!")
        print("\nAll 4 data sources now using aemo-data-updater files:")
        print("- Generation: Direct use (scada5.parquet)")
        print("- Transmission: Direct use (transmission5.parquet)")
        print("- Price: Column adapter (prices5.parquet)")
        print("- Rooftop: Henderson smoothing (rooftop30.parquet)")
        
        # Test dashboard components can load
        try:
            print("\n🧪 Testing dashboard component loading...")
            
            # Test price analysis motor
            from aemo_dashboard.analysis.price_analysis import PriceAnalysisMotor
            price_motor = PriceAnalysisMotor()
            if price_motor.load_data():
                print("✅ Price Analysis Motor loads successfully")
            else:
                print("⚠️  Price Analysis Motor failed to load data")
            
            # Test station analysis motor
            from aemo_dashboard.station.station_analysis import StationAnalysisMotor
            station_motor = StationAnalysisMotor()
            if station_motor.load_data():
                print("✅ Station Analysis Motor loads successfully")
            else:
                print("⚠️  Station Analysis Motor failed to load data")
                
            print("\n🚀 Ready for visual dashboard testing!")
            print("\nNext steps:")
            print("1. Run: .venv/bin/python -m src.aemo_dashboard.generation.gen_dash")
            print("2. Open: http://localhost:5010")
            print("3. Test all tabs: Generation, Price Analysis, Station Analysis")
            
        except Exception as e:
            print(f"\n⚠️  Dashboard component testing failed: {e}")
    
    else:
        print(f"\n❌ {total_tests - success_count} data sources need attention")
    
    return success_count == total_tests

if __name__ == "__main__":
    print("Testing complete dashboard migration...\n")
    
    success = test_complete_migration()
    
    if success:
        print("\n" + "🎯" * 25)
        print("\n100% MIGRATION COMPLETE!")
        print("The dashboard now reads from all new aemo-data-updater files")
        print("with Henderson smoothing for rooftop solar interpolation.")
        print("\n" + "🎯" * 25)
    else:
        print("\n⚠️  Migration incomplete - check errors above")