#!/usr/bin/env python3
"""
Extract last week of South Australian generation and price data for testing.
Saves to CSV file for standalone generation stack plot testing.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pickle

# Production data path
DATA_PATH = Path("/Volumes/davidleitch/aemo_production/data")

def extract_sa_data():
    """Extract last week of SA generation and price data at 30-minute resolution"""
    
    print("Starting SA data extraction...")
    print(f"Data path: {DATA_PATH}")
    
    # Check if path exists
    if not DATA_PATH.exists():
        print(f"ERROR: Data path {DATA_PATH} does not exist")
        print("Please ensure the production drive is mounted")
        return None
    
    # Use last 7 days - should now have charging data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        # Load generator info to map DUIDs to regions and fuel types
        gen_info_file = DATA_PATH / "gen_info.pkl"
        if not gen_info_file.exists():
            print(f"ERROR: Generator info file {gen_info_file} not found")
            return None
            
        print(f"Loading generator info from {gen_info_file}...")
        with open(gen_info_file, 'rb') as f:
            gen_info_df = pickle.load(f)
        
        # The gen_info is actually a DataFrame, not a dict
        # Filter for SA generators
        sa_gen_info = gen_info_df[gen_info_df['Region'] == 'SA1'].copy()
        sa_duids = sa_gen_info['DUID'].tolist()
        print(f"Found {len(sa_duids)} SA generators")
        
        # Create DUID to fuel type mapping
        duid_to_fuel = dict(zip(sa_gen_info['DUID'], sa_gen_info['Fuel']))
        
        # Load 30-minute generation data
        gen_file = DATA_PATH / "scada30.parquet"
        if not gen_file.exists():
            print(f"ERROR: Generation file {gen_file} not found")
            return None
            
        print(f"Loading generation data from {gen_file}...")
        gen_data = pd.read_parquet(gen_file)
        
        # Filter for SA DUIDs and date range
        gen_data['settlementdate'] = pd.to_datetime(gen_data['settlementdate'])
        sa_gen = gen_data[
            (gen_data['duid'].isin(sa_duids)) & 
            (gen_data['settlementdate'] >= start_date) &
            (gen_data['settlementdate'] <= end_date)
        ].copy()
        
        print(f"Found {len(sa_gen)} generation records for SA")
        
        # Add fuel type from mapping
        sa_gen['fuel_type'] = sa_gen['duid'].map(duid_to_fuel)
        sa_gen['regionid'] = 'SA1'
        
        # Load 30-minute price data
        price_file = DATA_PATH / "prices30.parquet"
        if not price_file.exists():
            print(f"ERROR: Price file {price_file} not found")
            return None
            
        print(f"Loading price data from {price_file}...")
        price_data = pd.read_parquet(price_file)
        
        # Filter for SA and date range
        price_data['settlementdate'] = pd.to_datetime(price_data['settlementdate'])
        sa_prices = price_data[
            (price_data['regionid'] == 'SA1') & 
            (price_data['settlementdate'] >= start_date) &
            (price_data['settlementdate'] <= end_date)
        ][['settlementdate', 'rrp']].copy()
        
        print(f"Found {len(sa_prices)} price records for SA")
        
        # Check for battery charging (negative values)
        battery_data = sa_gen[sa_gen['fuel_type'] == 'Battery Storage']
        if len(battery_data) > 0:
            negatives = battery_data[battery_data['scadavalue'] < 0]
            print(f"\nBattery Storage Analysis:")
            print(f"  Total battery records: {len(battery_data)}")
            print(f"  Charging records (negative): {len(negatives)} ({len(negatives)/len(battery_data)*100:.1f}%)")
            if len(negatives) > 0:
                print(f"  Max charging rate: {abs(negatives['scadavalue'].min()):.1f} MW")
                print(f"  ✅ Battery charging data present!")
        
        # Pivot generation data to have fuel types as columns
        # Aggregate by settlementdate and fuel_type (sum all DUIDs of same fuel type)
        sa_gen_pivot = sa_gen.pivot_table(
            index='settlementdate',
            columns='fuel_type', 
            values='scadavalue',  # The actual generation value column
            aggfunc='sum',
            fill_value=0
        )
        
        # Load rooftop solar data
        rooftop_file = DATA_PATH / "rooftop30.parquet"
        if rooftop_file.exists():
            print(f"\nLoading rooftop solar data...")
            rooftop_data = pd.read_parquet(rooftop_file)
            rooftop_data['settlementdate'] = pd.to_datetime(rooftop_data['settlementdate'])
            
            # Filter for SA and date range
            sa_rooftop = rooftop_data[
                (rooftop_data['regionid'] == 'SA1') & 
                (rooftop_data['settlementdate'] >= start_date) &
                (rooftop_data['settlementdate'] <= end_date)
            ][['settlementdate', 'power']].copy()
            # Rename to rooftop_solar for clarity
            sa_rooftop = sa_rooftop.rename(columns={'power': 'rooftop_solar'})
            
            print(f"Found {len(sa_rooftop)} rooftop solar records")
            
            # Add rooftop solar to the pivot table
            sa_gen_pivot = sa_gen_pivot.merge(
                sa_rooftop.set_index('settlementdate')['rooftop_solar'],
                left_index=True,
                right_index=True,
                how='left'
            )
            # Rename the column to be consistent
            sa_gen_pivot = sa_gen_pivot.rename(columns={'rooftop_solar': 'Rooftop Solar'})
            sa_gen_pivot['Rooftop Solar'] = sa_gen_pivot['Rooftop Solar'].fillna(0)
        
        # Merge with prices
        sa_combined = sa_gen_pivot.merge(
            sa_prices.set_index('settlementdate'),
            left_index=True,
            right_index=True,
            how='left'
        )
        
        # Reset index to have settlementdate as a column
        sa_combined = sa_combined.reset_index()
        
        # Add region column
        sa_combined['region'] = 'SA1'
        
        # Reorder columns - put metadata first, then generation by fuel, then price
        metadata_cols = ['settlementdate', 'region']
        price_cols = ['rrp']
        fuel_cols = [col for col in sa_combined.columns if col not in metadata_cols + price_cols]
        
        # Sort fuel columns alphabetically for consistency
        fuel_cols.sort()
        
        # Final column order
        sa_combined = sa_combined[metadata_cols + fuel_cols + price_cols]
        
        # Sort by time
        sa_combined = sa_combined.sort_values('settlementdate')
        
        print(f"\nFinal dataset shape: {sa_combined.shape}")
        print(f"Columns: {list(sa_combined.columns)}")
        print(f"Date range: {sa_combined['settlementdate'].min()} to {sa_combined['settlementdate'].max()}")
        print(f"Fuel types found: {fuel_cols}")
        
        # Show sample data
        print("\nFirst few rows:")
        print(sa_combined.head())
        
        print("\nLast few rows:")
        print(sa_combined.tail())
        
        # Save to CSV
        output_file = Path("sa_generation_price_test_data.csv")
        sa_combined.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
        print(f"File size: {output_file.stat().st_size / 1024:.1f} KB")
        
        # Print summary statistics
        print("\nSummary statistics:")
        print(f"Total generation by fuel type (MWh):")
        for fuel in fuel_cols:
            total_mwh = sa_combined[fuel].sum() / 2  # Convert MW to MWh (30-min periods)
            if total_mwh > 0:
                print(f"  {fuel}: {total_mwh:,.0f} MWh")
        
        print(f"\nPrice statistics:")
        print(f"  Mean: ${sa_combined['rrp'].mean():.2f}/MWh")
        print(f"  Min: ${sa_combined['rrp'].min():.2f}/MWh")
        print(f"  Max: ${sa_combined['rrp'].max():.2f}/MWh")
        
        return sa_combined
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    data = extract_sa_data()
    if data is not None:
        print("\nExtraction complete!")
    else:
        print("\nExtraction failed!")