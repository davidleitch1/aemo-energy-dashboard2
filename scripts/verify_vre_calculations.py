#!/usr/bin/env python3
"""
Verify VRE calculations and compare different smoothing methods.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

def load_generation_data():
    """Load generation data from parquet files."""
    data_dir = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2")
    
    # Load scada30 data
    scada_file = data_dir / "scada30.parquet"
    gen_info_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl")
    
    print("Loading generation data...")
    df = pd.read_parquet(scada_file)
    gen_info = pd.read_pickle(gen_info_file)
    
    # Merge with fuel type (columns are capitalized in gen_info)
    df = df.merge(gen_info[['DUID', 'Fuel']].rename(columns={'DUID': 'duid', 'Fuel': 'fuel'}), 
                  on='duid', how='left')
    
    return df

def load_rooftop_data():
    """Load rooftop data."""
    data_dir = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2")
    rooftop_file = data_dir / "rooftop30.parquet"
    
    print("Loading rooftop data...")
    df = pd.read_parquet(rooftop_file)
    
    # Convert wide to long format if needed
    if 'power' not in df.columns:
        # Assuming wide format with regions as columns
        value_cols = [col for col in df.columns if col not in ['settlementdate', 'regionid']]
        df = df.melt(id_vars=['settlementdate'], value_vars=value_cols, 
                     var_name='regionid', value_name='power')
    
    return df

def calculate_vre_with_different_smoothing():
    """Calculate VRE with different smoothing methods to compare."""
    
    # Load data
    gen_df = load_generation_data()
    rooftop_df = load_rooftop_data()
    
    # Filter for VRE fuels
    vre_gen = gen_df[gen_df['fuel'].isin(['Wind', 'Solar'])].copy()
    
    # Aggregate by date and fuel
    print("\nAggregating generation data...")
    gen_by_fuel = vre_gen.groupby(['settlementdate', 'fuel'])['scadavalue'].sum().reset_index()
    gen_pivot = gen_by_fuel.pivot(index='settlementdate', columns='fuel', values='scadavalue').fillna(0)
    
    # Aggregate rooftop by date
    print("Aggregating rooftop data...")
    rooftop_agg = rooftop_df.groupby('settlementdate')['power'].sum().reset_index()
    rooftop_agg.set_index('settlementdate', inplace=True)
    
    # Combine all VRE
    print("Combining VRE data...")
    vre_combined = pd.DataFrame(index=gen_pivot.index)
    vre_combined['Wind'] = gen_pivot.get('Wind', 0)
    vre_combined['Solar'] = gen_pivot.get('Solar', 0)
    
    # Align rooftop data
    vre_combined['Rooftop'] = rooftop_agg['power'].reindex(vre_combined.index, fill_value=0)
    vre_combined['VRE_Total'] = vre_combined.sum(axis=1)
    
    # Filter for recent years
    vre_combined = vre_combined[vre_combined.index >= '2023-01-01'].copy()
    
    # Calculate different smoothing methods
    print("\nCalculating different smoothing methods...")
    
    # 1. Simple Moving Average (30 days)
    vre_combined['VRE_MA30'] = vre_combined['VRE_Total'].rolling(window=30*48, center=False).mean()
    
    # 2. Centered Moving Average (30 days)
    vre_combined['VRE_MA30_centered'] = vre_combined['VRE_Total'].rolling(window=30*48, center=True).mean()
    
    # 3. Exponential Weighted Moving Average
    vre_combined['VRE_EWM30'] = vre_combined['VRE_Total'].ewm(span=30*48, adjust=False).mean()
    
    # Annualise each method (MW to TWh)
    # Formula: MW * 24 hours * 365 days / 1,000,000
    annualisation_factor = 24 * 365 / 1_000_000
    
    vre_combined['VRE_MA30_TWh'] = vre_combined['VRE_MA30'] * annualisation_factor
    vre_combined['VRE_MA30_centered_TWh'] = vre_combined['VRE_MA30_centered'] * annualisation_factor
    vre_combined['VRE_EWM30_TWh'] = vre_combined['VRE_EWM30'] * annualisation_factor
    
    # Add year and day of year
    vre_combined['year'] = vre_combined.index.year
    vre_combined['day_of_year'] = vre_combined.index.dayofyear
    
    # Sample some values for comparison
    print("\n=== Comparison of smoothing methods ===")
    
    # Get values for specific dates
    test_dates = ['2024-07-01 12:00:00', '2025-01-15 12:00:00', '2025-07-15 12:00:00']
    
    for date_str in test_dates:
        try:
            date = pd.to_datetime(date_str)
            if date in vre_combined.index:
                row = vre_combined.loc[date]
                print(f"\nDate: {date}")
                print(f"  Raw VRE Total: {row['VRE_Total']:.1f} MW")
                print(f"  MA30: {row['VRE_MA30']:.1f} MW -> {row['VRE_MA30_TWh']:.1f} TWh")
                print(f"  MA30 Centered: {row['VRE_MA30_centered']:.1f} MW -> {row['VRE_MA30_centered_TWh']:.1f} TWh")
                print(f"  EWM30: {row['VRE_EWM30']:.1f} MW -> {row['VRE_EWM30_TWh']:.1f} TWh")
        except:
            pass
    
    # Check peak values for each year
    print("\n=== Peak annualised values by year ===")
    for year in [2023, 2024, 2025]:
        year_data = vre_combined[vre_combined['year'] == year]
        if not year_data.empty:
            print(f"\n{year}:")
            print(f"  MA30 Peak: {year_data['VRE_MA30_TWh'].max():.1f} TWh")
            print(f"  MA30 Centered Peak: {year_data['VRE_MA30_centered_TWh'].max():.1f} TWh")
            print(f"  EWM30 Peak: {year_data['VRE_EWM30_TWh'].max():.1f} TWh")
            print(f"  Date range: {year_data.index.min()} to {year_data.index.max()}")
    
    # Check individual fuel contributions
    print("\n=== Individual fuel contributions (30-day MA) ===")
    for fuel in ['Wind', 'Solar', 'Rooftop']:
        fuel_ma = vre_combined[fuel].rolling(window=30*48).mean()
        fuel_twh = fuel_ma * annualisation_factor
        
        print(f"\n{fuel}:")
        print(f"  2024 Peak: {fuel_twh[vre_combined['year']==2024].max():.1f} TWh")
        print(f"  2025 Peak: {fuel_twh[vre_combined['year']==2025].max():.1f} TWh")
    
    # Save sample data for inspection
    sample_output = vre_combined[['VRE_Total', 'VRE_MA30', 'VRE_EWM30', 
                                  'VRE_MA30_TWh', 'VRE_EWM30_TWh']].resample('D').mean()
    sample_output.to_csv('vre_calculation_sample.csv')
    print("\nSaved daily averages to vre_calculation_sample.csv")

if __name__ == "__main__":
    calculate_vre_with_different_smoothing()