#!/usr/bin/env python3
"""
Test battery calculations for Hornsdale Power Reserve across all frequencies.
This script tests that energy and revenue totals are consistent regardless of 
the frequency selected (5 min, 30 min, 1 hour, daily).
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

# Import the data service
from src.data_service.shared_data_duckdb import duckdb_data_service

def get_battery_data(duid, start_date, end_date, resolution='30min'):
    """Get battery data at specified resolution."""
    
    # Select appropriate table based on resolution
    if resolution == '5min':
        gen_table = 'generation_5min'
        price_table = 'prices_5min'
    else:
        gen_table = 'generation_30min'
        price_table = 'prices_30min'
    
    # Query for battery data
    query = f"""
        SELECT 
            g.settlementdate as SETTLEMENTDATE,
            g.duid as DUID,
            g.scadavalue as SCADAVALUE,
            p.rrp as RRP
        FROM {gen_table} g
        JOIN {price_table} p 
          ON g.settlementdate = p.settlementdate 
          AND p.regionid = 'SA1'
        WHERE g.duid = '{duid}'
          AND g.settlementdate >= '{start_date.isoformat()}'
          AND g.settlementdate <= '{end_date.isoformat()}'
        ORDER BY g.settlementdate
    """
    
    df = duckdb_data_service.conn.execute(query).df()
    df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
    return df

def calculate_metrics(data, frequency, base_resolution):
    """Calculate battery metrics at specified frequency."""
    
    # Determine base time multiplier
    if base_resolution == '5min':
        base_time_multiplier = 1/12  # 5 minutes = 1/12 hours
    else:  # 30min
        base_time_multiplier = 0.5  # 30 minutes = 0.5 hours
    
    # Calculate MWh and revenue for each base period
    data = data.copy()
    data['MWH'] = data['SCADAVALUE'] * base_time_multiplier
    data['REVENUE'] = data['MWH'] * data['RRP']
    
    # Separate charge and discharge
    discharge_data = data[data['SCADAVALUE'] > 0].copy()
    charge_data = data[data['SCADAVALUE'] < 0].copy()
    
    # Apply frequency aggregation
    if frequency == '5min' and base_resolution == '5min':
        # No aggregation needed
        discharge_agg = discharge_data
        charge_agg = charge_data
    elif frequency == '30min' and base_resolution == '30min':
        # No aggregation needed
        discharge_agg = discharge_data
        charge_agg = charge_data
    elif frequency == '1hour':
        # Aggregate to hourly
        if not discharge_data.empty:
            discharge_agg = discharge_data.set_index('SETTLEMENTDATE').resample('1H').agg({
                'SCADAVALUE': 'mean',
                'MWH': 'sum',
                'REVENUE': 'sum',
                'RRP': 'mean'
            }).reset_index()
        else:
            discharge_agg = pd.DataFrame()
            
        if not charge_data.empty:
            charge_agg = charge_data.set_index('SETTLEMENTDATE').resample('1H').agg({
                'SCADAVALUE': 'mean',
                'MWH': 'sum',
                'REVENUE': 'sum',
                'RRP': 'mean'
            }).reset_index()
        else:
            charge_agg = pd.DataFrame()
    elif frequency == 'daily':
        # Aggregate to daily
        if not discharge_data.empty:
            discharge_agg = discharge_data.set_index('SETTLEMENTDATE').resample('1D').agg({
                'SCADAVALUE': 'mean',
                'MWH': 'sum',
                'REVENUE': 'sum',
                'RRP': 'mean'
            }).reset_index()
        else:
            discharge_agg = pd.DataFrame()
            
        if not charge_data.empty:
            charge_agg = charge_data.set_index('SETTLEMENTDATE').resample('1D').agg({
                'SCADAVALUE': 'mean',
                'MWH': 'sum',
                'REVENUE': 'sum',
                'RRP': 'mean'
            }).reset_index()
        else:
            charge_agg = pd.DataFrame()
    
    # Calculate totals
    metrics = {
        'frequency': frequency,
        'base_resolution': base_resolution,
        'discharge_energy_mwh': discharge_agg['MWH'].sum() if not discharge_agg.empty else 0,
        'charge_energy_mwh': abs(charge_agg['MWH'].sum()) if not charge_agg.empty else 0,
        'discharge_revenue': discharge_agg['REVENUE'].sum() if not discharge_agg.empty else 0,
        'charge_cost': abs(charge_agg['REVENUE'].sum()) if not charge_agg.empty else 0,
        'num_discharge_periods': len(discharge_agg),
        'num_charge_periods': len(charge_agg),
        'avg_discharge_price': (discharge_agg['REVENUE'].sum() / discharge_agg['MWH'].sum() 
                                if not discharge_agg.empty and discharge_agg['MWH'].sum() > 0 else 0),
        'avg_charge_price': (abs(charge_agg['REVENUE'].sum()) / abs(charge_agg['MWH'].sum()) 
                            if not charge_agg.empty and charge_agg['MWH'].sum() < 0 else 0)
    }
    
    metrics['gross_profit'] = metrics['discharge_revenue'] - metrics['charge_cost']
    
    return metrics

def main():
    """Main test function."""
    
    # Initialize DuckDB service
    print("Initializing DuckDB service...")
    
    # Get the latest full 24 hours of data
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=1)
    
    print(f"\nTest Period: {start_date} to {end_date}")
    print("Battery: Hornsdale Power Reserve Unit 1 (HPR1)")
    print("Region: SA1")
    print("="*80)
    
    results = []
    
    # Test 5-minute frequency
    print("\nTesting 5-minute frequency...")
    data_5min = get_battery_data('HPR1', start_date, end_date, resolution='5min')
    if not data_5min.empty:
        metrics = calculate_metrics(data_5min, '5min', '5min')
        results.append(metrics)
        print(f"  Records loaded: {len(data_5min)}")
    else:
        print("  No 5-minute data available")
    
    # Test 30-minute frequency with 30-min base data
    print("\nTesting 30-minute frequency...")
    data_30min = get_battery_data('HPR1', start_date, end_date, resolution='30min')
    if not data_30min.empty:
        metrics = calculate_metrics(data_30min, '30min', '30min')
        results.append(metrics)
        print(f"  Records loaded: {len(data_30min)}")
    
    # Test 1-hour frequency with 30-min base data
    print("\nTesting 1-hour frequency (from 30-min data)...")
    if not data_30min.empty:
        metrics = calculate_metrics(data_30min, '1hour', '30min')
        results.append(metrics)
    
    # Test daily frequency with 30-min base data
    print("\nTesting daily frequency (from 30-min data)...")
    if not data_30min.empty:
        metrics = calculate_metrics(data_30min, 'daily', '30min')
        results.append(metrics)
    
    # Create comparison DataFrame
    if results:
        df_results = pd.DataFrame(results)
        
        # Format for display
        print("\n" + "="*80)
        print("COMPARISON TABLE - BATTERY CALCULATIONS")
        print("="*80)
        
        # Round numeric columns for display
        display_cols = ['frequency', 'base_resolution', 'discharge_energy_mwh', 'charge_energy_mwh', 
                       'discharge_revenue', 'charge_cost', 'gross_profit', 
                       'avg_discharge_price', 'avg_charge_price']
        
        df_display = df_results[display_cols].copy()
        
        # Format numeric columns
        for col in df_display.columns:
            if col not in ['frequency', 'base_resolution']:
                if 'energy' in col:
                    df_display[col] = df_display[col].round(1)
                elif 'revenue' in col or 'cost' in col or 'profit' in col:
                    df_display[col] = df_display[col].round(0)
                elif 'price' in col:
                    df_display[col] = df_display[col].round(2)
        
        print(df_display.to_string(index=False))
        
        # Calculate percentage differences from 30-min baseline
        if len(results) > 1:
            print("\n" + "="*80)
            print("PERCENTAGE DIFFERENCES FROM 30-MIN BASELINE")
            print("="*80)
            
            baseline_idx = next((i for i, r in enumerate(results) if r['frequency'] == '30min'), 0)
            baseline = results[baseline_idx]
            
            for i, result in enumerate(results):
                if i != baseline_idx:
                    print(f"\n{result['frequency'].upper()} vs 30-MIN:")
                    
                    # Calculate percentage differences for key metrics
                    metrics_to_compare = [
                        ('Discharge Energy', 'discharge_energy_mwh'),
                        ('Charge Energy', 'charge_energy_mwh'),
                        ('Discharge Revenue', 'discharge_revenue'),
                        ('Charge Cost', 'charge_cost'),
                        ('Gross Profit', 'gross_profit')
                    ]
                    
                    for name, key in metrics_to_compare:
                        baseline_val = baseline[key]
                        current_val = result[key]
                        if baseline_val != 0:
                            diff_pct = ((current_val - baseline_val) / abs(baseline_val)) * 100
                            print(f"  {name}: {diff_pct:+.2f}%")
                        else:
                            print(f"  {name}: N/A (baseline is 0)")
        
        # Check if totals match
        print("\n" + "="*80)
        print("CONSISTENCY CHECK")
        print("="*80)
        
        # Check if key totals are identical
        energy_discharge_values = df_results['discharge_energy_mwh'].unique()
        energy_charge_values = df_results['charge_energy_mwh'].unique()
        revenue_values = df_results['discharge_revenue'].unique()
        cost_values = df_results['charge_cost'].unique()
        
        print(f"Unique discharge energy values: {len(energy_discharge_values)}")
        if len(energy_discharge_values) > 1:
            print(f"  Values: {energy_discharge_values}")
            print(f"  Range: {energy_discharge_values.min():.1f} to {energy_discharge_values.max():.1f}")
            print(f"  Max difference: {energy_discharge_values.max() - energy_discharge_values.min():.1f} MWh")
        
        print(f"Unique charge energy values: {len(energy_charge_values)}")
        if len(energy_charge_values) > 1:
            print(f"  Values: {energy_charge_values}")
            print(f"  Range: {energy_charge_values.min():.1f} to {energy_charge_values.max():.1f}")
            print(f"  Max difference: {energy_charge_values.max() - energy_charge_values.min():.1f} MWh")
        
        print(f"Unique discharge revenue values: {len(revenue_values)}")
        if len(revenue_values) > 1:
            print(f"  Range: ${revenue_values.min():,.0f} to ${revenue_values.max():,.0f}")
            print(f"  Max difference: ${revenue_values.max() - revenue_values.min():,.0f}")
        
        print(f"Unique charge cost values: {len(cost_values)}")
        if len(cost_values) > 1:
            print(f"  Range: ${cost_values.min():,.0f} to ${cost_values.max():,.0f}")
            print(f"  Max difference: ${cost_values.max() - cost_values.min():,.0f}")
        
        if (len(energy_discharge_values) == 1 and len(energy_charge_values) == 1 and 
            len(revenue_values) == 1 and len(cost_values) == 1):
            print("\n✅ SUCCESS: All frequencies show identical totals!")
        else:
            print("\n❌ ISSUE: Totals differ between frequencies")
    
    else:
        print("\nNo data available for testing")

if __name__ == "__main__":
    main()