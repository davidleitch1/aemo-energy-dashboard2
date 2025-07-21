#!/usr/bin/env python3
"""
Check local rooftop data file.
"""
import pandas as pd
from pathlib import Path

def check_local_rooftop():
    """Check local rooftop file."""
    local_file = Path("data/rooftop_solar.parquet")
    
    if local_file.exists():
        print(f"Found local rooftop file: {local_file}")
        
        df = pd.read_parquet(local_file)
        print(f"\nShape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Check dates
        if 'settlementdate' in df.columns:
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            print(f"\nDate range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            
            # Check 2025
            df_2025 = df[df['settlementdate'].dt.year == 2025]
            print(f"\n2025 records: {len(df_2025)}")
            if not df_2025.empty:
                print(f"2025 date range: {df_2025['settlementdate'].min()} to {df_2025['settlementdate'].max()}")
        
        # Check format
        print(f"\nFirst few rows:")
        print(df.head())
    else:
        print(f"Local file not found: {local_file}")
        
    # Also check .env for ROOFTOP_SOLAR_FILE
    from dotenv import load_dotenv
    import os
    load_dotenv()
    
    rooftop_env = os.getenv('ROOFTOP_SOLAR_FILE')
    print(f"\nROOFTOP_SOLAR_FILE from .env: {rooftop_env}")

if __name__ == "__main__":
    check_local_rooftop()