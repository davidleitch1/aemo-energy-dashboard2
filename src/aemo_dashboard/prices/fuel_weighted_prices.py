"""Fuel-weighted price computation for the Prices tab.

Extracted from gen_dash.py — computes demand-weighted average prices
by fuel type for each NEM region.
"""

import logging
import pickle

import numpy as np
import pandas as pd

from ..shared.config import config

logger = logging.getLogger(__name__)

# Consolidated fuel type mapping
FUEL_TYPE_MAPPING = {
    'Battery Storage': 'Battery',
    'OCGT': 'Gas',
    'CCGT': 'Gas',
    'Gas other': 'Gas',
    'Water': 'Hydro',
    'Coal': 'Coal',
    'Wind': 'Wind',
    'Solar': 'Solar',
    'Biomass': 'Other',
    'Other': 'Other',
    '': 'Other',
}

FUEL_DISPLAY_ORDER = ['Battery', 'Gas', 'Hydro', 'Coal', 'Wind', 'Solar']

GEN_INFO_FILE = config.gen_info_file


def _load_duid_mapping():
    """Load and normalise the DUID mapping DataFrame."""
    with open(GEN_INFO_FILE, 'rb') as f:
        duid_mapping = pickle.load(f)
    if 'Fuel' in duid_mapping.columns:
        duid_mapping = duid_mapping.rename(columns={'Fuel': 'FUEL_TYPE', 'Region': 'REGIONID'})
    return duid_mapping


def _ensure_column_names(gen_data):
    """Rename lowercase column names to the uppercase convention used here."""
    rename_map = {}
    if 'settlementdate' in gen_data.columns:
        rename_map['settlementdate'] = 'SETTLEMENTDATE'
    if 'duid' in gen_data.columns:
        rename_map['duid'] = 'DUID'
    if 'scadavalue' in gen_data.columns:
        rename_map['scadavalue'] = 'SCADAVALUE'
    if rename_map:
        gen_data = gen_data.rename(columns=rename_map)
    return gen_data


def compute_fuel_weighted_prices(
    gen_data,
    original_price_data,
    original_30min_price_data,
    selected_regions,
    freq,
    query_manager=None,
    start_datetime=None,
    end_datetime=None,
):
    """Compute fuel-weighted (demand-weighted average) prices per region.

    Parameters
    ----------
    gen_data : DataFrame
        Generation data with DUID-level output.
    original_price_data : DataFrame
        Price data (possibly resampled to *freq*).
    original_30min_price_data : DataFrame
        The un-resampled 30-min price data.
    selected_regions : list[str]
        Regions to compute for (e.g. ``['NSW1', 'VIC1']``).
    freq : str
        Pandas frequency string currently in use.
    query_manager : GenerationQueryManager, optional
        Used as fallback to load generation data.
    start_datetime, end_datetime : datetime, optional
        Date range for fallback generation query.

    Returns
    -------
    fuel_prices_by_region : dict[str, dict[str, str]]
        ``{region: {fuel_type: formatted_price_str}}``.
    """
    duid_mapping = _load_duid_mapping()

    if gen_data.empty:
        # Fallback via query_manager
        if query_manager is not None and start_datetime is not None:
            from ..shared.adapter_selector import load_generation_data
            gen_data = load_generation_data(
                start_date=start_datetime,
                end_date=end_datetime,
                region=selected_regions[0],
                resolution='auto',
            )
        if gen_data.empty:
            logger.warning("No generation data for fuel-weighted prices")
            return {}

    gen_data = _ensure_column_names(gen_data)

    # Add region/fuel info from DUID mapping if missing
    if 'REGIONID' not in gen_data.columns and 'DUID' in gen_data.columns:
        gen_data = gen_data.merge(
            duid_mapping[['DUID', 'REGIONID', 'FUEL_TYPE']],
            on='DUID',
            how='left',
        )

    gen_data = gen_data[gen_data['REGIONID'].isin(selected_regions)]
    gen_data['FUEL_TYPE_CONSOLIDATED'] = (
        gen_data['FUEL_TYPE'].map(FUEL_TYPE_MAPPING).fillna('Other')
    )

    fuel_prices_by_region = {}

    for region in selected_regions:
        fuel_prices_by_region[region] = {}
        region_gen_data = gen_data[gen_data['REGIONID'] == region].copy()
        region_price_data = original_price_data[original_price_data['REGIONID'] == region].copy()

        original_region_gen_data = region_gen_data.copy()
        original_region_price_data = original_30min_price_data[
            original_30min_price_data['REGIONID'] == region
        ].copy()

        # Resample generation for display if needed
        if not region_gen_data.empty and not region_price_data.empty:
            price_periods = len(region_price_data['SETTLEMENTDATE'].unique())
            gen_periods = len(region_gen_data['SETTLEMENTDATE'].unique())
            if price_periods < gen_periods and freq not in ('5min', '30min'):
                region_gen_data = (
                    region_gen_data
                    .groupby([
                        'FUEL_TYPE_CONSOLIDATED',
                        pd.Grouper(key='SETTLEMENTDATE', freq=freq),
                    ])
                    .agg({'SCADAVALUE': 'sum', 'DUID': 'first', 'REGIONID': 'first'})
                    .reset_index()
                )

        if region_gen_data.empty or region_price_data.empty:
            continue

        region_gen_data['SETTLEMENTDATE'] = pd.to_datetime(region_gen_data['SETTLEMENTDATE'])
        region_price_data['SETTLEMENTDATE'] = pd.to_datetime(region_price_data['SETTLEMENTDATE'])

        for fuel_type in FUEL_DISPLAY_ORDER:
            if freq in ('D', 'M', 'Q', 'Y'):
                fuel_gen = original_region_gen_data[
                    original_region_gen_data['FUEL_TYPE_CONSOLIDATED'] == fuel_type
                ].copy()
                use_original_prices = True
            else:
                fuel_gen = region_gen_data[
                    region_gen_data['FUEL_TYPE_CONSOLIDATED'] == fuel_type
                ].copy()
                use_original_prices = False

            if fuel_gen.empty:
                fuel_prices_by_region[region][fuel_type] = "-"
                continue

            # Battery: discharge only
            if fuel_type == 'Battery':
                fuel_gen = fuel_gen[fuel_gen['SCADAVALUE'] > 0].copy()

            if fuel_gen.empty:
                fuel_prices_by_region[region][fuel_type] = "-"
                continue

            fuel_gen_agg = fuel_gen.groupby('SETTLEMENTDATE')['SCADAVALUE'].sum().reset_index()

            price_source = (
                original_region_price_data if use_original_prices else region_price_data
            )
            merged = pd.merge(
                fuel_gen_agg,
                price_source[['SETTLEMENTDATE', 'RRP']],
                on='SETTLEMENTDATE',
                how='inner',
            )

            if merged.empty or merged['SCADAVALUE'].sum() <= 0:
                fuel_prices_by_region[region][fuel_type] = "-"
                continue

            if freq in ('D', 'M', 'Q', 'Y'):
                hours_per_period = 0.5
            else:
                if len(merged) > 1:
                    td = merged['SETTLEMENTDATE'].iloc[1] - merged['SETTLEMENTDATE'].iloc[0]
                    hours_per_period = td.total_seconds() / 3600
                else:
                    hours_per_period = 0.5

            revenue = (merged['SCADAVALUE'] * merged['RRP'] * hours_per_period).sum()
            energy = (merged['SCADAVALUE'] * hours_per_period).sum()
            weighted_price = revenue / energy if energy > 0 else 0
            fuel_prices_by_region[region][fuel_type] = f"{weighted_price:.0f}"

    return fuel_prices_by_region


def build_combined_stats_table(base_stats_df, fuel_prices_by_region, selected_regions):
    """Append fuel-weighted price rows to the base statistics DataFrame.

    Returns
    -------
    combined_df : DataFrame
        Base stats (Mean/Max/Min) plus one row per fuel type.
    """
    if not fuel_prices_by_region:
        return base_stats_df

    fuel_rows = []
    for fuel_type in FUEL_DISPLAY_ORDER:
        row = {'Statistic': fuel_type}
        for region in selected_regions:
            row[region] = fuel_prices_by_region.get(region, {}).get(fuel_type, "-")
        fuel_rows.append(row)

    fuel_df = pd.DataFrame(fuel_rows)
    return pd.concat([base_stats_df, fuel_df], ignore_index=True)
