"""
Tests for fuel-weighted price calculation in gen_dash.py Price Analysis tab.

Tests the data flow from generation query through column renaming to
fuel price computation. Must work for all selected regions, not just first.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ── helpers ──────────────────────────────────────────────────────────
def make_gen_data_from_view(regions, n_periods=48):
    """Simulate what query_generation_by_fuel returns per-region (DuckDB view)."""
    fuel_types = ['Coal', 'Wind', 'Solar', 'CCGT', 'Battery Storage', 'Water']
    rows = []
    base = datetime(2026, 3, 10)
    for region in regions:
        for i in range(n_periods):
            ts = base + timedelta(minutes=30 * i)
            for fuel in fuel_types:
                rows.append({
                    'settlementdate': ts,
                    'fuel_type': fuel,
                    'total_generation_mw': np.random.uniform(100, 2000),
                    'total_capacity_mw': 5000.0,
                    'unit_count': 10,
                    'region': region,  # added by per-region loop
                })
    return pd.DataFrame(rows)


def make_gen_data_from_fallback(regions, n_periods=48):
    """Simulate what load_generation_data returns (per-DUID raw data)."""
    duids = {
        'NSW1': [('LIDDELL', 'Coal'), ('BANGOWF1', 'Wind'), ('AVLSF1', 'Solar')],
        'QLD1': [('GLADSTONE', 'Coal'), ('COOlwf1', 'Wind'), ('DARLING1', 'Solar')],
        'SA1': [('TORRENS', 'Gas other'), ('HALLWF1', 'Wind'), ('LKBNWF1', 'Solar')],
        'VIC1': [('YWPS1', 'Coal'), ('ARWF1', 'Wind'), ('NUMURKAH', 'Solar')],
    }
    rows = []
    base = datetime(2026, 3, 10)
    for region in regions:
        for i in range(n_periods):
            ts = base + timedelta(minutes=30 * i)
            for duid, fuel in duids.get(region, duids['NSW1']):
                rows.append({
                    'settlementdate': ts,
                    'duid': duid,
                    'scadavalue': np.random.uniform(50, 500),
                    'region': region,  # added by per-region loop
                })
    return pd.DataFrame(rows)


def make_duid_mapping():
    """Simulate the DUID mapping pickle (GEN_INFO_FILE)."""
    entries = [
        ('LIDDELL', 'Coal', 'NSW1'), ('BANGOWF1', 'Wind', 'NSW1'),
        ('AVLSF1', 'Solar', 'NSW1'), ('GLADSTONE', 'Coal', 'QLD1'),
        ('COOPERT1', 'Wind', 'QLD1'), ('DARLING1', 'Solar', 'QLD1'),
        ('TORRENS', 'Gas other', 'SA1'), ('HALLWF1', 'Wind', 'SA1'),
        ('LKBNWF1', 'Solar', 'SA1'), ('YWPS1', 'Coal', 'VIC1'),
        ('ARWF1', 'Wind', 'VIC1'), ('NUMURKAH', 'Solar', 'VIC1'),
    ]
    return pd.DataFrame(entries, columns=['DUID', 'FUEL_TYPE', 'REGIONID'])


def make_price_data(regions, n_periods=48):
    """Simulate original_price_data with SETTLEMENTDATE, REGIONID, RRP."""
    rows = []
    base = datetime(2026, 3, 10)
    for region in regions:
        for i in range(n_periods):
            ts = base + timedelta(minutes=30 * i)
            rows.append({
                'SETTLEMENTDATE': ts,
                'REGIONID': region,
                'RRP': np.random.uniform(20, 200),
            })
    return pd.DataFrame(rows)


# ── Rename logic (extracted from gen_dash.py lines 5337-5365) ────────
def apply_column_renames(gen_data, duid_mapping):
    """
    Apply the column rename logic from gen_dash.py lines 5337-5365.
    This is the EXACT logic from the dashboard code.
    """
    if 'settlementdate' in gen_data.columns:
        gen_data = gen_data.rename(columns={'settlementdate': 'SETTLEMENTDATE'})
    if 'duid' in gen_data.columns:
        gen_data = gen_data.rename(columns={'duid': 'DUID'})
    if 'scadavalue' in gen_data.columns:
        gen_data = gen_data.rename(columns={'scadavalue': 'SCADAVALUE'})

    # Rename region column from view to match expected REGIONID
    if 'region' in gen_data.columns and 'REGIONID' not in gen_data.columns:
        gen_data = gen_data.rename(columns={'region': 'REGIONID'})

    # Add fuel type (and region if missing) from DUID mapping
    if 'FUEL_TYPE' not in gen_data.columns and 'fuel_type' not in gen_data.columns and 'DUID' in gen_data.columns:
        merge_cols = ['DUID', 'FUEL_TYPE']
        if 'REGIONID' not in gen_data.columns:
            merge_cols.append('REGIONID')
        gen_data = gen_data.merge(
            duid_mapping[merge_cols],
            on='DUID',
            how='left'
        )

    # Rename fuel_type from view to match expected FUEL_TYPE
    if 'fuel_type' in gen_data.columns and 'FUEL_TYPE' not in gen_data.columns:
        gen_data = gen_data.rename(columns={'fuel_type': 'FUEL_TYPE'})
    if 'total_generation_mw' in gen_data.columns and 'SCADAVALUE' not in gen_data.columns:
        gen_data = gen_data.rename(columns={'total_generation_mw': 'SCADAVALUE'})

    return gen_data


def compute_fuel_prices(gen_data, price_data, selected_regions):
    """
    Compute fuel-weighted prices per region. Returns fuel_prices_by_region dict.
    Extracted from gen_dash.py lines 5367-5498.
    """
    fuel_type_mapping = {
        'Battery Storage': 'Battery', 'OCGT': 'Gas', 'CCGT': 'Gas',
        'Gas other': 'Gas', 'Water': 'Hydro', 'Coal': 'Coal',
        'Wind': 'Wind', 'Solar': 'Solar', 'Biomass': 'Other',
        'Other': 'Other', '': 'Other',
    }
    fuel_display_order = ['Battery', 'Gas', 'Hydro', 'Coal', 'Wind', 'Solar']

    # Filter for selected regions
    gen_data = gen_data[gen_data['REGIONID'].isin(selected_regions)].copy()
    gen_data['FUEL_TYPE_CONSOLIDATED'] = gen_data['FUEL_TYPE'].map(fuel_type_mapping).fillna('Other')

    fuel_prices_by_region = {}
    for region in selected_regions:
        fuel_prices_by_region[region] = {}
        region_gen = gen_data[gen_data['REGIONID'] == region].copy()
        region_prices = price_data[price_data['REGIONID'] == region].copy()

        if region_gen.empty or region_prices.empty:
            for ft in fuel_display_order:
                fuel_prices_by_region[region][ft] = "-"
            continue

        region_gen['SETTLEMENTDATE'] = pd.to_datetime(region_gen['SETTLEMENTDATE'])
        region_prices['SETTLEMENTDATE'] = pd.to_datetime(region_prices['SETTLEMENTDATE'])

        for fuel_type in fuel_display_order:
            fuel_gen = region_gen[region_gen['FUEL_TYPE_CONSOLIDATED'] == fuel_type].copy()
            if fuel_type == 'Battery':
                fuel_gen = fuel_gen[fuel_gen['SCADAVALUE'] > 0].copy()

            if fuel_gen.empty:
                fuel_prices_by_region[region][fuel_type] = "-"
                continue

            fuel_gen_agg = fuel_gen.groupby('SETTLEMENTDATE')['SCADAVALUE'].sum().reset_index()
            merged = pd.merge(
                fuel_gen_agg,
                region_prices[['SETTLEMENTDATE', 'RRP']],
                on='SETTLEMENTDATE',
                how='inner'
            )

            if not merged.empty and merged['SCADAVALUE'].sum() > 0:
                revenue = (merged['SCADAVALUE'] * merged['RRP'] * 0.5).sum()
                energy = (merged['SCADAVALUE'] * 0.5).sum()
                weighted_price = revenue / energy if energy > 0 else 0
                fuel_prices_by_region[region][fuel_type] = f"{weighted_price:.0f}"
            else:
                fuel_prices_by_region[region][fuel_type] = "-"

    return fuel_prices_by_region


# ═══════════════════════════════════════════════════════════════
#                          TESTS
# ═══════════════════════════════════════════════════════════════

class TestColumnRenames:
    """Test that column renames produce required columns."""

    def test_view_path_has_fuel_type_after_rename(self):
        """DuckDB view data should have FUEL_TYPE after rename."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        result = apply_column_renames(gen_data, duid_mapping)
        assert 'FUEL_TYPE' in result.columns, f"FUEL_TYPE missing. Columns: {list(result.columns)}"

    def test_view_path_has_regionid_after_rename(self):
        """DuckDB view data should have REGIONID after rename."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        result = apply_column_renames(gen_data, duid_mapping)
        assert 'REGIONID' in result.columns, f"REGIONID missing. Columns: {list(result.columns)}"

    def test_view_path_has_scadavalue_after_rename(self):
        """DuckDB view data should have SCADAVALUE after rename."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        result = apply_column_renames(gen_data, duid_mapping)
        assert 'SCADAVALUE' in result.columns, f"SCADAVALUE missing. Columns: {list(result.columns)}"

    def test_fallback_path_has_fuel_type_after_rename(self):
        """Fallback load_generation_data should have FUEL_TYPE after rename.
        
        THIS IS THE KEY TEST — currently fails because fallback returns
        [settlementdate, duid, scadavalue] with no fuel_type column,
        and the DUID→FUEL_TYPE merge only works if REGIONID is absent
        AND DUID is present.
        """
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_fallback(regions)
        duid_mapping = make_duid_mapping()
        result = apply_column_renames(gen_data, duid_mapping)
        assert 'FUEL_TYPE' in result.columns, (
            f"FUEL_TYPE missing after fallback path rename. "
            f"Columns: {list(result.columns)}"
        )

    def test_fallback_path_has_regionid_after_rename(self):
        """Fallback path should have REGIONID after rename."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_fallback(regions)
        duid_mapping = make_duid_mapping()
        result = apply_column_renames(gen_data, duid_mapping)
        assert 'REGIONID' in result.columns, (
            f"REGIONID missing after fallback path rename. "
            f"Columns: {list(result.columns)}"
        )

    def test_fallback_path_has_scadavalue_after_rename(self):
        """Fallback path should have SCADAVALUE after rename."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_fallback(regions)
        duid_mapping = make_duid_mapping()
        result = apply_column_renames(gen_data, duid_mapping)
        assert 'SCADAVALUE' in result.columns


class TestFuelPriceComputation:
    """Test that fuel prices are computed for ALL selected regions."""

    def test_all_regions_have_coal_price(self):
        """Every selected region should have a Coal price (not '-')."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        gen_data = apply_column_renames(gen_data, duid_mapping)
        price_data = make_price_data(regions)

        fuel_prices = compute_fuel_prices(gen_data, price_data, regions)
        for region in regions:
            assert fuel_prices[region]['Coal'] != "-", (
                f"Coal price missing for {region}"
            )

    def test_all_regions_have_wind_price(self):
        """Every selected region should have a Wind price (not '-')."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        gen_data = apply_column_renames(gen_data, duid_mapping)
        price_data = make_price_data(regions)

        fuel_prices = compute_fuel_prices(gen_data, price_data, regions)
        for region in regions:
            assert fuel_prices[region]['Wind'] != "-", (
                f"Wind price missing for {region}"
            )

    def test_all_regions_present_in_results(self):
        """All selected regions must appear in fuel_prices_by_region."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        gen_data = apply_column_renames(gen_data, duid_mapping)
        price_data = make_price_data(regions)

        fuel_prices = compute_fuel_prices(gen_data, price_data, regions)
        for region in regions:
            assert region in fuel_prices, f"Region {region} missing from results"
            # At least some fuels should have numeric prices
            numeric_count = sum(1 for v in fuel_prices[region].values() if v != "-")
            assert numeric_count >= 2, (
                f"Region {region} has only {numeric_count} fuel prices: {fuel_prices[region]}"
            )

    def test_fallback_path_all_regions_have_prices(self):
        """Fallback path (load_generation_data) should also produce prices for all regions.
        
        THIS IS THE KEY END-TO-END TEST — the fallback path returns per-DUID data
        which needs DUID→FUEL_TYPE mapping via duid_mapping merge.
        """
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_data = make_gen_data_from_fallback(regions)
        duid_mapping = make_duid_mapping()
        gen_data = apply_column_renames(gen_data, duid_mapping)
        price_data = make_price_data(regions)

        fuel_prices = compute_fuel_prices(gen_data, price_data, regions)
        for region in regions:
            assert region in fuel_prices, f"Region {region} missing"
            numeric_count = sum(1 for v in fuel_prices[region].values() if v != "-")
            assert numeric_count >= 1, (
                f"Region {region} has no numeric fuel prices: {fuel_prices[region]}"
            )

    def test_fuel_prices_are_reasonable(self):
        """Fuel-weighted prices should be in a reasonable range (0-500 $/MWh)."""
        regions = ['NSW1', 'QLD1']
        gen_data = make_gen_data_from_view(regions)
        duid_mapping = make_duid_mapping()
        gen_data = apply_column_renames(gen_data, duid_mapping)
        price_data = make_price_data(regions)

        fuel_prices = compute_fuel_prices(gen_data, price_data, regions)
        for region in regions:
            for fuel, val in fuel_prices[region].items():
                if val != "-":
                    numeric_val = float(val)
                    assert 0 < numeric_val < 500, (
                        f"{fuel} in {region} has unreasonable price: {val}"
                    )


class TestLiveDuckDB:
    """Integration tests against the actual DuckDB on production.

    These verify the real query_generation_by_fuel returns data
    that flows correctly through the rename + fuel price logic.
    """

    @pytest.fixture
    def duckdb_conn(self):
        """Connect to the read-only DuckDB replica."""
        import duckdb
        db_path = '/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb'
        try:
            conn = duckdb.connect(db_path, read_only=True)
            yield conn
            conn.close()
        except Exception:
            pytest.skip("DuckDB not available")

    def test_per_region_query_columns(self, duckdb_conn):
        """Per-region query should return expected columns."""
        result = duckdb_conn.execute("""
            SELECT settlementdate, fuel_type, total_generation_mw,
                   total_capacity_mw, unit_count
            FROM generation_by_fuel_30min
            WHERE settlementdate >= '2026-03-10'
            AND settlementdate <= '2026-03-11'
            AND region = 'NSW1'
            LIMIT 5
        """).df()
        assert not result.empty, "Query returned no data"
        assert 'fuel_type' in result.columns
        assert 'total_generation_mw' in result.columns

    def test_per_region_query_renames_correctly(self, duckdb_conn):
        """Per-region DuckDB result + region tag should rename to correct columns."""
        result = duckdb_conn.execute("""
            SELECT settlementdate, fuel_type, total_generation_mw,
                   total_capacity_mw, unit_count
            FROM generation_by_fuel_30min
            WHERE settlementdate >= '2026-03-10'
            AND settlementdate <= '2026-03-11'
            AND region = 'NSW1'
        """).df()
        result['region'] = 'NSW1'  # as done in per-region loop
        duid_mapping = make_duid_mapping()
        renamed = apply_column_renames(result, duid_mapping)
        assert 'FUEL_TYPE' in renamed.columns, f"Columns: {list(renamed.columns)}"
        assert 'REGIONID' in renamed.columns, f"Columns: {list(renamed.columns)}"
        assert 'SCADAVALUE' in renamed.columns, f"Columns: {list(renamed.columns)}"
        assert 'SETTLEMENTDATE' in renamed.columns

    def test_multi_region_fuel_prices_from_duckdb(self, duckdb_conn):
        """Full end-to-end: query 4 regions, compute fuel prices, all should have results."""
        regions = ['NSW1', 'QLD1', 'SA1', 'VIC1']
        gen_parts = []
        for region in regions:
            r = duckdb_conn.execute(f"""
                SELECT settlementdate, fuel_type, total_generation_mw,
                       total_capacity_mw, unit_count
                FROM generation_by_fuel_30min
                WHERE settlementdate >= '2026-03-08'
                AND settlementdate <= '2026-03-11'
                AND region = '{region}'
            """).df()
            if not r.empty:
                r['region'] = region
                gen_parts.append(r)

        assert len(gen_parts) == 4, f"Only got data for {len(gen_parts)} regions"
        gen_data = pd.concat(gen_parts, ignore_index=True)

        # Get price data
        price_data = duckdb_conn.execute(f"""
            SELECT settlementdate as SETTLEMENTDATE, regionid as REGIONID, rrp as RRP
            FROM prices30
            WHERE settlementdate >= '2026-03-08'
            AND settlementdate <= '2026-03-11'
        """).df()

        duid_mapping = make_duid_mapping()
        gen_data = apply_column_renames(gen_data, duid_mapping)
        fuel_prices = compute_fuel_prices(gen_data, price_data, regions)

        for region in regions:
            assert region in fuel_prices, f"{region} missing from fuel prices"
            numeric = {k: v for k, v in fuel_prices[region].items() if v != "-"}
            assert len(numeric) >= 3, (
                f"{region}: only {len(numeric)} fuel prices: {fuel_prices[region]}"
            )
            # Coal present in NSW1, QLD1, VIC1 (SA1 has no coal)
            if region != 'SA1':
                assert fuel_prices[region].get('Coal') != "-", (
                    f"{region} missing Coal price: {fuel_prices[region]}"
                )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
