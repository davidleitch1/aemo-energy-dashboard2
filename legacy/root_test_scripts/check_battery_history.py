#!/usr/bin/env python3
"""Check when missing batteries were last seen in scada data"""

import sys
sys.path.insert(0, 'src')
import duckdb
import pickle

conn = duckdb.connect(':memory:')

scada30_path = '/Volumes/davidleitch/aemo_production/data/scada30.parquet'
gen_info_path = '/Volumes/davidleitch/aemo_production/data/gen_info.pkl'

conn.execute(f"CREATE VIEW scada30 AS SELECT * FROM read_parquet('{scada30_path}')")

with open(gen_info_path, 'rb') as f:
    duid_df = pickle.load(f)
conn.register('duid_mapping', duid_df)

# Missing batteries list
missing = ['JRBATT1', 'ORANA', 'QPBESS', 'RESS1G', 'GBBATT1', 'SNB02', 'SNB03',
           'SNB04', 'SNB05', 'TieriBESS1', 'ULBESS', 'ULBESS1', 'LGAPBS1', 'TB2BG1',
           'BEER01', 'BIRKBESS', 'FTVBESS', 'KIAMBE', 'MOORBESS', 'SHBESS01', 'TRGBESS']

print("Checking historical data for missing batteries...")
print("="*80)

for duid in missing:
    result = conn.execute(f"""
        SELECT
            '{duid}' as duid,
            COUNT(*) as total_records,
            MIN(settlementdate) as first_seen,
            MAX(settlementdate) as last_seen,
            MAX(ABS(scadavalue)) as max_abs_mw
        FROM scada30
        WHERE duid = '{duid}'
    """).df()

    if result['total_records'].iloc[0] == 0:
        info = conn.execute(f"""
            SELECT "Site Name", Region, "Capacity(MW)"
            FROM duid_mapping
            WHERE DUID = '{duid}'
        """).df()
        print(f"{duid:12} | NEVER IN SCADA | {info['Site Name'].iloc[0]:40} | {info['Region'].iloc[0]:5} | {info['Capacity(MW)'].iloc[0]:6.0f} MW")
    else:
        info = conn.execute(f"""
            SELECT "Site Name", Region
            FROM duid_mapping
            WHERE DUID = '{duid}'
        """).df()
        print(f"{duid:12} | Last: {result['last_seen'].iloc[0]} | {info['Site Name'].iloc[0]:40} | {info['Region'].iloc[0]}")

# Check NEM-wide peak battery discharge
print("\n" + "="*80)
print("NEM-WIDE BATTERY PEAK (last 7 days - at each 30-min interval):")
print("="*80)

nem_timeseries = conn.execute("""
    SELECT
        s.settlementdate,
        SUM(CASE WHEN s.scadavalue > 0 THEN s.scadavalue ELSE 0 END) as total_discharge_mw,
        SUM(CASE WHEN s.scadavalue < 0 THEN ABS(s.scadavalue) ELSE 0 END) as total_charge_mw,
        SUM(s.scadavalue) as net_mw
    FROM scada30 s
    INNER JOIN duid_mapping d ON s.duid = d.DUID
    WHERE d.Fuel = 'Battery Storage'
      AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY s.settlementdate
    ORDER BY total_discharge_mw DESC
    LIMIT 10
""").df()

print("\nTop 10 discharge periods:")
print(nem_timeseries[['settlementdate', 'total_discharge_mw', 'total_charge_mw', 'net_mw']].to_string(index=False))

# Overall stats
stats = conn.execute("""
    SELECT
        MAX(total_discharge) as peak_discharge_mw,
        MAX(total_charge) as peak_charge_mw,
        AVG(total_discharge) as avg_discharge_mw,
        AVG(total_charge) as avg_charge_mw
    FROM (
        SELECT
            s.settlementdate,
            SUM(CASE WHEN s.scadavalue > 0 THEN s.scadavalue ELSE 0 END) as total_discharge,
            SUM(CASE WHEN s.scadavalue < 0 THEN ABS(s.scadavalue) ELSE 0 END) as total_charge
        FROM scada30 s
        INNER JOIN duid_mapping d ON s.duid = d.DUID
        WHERE d.Fuel = 'Battery Storage'
          AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY s.settlementdate
    ) t
""").df()

print(f"\nNEM-wide battery stats (last 7 days):")
print(f"  Peak discharge: {stats['peak_discharge_mw'].iloc[0]:.1f} MW")
print(f"  Peak charge: {stats['peak_charge_mw'].iloc[0]:.1f} MW")
print(f"  Avg discharge: {stats['avg_discharge_mw'].iloc[0]:.1f} MW")
print(f"  Avg charge: {stats['avg_charge_mw'].iloc[0]:.1f} MW")
