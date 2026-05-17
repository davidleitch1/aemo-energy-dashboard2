"""Cross-check the per-interval revenue calculation against:
  1. Direct SQL aggregation (no band bucketing) — sanity check the SQL is doing what we think.
  2. Self-consistency — sum of contributions = mean price, per region.
  3. Production's shortcut formula (avg_price × hours × avg_demand) — quantify the bias.

Window: trailing 30 days from latest demand30 row. Regions: NSW1 + VIC1.
"""
import sys
import duckdb
import pandas as pd

DB = "/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb"
REGIONS = ["NSW1", "VIC1"]
WINDOW_DAYS = 30

conn = duckdb.connect(DB, read_only=True)
latest = conn.execute("SELECT MAX(settlementdate) FROM demand30").fetchone()[0]
e_ts = pd.Timestamp(latest) + pd.Timedelta(minutes=30)  # exclusive upper
s_ts = e_ts - pd.Timedelta(days=WINDOW_DAYS)
region_list = ",".join(f"'{r}'" for r in REGIONS)
print(f"Window: {s_ts} → {e_ts}  ({WINDOW_DAYS} days)")
print(f"Regions: {REGIONS}\n")

# -----------------------------------------------------------------------------
# Check 1. Σ(price × demand × 0.5) directly, no banding — the "truth" total
# -----------------------------------------------------------------------------
direct = conn.execute(f"""
    SELECT p.regionid,
           COUNT(*)                          AS n,
           AVG(p.rrp)                        AS avg_price,
           AVG(d.demand)                     AS avg_demand,
           SUM(p.rrp * d.demand * 0.5)       AS revenue_dollars,
           SUM(d.demand * 0.5)               AS energy_mwh
      FROM prices30 p
      JOIN demand30 d
        ON p.settlementdate = d.settlementdate
       AND p.regionid       = d.regionid
     WHERE p.settlementdate >= ? AND p.settlementdate < ?
       AND p.regionid IN ({region_list})
     GROUP BY p.regionid
     ORDER BY p.regionid
""", [s_ts, e_ts]).df()
direct["revenue_m"] = direct["revenue_dollars"] / 1e6
direct["vwap_implied"] = direct["revenue_dollars"] / direct["energy_mwh"]
print("[1] Direct per-region totals (truth):")
print(direct.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))

# -----------------------------------------------------------------------------
# Check 2. Banded version — sum across bands should reproduce (1) within
# whatever falls in the [300, 301) gap that production's CASE leaves out.
# -----------------------------------------------------------------------------
banded = conn.execute(f"""
    WITH joined AS (
        SELECT p.regionid, p.rrp, d.demand,
               CASE
                 WHEN p.rrp < 0                       THEN 'Below $0'
                 WHEN p.rrp >= 0    AND p.rrp < 300   THEN '$0-$300'
                 WHEN p.rrp >= 301  AND p.rrp < 1000  THEN '$301-$1000'
                 WHEN p.rrp >= 1000                   THEN 'Above $1000'
                 ELSE NULL
               END AS band
          FROM prices30 p
          JOIN demand30 d
            ON p.settlementdate = d.settlementdate
           AND p.regionid       = d.regionid
         WHERE p.settlementdate >= ? AND p.settlementdate < ?
           AND p.regionid IN ({region_list})
    )
    SELECT regionid, band,
           COUNT(*)                       AS n,
           AVG(rrp)                       AS band_avg_price,
           SUM(rrp * demand * 0.5)        AS revenue_dollars,
           SUM(demand * 0.5)              AS energy_mwh
      FROM joined
     WHERE band IS NOT NULL
     GROUP BY regionid, band
     ORDER BY regionid, band
""", [s_ts, e_ts]).df()

# Per-region band sums
band_sum = (banded.groupby("regionid")
            .agg(n_in_bands=("n", "sum"),
                 revenue_in_bands=("revenue_dollars", "sum"),
                 energy_in_bands=("energy_mwh", "sum"))
            .reset_index())

gap = conn.execute(f"""
    SELECT p.regionid, COUNT(*) AS n_gap_intervals,
           SUM(p.rrp * d.demand * 0.5) AS revenue_gap,
           SUM(d.demand * 0.5) AS energy_gap
      FROM prices30 p JOIN demand30 d
        ON p.settlementdate = d.settlementdate AND p.regionid = d.regionid
     WHERE p.settlementdate >= ? AND p.settlementdate < ?
       AND p.regionid IN ({region_list})
       AND p.rrp >= 300 AND p.rrp < 301
     GROUP BY p.regionid
""", [s_ts, e_ts]).df()
print("\n[2] Banded — per-band:")
print(banded.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
print("\n  Sum of band totals (should equal direct minus gap):")
print(band_sum.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
print(f"\n  Intervals in [$300, $301) gap (excluded by production CASE):")
print(gap.to_string(index=False) if not gap.empty else "  none")

merged = direct.merge(band_sum, on="regionid")
merged["delta_intervals"] = merged["n"] - merged["n_in_bands"]
merged["delta_revenue"] = merged["revenue_dollars"] - merged["revenue_in_bands"]
print("\n  Delta (direct − banded), should match gap:")
print(merged[["regionid", "delta_intervals", "delta_revenue"]].to_string(
    index=False, float_format=lambda x: f"{x:,.2f}"))

# -----------------------------------------------------------------------------
# Check 3. Self-consistency: Σ(band_pct × band_avg) = region mean price
# -----------------------------------------------------------------------------
print("\n[3] Self-consistency — Σ(pct × band_avg) per region should equal mean price:")
totals = banded.groupby("regionid")["n"].sum().rename("total_n")
banded2 = banded.merge(totals, on="regionid")
banded2["contribution"] = (banded2["n"] / banded2["total_n"]) * banded2["band_avg_price"]
contrib_sum = banded2.groupby("regionid")["contribution"].sum().rename("sum_contrib")
mean_check = direct[["regionid", "avg_price"]].merge(contrib_sum, on="regionid")
mean_check["delta"] = mean_check["avg_price"] - mean_check["sum_contrib"]
print(mean_check.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))

# -----------------------------------------------------------------------------
# Check 4. Compare to production's shortcut formula
# -----------------------------------------------------------------------------
print("\n[4] Production shortcut vs per-interval revenue, per band:")
production_shortcut = banded.copy()
total_hours = WINDOW_DAYS * 24
n_per_region = banded.groupby("regionid")["n"].transform("sum")
production_shortcut["pct_time"] = banded["n"] / n_per_region * 100
production_shortcut["hours_in_band"] = production_shortcut["pct_time"] / 100 * total_hours
avg_demand_map = direct.set_index("regionid")["avg_demand"].to_dict()
production_shortcut["avg_demand"] = production_shortcut["regionid"].map(avg_demand_map)
production_shortcut["revenue_shortcut"] = (
    production_shortcut["band_avg_price"]
    * production_shortcut["hours_in_band"]
    * production_shortcut["avg_demand"]
)
production_shortcut["revenue_exact"] = banded["revenue_dollars"]
production_shortcut["delta_pct"] = (
    (production_shortcut["revenue_exact"] - production_shortcut["revenue_shortcut"])
    / production_shortcut["revenue_shortcut"] * 100
)
cmp = production_shortcut[["regionid", "band", "revenue_shortcut",
                            "revenue_exact", "delta_pct"]]
cmp = cmp.assign(
    revenue_shortcut_m=cmp["revenue_shortcut"] / 1e6,
    revenue_exact_m=cmp["revenue_exact"] / 1e6,
)[["regionid", "band", "revenue_shortcut_m", "revenue_exact_m", "delta_pct"]]
print(cmp.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
print("\n  Interpretation: positive delta_pct = exact > shortcut (production understates).")
print("  Expected sign: positive for high-price bands (price-demand correlation),")
print("  near zero or slightly negative for $0-$300 (the bulk band).")
