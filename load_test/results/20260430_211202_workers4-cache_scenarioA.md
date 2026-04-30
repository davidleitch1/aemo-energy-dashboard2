# Load test — scenario A, label 
Run at 2026-04-30T11:11:27.455424+00:00.
Base URL: 

## Scenario A — sequential single-request baseline

| Test | N | mean ms | p50 | p95 | p99 | notes |
|---|---:|---:|---:|---:|---:|---|
| meta_freshness (cheap) | 50 |      8 |      8 |     13 |     17 |  |
| gauges_today (medium) | 50 |      1 |      1 |      2 |      2 |  |
| notices (cheap) | 50 |      4 |      4 |      4 |      5 |  |
| predispatch_nsw (medium) | 50 |      9 |      9 |     14 |     22 |  |
| prices_spot_24h (medium) | 50 |    388 |    386 |    425 |    429 |  |
| prices_byfuel_30d (medium) | 50 |     67 |     67 |     69 |     70 |  |
| gen_mix_30d (medium) | 50 |     29 |     25 |     31 |    184 |  |
| batteries_overview (medium) | 50 |     17 |     17 |     18 |     25 |  |
| futures_forward (cheap) | 50 |      4 |      4 |      4 |      4 |  |
| evening_peak_nem (heavy) | 50 |     47 |     46 |     48 |     57 |  |
| station_eraring_90d (heavy) | 50 |     26 |     26 |     27 |     27 |  |
| station_tod_eraring (medium) | 50 |     12 |     12 |     13 |     13 |  |