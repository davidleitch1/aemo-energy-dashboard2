# Load test — scenario A, label 
Run at 2026-04-30T10:54:21.012762+00:00.
Base URL: 

## Scenario A — sequential single-request baseline

| Test | N | mean ms | p50 | p95 | p99 | notes |
|---|---:|---:|---:|---:|---:|---|
| meta_freshness (cheap) | 50 |      8 |      8 |     12 |     17 |  |
| gauges_today (medium) | 50 |   2491 |   2467 |   2615 |   2656 |  |
| notices (cheap) | 50 |      2 |      2 |      2 |      2 |  |
| predispatch_nsw (medium) | 50 |      9 |      9 |      9 |     11 |  |
| prices_spot_24h (medium) | 50 |    389 |    387 |    399 |    448 |  |
| prices_byfuel_30d (medium) | 50 |     67 |     67 |     69 |     70 |  |
| gen_mix_30d (medium) | 50 |     26 |     25 |     26 |     33 |  |
| batteries_overview (medium) | 50 |     17 |     17 |     18 |     18 |  |
| futures_forward (cheap) | 50 |      5 |      5 |      6 |      6 |  |
| evening_peak_nem (heavy) | 50 |     48 |     48 |     49 |     49 |  |
| station_eraring_90d (heavy) | 50 |     25 |     25 |     27 |     27 |  |
| station_tod_eraring (medium) | 50 |     11 |     11 |     12 |     12 |  |