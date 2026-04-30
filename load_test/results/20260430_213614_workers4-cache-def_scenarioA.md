# Load test — scenario A, label 
Run at 2026-04-30T11:35:39.015531+00:00.
Base URL: 

## Scenario A — sequential single-request baseline

| Test | N | mean ms | p50 | p95 | p99 | notes |
|---|---:|---:|---:|---:|---:|---|
| meta_freshness (cheap) | 50 |      8 |      8 |     11 |     15 |  |
| gauges_today (medium) | 50 |      2 |      2 |      2 |      3 |  |
| notices (cheap) | 50 |      4 |      4 |      6 |      7 |  |
| predispatch_nsw (medium) | 50 |     10 |      9 |     17 |     24 |  |
| prices_spot_24h (medium) | 50 |    383 |    382 |    393 |    394 |  |
| prices_byfuel_30d (medium) | 50 |     66 |     66 |     68 |     69 |  |
| gen_mix_30d (medium) | 50 |     26 |     26 |     26 |     26 |  |
| batteries_overview (medium) | 50 |     17 |     17 |     18 |     18 |  |
| futures_forward (cheap) | 50 |      5 |      5 |      6 |      6 |  |
| evening_peak_nem (heavy) | 50 |     48 |     48 |     49 |     49 |  |
| station_eraring_90d (heavy) | 50 |     27 |     27 |     27 |     28 |  |
| station_tod_eraring (medium) | 50 |     12 |     12 |     12 |     13 |  |