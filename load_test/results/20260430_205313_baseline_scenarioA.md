# Load test — scenario A, label 
Run at 2026-04-30T10:50:34.320421+00:00.
Base URL: 

## Scenario A — sequential single-request baseline

| Test | N | mean ms | p50 | p95 | p99 | notes |
|---|---:|---:|---:|---:|---:|---|
| meta_freshness (cheap) | 50 |      8 |      8 |     12 |     20 |  |
| gauges_today (medium) | 50 |   2512 |   2482 |   2656 |   2780 |  |
| notices (cheap) | 50 |      4 |      4 |      5 |      5 |  |
| predispatch_nsw (medium) | 50 |      9 |      9 |     15 |     23 |  |
| prices_spot_24h (medium) | 50 |    393 |    389 |    427 |    438 |  |
| prices_byfuel_30d (medium) | — | — | — | — | — | errors 50/50 |
| gen_mix_30d (medium) | — | — | — | — | — | errors 50/50 |
| batteries_overview (medium) | 50 |     17 |     17 |     19 |     20 |  |
| futures_forward (cheap) | 50 |      5 |      5 |      6 |      6 |  |
| evening_peak_nem (heavy) | 50 |     48 |     48 |     48 |     49 |  |
| station_eraring_90d (heavy) | 50 |     26 |     26 |     27 |     27 |  |
| station_tod_eraring (medium) | 50 |     12 |     12 |     13 |     13 |  |