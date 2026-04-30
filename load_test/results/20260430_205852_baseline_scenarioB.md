# Load test — scenario B, label 
Run at 2026-04-30T10:57:11.310892+00:00.
Base URL: 

## Scenario B — concurrency sweep on  (heavy)

Each level closed-loop for 20 s. Throughput = completed / duration.

| concurrency | N | mean ms | p50 | p95 | p99 | req/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 754 |     27 |     26 |     27 |     28 |  37.7 |
| 5 | 1329 |     75 |    107 |    125 |    136 |  66.5 |
| 10 | 1436 |    139 |    153 |    170 |    177 |  71.8 |
| 20 | 1454 |    277 |    280 |    290 |    310 |  72.7 |
| 50 | 1478 |    687 |    701 |    718 |    743 |  73.9 |