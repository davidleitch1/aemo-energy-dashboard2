# Load test — scenario B, label 
Run at 2026-04-30T11:12:19.841141+00:00.
Base URL: 

## Scenario B — concurrency sweep on  (heavy)

Each level closed-loop for 20 s. Throughput = completed / duration.

| concurrency | N | mean ms | p50 | p95 | p99 | req/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 571 |     35 |     26 |     31 |    139 |  28.6 |
| 5 | 1329 |     75 |    105 |    125 |    146 |  66.5 |
| 10 | 2084 |     96 |     99 |    157 |    164 | 104.2 |
| 20 | 2160 |    186 |    184 |    228 |    258 | 108.0 |
| 50 | 1418 |    713 |    476 |   1530 |   5737 |  70.9 |