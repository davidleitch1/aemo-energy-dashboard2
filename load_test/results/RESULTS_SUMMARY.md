# Load test results — 30 April 2026

## TL;DR

API on `.71` (M2 mini, 8 cores) **meets the 20-user target with headroom**.
After three software levers, p95 latency under a 20-user mixed workload
is **761 ms** (target ≤ 1.5 s).  No need to migrate to the spare M4 mini.

## Levers applied (in order)

1. **uvicorn `--workers 4`** (was 2) — process-level fan-out.
2. **`async def` → `def` on slow handlers** (gauges, prices, evening-peak,
   today). FastAPI now runs them in its 40-thread executor, so a 388 ms
   prices_spot or 2.5 s gauges cold-cache no longer blocks the worker's
   event loop while other endpoints wait.
3. **30 → 60 s TTL cache on `/v1/gauges/today`** (the all-time-record
   queries do full table scans on demand30 + rooftop30; ~2.5 s each).
   Plus a startup pre-warm so workers don't cold-miss on first request.

## Scenario C (mixed workload, 6-screen sessions, 180 s per level)

| Users | Baseline p95 | After p95 | Change |
|---:|---:|---:|---:|
| 10 | 24,760 ms | **477 ms** | 52× |
| 20 | 28,233 ms | **761 ms** | 37× |
| 30 | 28,144 ms | **3,631 ms** | 8× |

## Scenario B (concurrency sweep on heavy endpoint, 20 s each)

| Concurrency | Baseline req/s | After req/s |
|---:|---:|---:|
| 10 |  72 | 104 |
| 20 |  73 | 108 |
| 50 |  74 |  71 |

## Single-request latencies (Scenario A, after levers)

| Endpoint | mean | p95 |
|---|---:|---:|
| meta_freshness | 8 ms | 11 ms |
| gauges_today (cached) | 2 ms | 2 ms |
| notices (cached) | 4 ms | 6 ms |
| predispatch | 10 ms | 17 ms |
| prices_spot_24h | 383 ms | 393 ms |
| prices_byfuel_30d | 66 ms | 68 ms |
| gen_mix_30d | 26 ms | 26 ms |
| batteries_overview | 17 ms | 18 ms |
| futures_forward | 5 ms | 6 ms |
| evening_peak | 48 ms | 49 ms |
| station_eraring_90d | 27 ms | 27 ms |
| station_tod | 12 ms | 12 ms |

## What we'll watch

- **30+ users**: p95 starts climbing past 1.5 s. If the user count grows
  past 25 it's time to apply lever 4 (M4 mini, either as replacement or
  paired with .71).
- **Gauges cold path** (~2.5 s) still hits once per worker per 60 s. Could
  be reduced by background refresh (a periodic task that re-runs the
  computation off-loop) or by splitting hot/cold parts (the all-time
  records change daily, not 5-minutely).
