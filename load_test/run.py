"""Nem Analyst API load-test harness.

Runs against http://127.0.0.1:8002 (loopback on .71 — Cloudflare excluded).

Three scenarios:
  A — single-endpoint sequential baseline (50 calls each)
  B — concurrency sweep on one heavy endpoint
  C — mixed-workload sessions (N users * 6-screen loop * 5 min)

Emits a markdown table per run into load_test/results/<timestamp>_<label>.md.

Reads the bearer token from ~/.config/aemo-api/tokens.yaml (first key).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import httpx

BASE_URL    = os.environ.get('LOAD_TEST_BASE_URL', 'http://127.0.0.1:8002')
RESULTS_DIR = Path(__file__).parent / 'results'


def _read_token() -> str:
    p = Path.home() / '.config' / 'aemo-api' / 'tokens.yaml'
    line = p.read_text().splitlines()[0]
    return line.split(':')[0].strip()


@dataclass(frozen=True)
class Endpoint:
    label: str
    path:  str
    tier:  str  # cheap, medium, heavy

    @property
    def url(self) -> str:
        return f'{BASE_URL}{self.path}'


# Representative endpoint set (~ what a 6-screen session hits, plus extras).
ENDPOINTS: tuple[Endpoint, ...] = (
    Endpoint('meta_freshness',     '/v1/meta/freshness',                                                'cheap'),
    Endpoint('gauges_today',       '/v1/gauges/today',                                                  'medium'),
    Endpoint('notices',            '/v1/notices?limit=10',                                              'cheap'),
    Endpoint('predispatch_nsw',    '/v1/predispatch?region=NSW1',                                       'medium'),
    Endpoint('prices_spot_24h',    '/v1/prices/spot?regions=NSW1,QLD1,VIC1,SA1,TAS1',                   'medium'),
    Endpoint('prices_byfuel_30d',  '/v1/prices/by-fuel?regions=NSW1,QLD1,VIC1,SA1,TAS1&days=30',                            'medium'),
    Endpoint('gen_mix_30d',        '/v1/generation/mix?regions=NSW1,QLD1,VIC1,SA1,TAS1',                                    'medium'),
    Endpoint('batteries_overview', '/v1/batteries/overview?region=NEM&metric=discharge_revenue',        'medium'),
    Endpoint('futures_forward',    '/v1/futures/forward-curve?region=NSW1',                             'cheap'),
    Endpoint('evening_peak_nem',   '/v1/evening-peak?region=NEM&period_days=30',                        'heavy'),
    Endpoint('station_eraring_90d',
             '/v1/stations/time-series?station=Eraring&period_days=90&frequency=1h',                   'heavy'),
    Endpoint('station_tod_eraring',
             '/v1/stations/tod?station=Eraring&period_days=30',                                         'medium'),
)

# A typical session order (mirrors what a user does opening the app).
SESSION_FLOW: tuple[Endpoint, ...] = (
    next(e for e in ENDPOINTS if e.label == 'gauges_today'),
    next(e for e in ENDPOINTS if e.label == 'prices_spot_24h'),
    next(e for e in ENDPOINTS if e.label == 'gen_mix_30d'),
    next(e for e in ENDPOINTS if e.label == 'batteries_overview'),
    next(e for e in ENDPOINTS if e.label == 'evening_peak_nem'),
    next(e for e in ENDPOINTS if e.label == 'station_eraring_90d'),
)

HEAVY_ENDPOINT = next(e for e in ENDPOINTS if e.label == 'station_eraring_90d')


# ---------- helpers ----------

def _percentiles(values: Sequence[float], qs: Iterable[float]) -> dict:
    if not values:
        return {q: float('nan') for q in qs}
    s = sorted(values)
    out = {}
    n = len(s)
    for q in qs:
        idx = max(0, min(n - 1, int(round(q * (n - 1)))))
        out[q] = s[idx]
    return out


def _row(name: str, values: list[float], extra: str = '') -> str:
    if not values:
        return f'| {name} | — | — | — | — | — | {extra} |'
    p = _percentiles(values, [0.5, 0.95, 0.99])
    mn = min(values); mx = max(values); mean = statistics.fmean(values)
    return (
        f'| {name} | {len(values)} | {mean*1000:6.0f} | {p[0.5]*1000:6.0f} | '
        f'{p[0.95]*1000:6.0f} | {p[0.99]*1000:6.0f} | {extra} |'
    )


HEADER = '| Test | N | mean ms | p50 | p95 | p99 | notes |\n|---|---:|---:|---:|---:|---:|---|'


# ---------- scenario A: sequential baseline ----------

async def scenario_a(client: httpx.AsyncClient) -> str:
    rows = ['## Scenario A — sequential single-request baseline\n', HEADER]
    for ep in ENDPOINTS:
        latencies: list[float] = []
        errors = 0
        # warmup
        try:
            await client.get(ep.url)
        except Exception:
            pass
        for _ in range(50):
            t0 = time.perf_counter()
            try:
                r = await client.get(ep.url)
                if r.status_code != 200:
                    errors += 1
                    continue
            except Exception:
                errors += 1
                continue
            latencies.append(time.perf_counter() - t0)
        rows.append(_row(f'{ep.label} ({ep.tier})', latencies,
                         f'errors {errors}/50' if errors else ''))
    return '\n'.join(rows)


# ---------- scenario B: concurrency sweep ----------

async def _hammer(client: httpx.AsyncClient, ep: Endpoint, concurrency: int,
                  duration_s: int) -> list[float]:
    """Closed-loop: <concurrency> workers each calling in a loop for <duration_s>."""
    latencies: list[float] = []
    end = time.perf_counter() + duration_s

    async def worker():
        while time.perf_counter() < end:
            t0 = time.perf_counter()
            try:
                r = await client.get(ep.url)
                if r.status_code == 200:
                    latencies.append(time.perf_counter() - t0)
            except Exception:
                pass

    await asyncio.gather(*[worker() for _ in range(concurrency)])
    return latencies


async def scenario_b(client: httpx.AsyncClient, ep: Endpoint = HEAVY_ENDPOINT,
                     levels=(1, 5, 10, 20, 50), duration_s: int = 20) -> str:
    rows = [f'## Scenario B — concurrency sweep on  ({ep.tier})\n',
            'Each level closed-loop for {} s. Throughput = completed / duration.\n'.format(duration_s),
            '| concurrency | N | mean ms | p50 | p95 | p99 | req/s |',
            '|---:|---:|---:|---:|---:|---:|---:|']
    for c in levels:
        latencies = await _hammer(client, ep, c, duration_s)
        rps = len(latencies) / duration_s if duration_s else 0
        if not latencies:
            rows.append(f'| {c} | 0 | — | — | — | — | 0 |')
            continue
        p = _percentiles(latencies, [0.5, 0.95, 0.99])
        rows.append(
            f'| {c} | {len(latencies)} | {statistics.fmean(latencies)*1000:6.0f} | '
            f'{p[0.5]*1000:6.0f} | {p[0.95]*1000:6.0f} | {p[0.99]*1000:6.0f} | {rps:5.1f} |'
        )
    return '\n'.join(rows)


# ---------- scenario C: mixed workload ----------

async def _user_session(client: httpx.AsyncClient, end_t: float,
                        think_s: tuple[float, float], latencies: list[float]) -> None:
    while time.perf_counter() < end_t:
        for ep in SESSION_FLOW:
            if time.perf_counter() >= end_t:
                return
            t0 = time.perf_counter()
            try:
                r = await client.get(ep.url)
                if r.status_code == 200:
                    latencies.append(time.perf_counter() - t0)
            except Exception:
                pass
            await asyncio.sleep(random.uniform(*think_s))


async def scenario_c(client: httpx.AsyncClient, users_levels=(10, 20, 30),
                     duration_s: int = 180,
                     think_s: tuple[float, float] = (10.0, 20.0)) -> str:
    rows = [f'## Scenario C — mixed workload, 6-screen sessions, '
            f'{duration_s}s per level, think {think_s[0]}-{think_s[1]}s\n',
            HEADER.replace('| notes |', '| reqs/s |')]
    for n_users in users_levels:
        latencies: list[float] = []
        end_t = time.perf_counter() + duration_s
        await asyncio.gather(*[
            _user_session(client, end_t, think_s, latencies)
            for _ in range(n_users)
        ])
        rps = len(latencies) / duration_s if duration_s else 0
        rows.append(_row(f'{n_users} users', latencies, f'{rps:5.1f} req/s'))
    return '\n'.join(rows)


# ---------- main ----------

async def main(scenario: str, label: str) -> None:
    token = _read_token()
    headers = {'Authorization': f'Bearer {token}'}
    timeout = httpx.Timeout(connect=2.0, read=30.0, write=2.0, pool=2.0)

    out_lines = [
        f'# Load test — scenario {scenario}, label ',
        f'Run at {datetime.now(timezone.utc).isoformat()}.',
        f'Base URL: ',
        '',
    ]

    async with httpx.AsyncClient(headers=headers, timeout=timeout, http2=False) as client:
        if scenario == 'A':
            out_lines.append(await scenario_a(client))
        elif scenario == 'B':
            out_lines.append(await scenario_b(client))
        elif scenario == 'C':
            out_lines.append(await scenario_c(client))
        elif scenario == 'ALL':
            out_lines.append(await scenario_a(client))
            out_lines.append('')
            out_lines.append(await scenario_b(client))
            out_lines.append('')
            out_lines.append(await scenario_c(client))
        else:
            raise SystemExit(f'unknown scenario {scenario}')

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = RESULTS_DIR / f'{ts}_{label}_scenario{scenario}.md'
    out_path.write_text('\n'.join(out_lines))
    print(f'\n--- results written to {out_path}\n')
    print('\n'.join(out_lines))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--scenario', choices=['A', 'B', 'C', 'ALL'], default='A')
    p.add_argument('--label',    default='baseline')
    args = p.parse_args()
    asyncio.run(main(args.scenario, args.label))
