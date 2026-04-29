"""Shape tests for GET /v1/generation/time-of-day.

TOD aggregates across the requested regions per (hour, fuel) — one
stack per call, mirroring /generation/mix. Single physical region
exposes Battery Charging and Transmission Imports/Exports; multi-region
(NEM) nets transmission to zero and omits those buckets.
"""
from __future__ import annotations


def test_gentod_returns_200(client, auth_headers):
    resp = client.get('/v1/generation/time-of-day?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_gentod_returns_401_without_auth(client):
    resp = client.get('/v1/generation/time-of-day?regions=NSW1')
    assert resp.status_code == 401


def test_gentod_top_level_shape(client, auth_headers):
    body = client.get('/v1/generation/time-of-day?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'lookback_days', 'fuels', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_gentod_point_shape(client, auth_headers):
    body = client.get('/v1/generation/time-of-day?regions=NSW1', headers=auth_headers).json()
    assert body['data'], 'expected non-empty TOD data on fixture'
    p = body['data'][0]
    for k in ('hour', 'fuel', 'mw'):
        assert k in p, f'missing point.{k}'
    assert isinstance(p['hour'], int)
    assert 0 <= p['hour'] <= 23
    assert isinstance(p['mw'], (int, float))


def test_gentod_at_most_one_row_per_hour_fuel(client, auth_headers):
    body = client.get(
        '/v1/generation/time-of-day?regions=NSW1,QLD1,VIC1,SA1,TAS1',
        headers=auth_headers,
    ).json()
    seen = set()
    for p in body['data']:
        key = (p['hour'], p['fuel'])
        assert key not in seen, f'duplicate row for {key}'
        seen.add(key)


def test_gentod_no_biomass_no_unmerged_gas(client, auth_headers):
    body = client.get(
        '/v1/generation/time-of-day?regions=NSW1,QLD1,VIC1,SA1,TAS1',
        headers=auth_headers,
    ).json()
    fuels = {p['fuel'] for p in body['data']}
    assert 'Biomass' not in fuels
    assert 'CCGT' not in fuels
    assert 'OCGT' not in fuels


def test_gentod_invalid_region(client, auth_headers):
    resp = client.get('/v1/generation/time-of-day?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400


def test_gentod_days_clamped_low(client, auth_headers):
    resp = client.get('/v1/generation/time-of-day?regions=NSW1&days=0', headers=auth_headers)
    assert resp.status_code == 422


def test_gentod_days_clamped_high(client, auth_headers):
    resp = client.get('/v1/generation/time-of-day?regions=NSW1&days=400', headers=auth_headers)
    assert resp.status_code == 422


def test_gentod_hour_values_in_range(client, auth_headers):
    body = client.get(
        '/v1/generation/time-of-day?regions=NSW1,QLD1,VIC1,SA1,TAS1',
        headers=auth_headers,
    ).json()
    assert body['data'], 'expected non-empty TOD data on fixture'
    for p in body['data']:
        assert 0 <= p['hour'] <= 23, f'hour out of range: {p["hour"]}'


def test_gentod_lookback_days_in_meta(client, auth_headers):
    body = client.get(
        '/v1/generation/time-of-day?regions=NSW1&days=30',
        headers=auth_headers,
    ).json()
    assert body['meta']['lookback_days'] == 30


def test_gentod_fuels_meta_matches_data(client, auth_headers):
    body = client.get(
        '/v1/generation/time-of-day?regions=NSW1,QLD1,VIC1,SA1,TAS1',
        headers=auth_headers,
    ).json()
    seen = {p['fuel'] for p in body['data']}
    assert set(body['meta']['fuels']) == seen
