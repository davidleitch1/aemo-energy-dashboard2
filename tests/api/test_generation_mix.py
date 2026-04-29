"""Shape tests for GET /v1/generation/mix."""
from __future__ import annotations


def test_mix_returns_200(client, auth_headers):
    resp = client.get('/v1/generation/mix?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_mix_returns_401_without_auth(client):
    resp = client.get('/v1/generation/mix?regions=NSW1')
    assert resp.status_code == 401


def test_mix_top_level_shape(client, auth_headers):
    body = client.get('/v1/generation/mix?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'resolution', 'fuels', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_mix_point_shape(client, auth_headers):
    body = client.get('/v1/generation/mix?regions=NSW1', headers=auth_headers).json()
    if body['data']:
        p = body['data'][0]
        for k in ('timestamp', 'fuel', 'mw'):
            assert k in p, f'missing {k}'
        assert isinstance(p['mw'], (int, float))


def test_mix_no_biomass(client, auth_headers):
    body = client.get('/v1/generation/mix?regions=NSW1,QLD1,VIC1,SA1,TAS1', headers=auth_headers).json()
    fuels = {p['fuel'] for p in body['data']}
    assert 'Biomass' not in fuels
    # CCGT/OCGT/Gas other should be merged into 'Gas'
    assert 'CCGT' not in fuels
    assert 'OCGT' not in fuels


def test_mix_invalid_region(client, auth_headers):
    resp = client.get('/v1/generation/mix?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400


def test_mix_resolution_auto_picks_buckets(client, auth_headers):
    # 1y window should pick a coarser bucket than auto
    resp = client.get(
        '/v1/generation/mix?regions=NSW1&from=2025-04-01T00:00:00Z&to=2026-04-01T00:00:00Z',
        headers=auth_headers,
    )
    body = resp.json()
    # Resolution should not be raw 5min for a 365-day window
    assert body['meta']['resolution'] in ('1h', '6h', '1d', '30min'), body['meta']['resolution']
