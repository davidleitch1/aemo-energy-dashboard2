"""Shape tests for GET /v1/futures/forward-curve."""
from __future__ import annotations


def test_fwd_returns_200(client, auth_headers):
    resp = client.get('/v1/futures/forward-curve?region=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_fwd_returns_401_without_auth(client):
    resp = client.get('/v1/futures/forward-curve?region=NSW1')
    assert resp.status_code == 401


def test_fwd_top_level_shape(client, auth_headers):
    body = client.get('/v1/futures/forward-curve?region=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('region', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_fwd_snapshots_shape(client, auth_headers):
    body = client.get('/v1/futures/forward-curve?region=NSW1', headers=auth_headers).json()
    snaps = body['data'].get('snapshots', [])
    assert isinstance(snaps, list)
    if snaps:
        s = snaps[0]
        for k in ('label', 'as_of', 'points'):
            assert k in s, f'missing snapshot.{k}'
        if s['points']:
            p = s['points'][0]
            for k in ('quarter', 'year', 'q', 'price'):
                assert k in p, f'missing point.{k}'
            assert 1 <= p['q'] <= 4


def test_fwd_invalid_region(client, auth_headers):
    # Mobile API uses NSW1/QLD1/SA1/VIC1 (no TAS — no ASX contract)
    resp = client.get('/v1/futures/forward-curve?region=TAS1', headers=auth_headers)
    assert resp.status_code == 400


def test_fwd_quarters_sorted(client, auth_headers):
    body = client.get('/v1/futures/forward-curve?region=NSW1', headers=auth_headers).json()
    snaps = body['data'].get('snapshots', [])
    for s in snaps:
        keys = [(p['year'], p['q']) for p in s['points']]
        assert keys == sorted(keys), 'quarters must be ascending'
