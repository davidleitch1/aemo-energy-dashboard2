"""Shape tests for GET /v1/prices/time-of-day."""
from __future__ import annotations


def test_tod_returns_200(client, auth_headers):
    resp = client.get('/v1/prices/time-of-day?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_tod_returns_401_without_auth(client):
    resp = client.get('/v1/prices/time-of-day?regions=NSW1')
    assert resp.status_code == 401


def test_tod_top_level_shape(client, auth_headers):
    body = client.get('/v1/prices/time-of-day?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    meta = body['meta']
    for k in ('regions', 'lookback_days', 'as_of'):
        assert k in meta, f'missing meta.{k}'


def test_tod_point_shape(client, auth_headers):
    body = client.get('/v1/prices/time-of-day?regions=NSW1', headers=auth_headers).json()
    if body['data']:
        p = body['data'][0]
        for k in ('region', 'hour', 'avg_price'):
            assert k in p, f'missing point.{k}'
        assert isinstance(p['hour'], int)
        assert 0 <= p['hour'] <= 23
        assert isinstance(p['avg_price'], (int, float))


def test_tod_multi_region(client, auth_headers):
    body = client.get('/v1/prices/time-of-day?regions=NSW1,QLD1,VIC1', headers=auth_headers).json()
    assert body['meta']['regions'] == ['NSW1', 'QLD1', 'VIC1']
    seen = {p['region'] for p in body['data']}
    assert seen.issubset({'NSW1', 'QLD1', 'VIC1'})


def test_tod_at_most_one_row_per_region_hour(client, auth_headers):
    body = client.get('/v1/prices/time-of-day?regions=NSW1,QLD1', headers=auth_headers).json()
    seen = set()
    for p in body['data']:
        key = (p['region'], p['hour'])
        assert key not in seen, f'duplicate row for {key}'
        seen.add(key)


def test_tod_invalid_region(client, auth_headers):
    resp = client.get('/v1/prices/time-of-day?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400
