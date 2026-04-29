"""Shape tests for GET /v1/prices/stats."""
from __future__ import annotations


def test_stats_returns_200(client, auth_headers):
    resp = client.get('/v1/prices/stats?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_stats_returns_401_without_auth(client):
    resp = client.get('/v1/prices/stats?regions=NSW1')
    assert resp.status_code == 401


def test_stats_top_level_shape(client, auth_headers):
    body = client.get('/v1/prices/stats?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'lookback_days', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_stats_row_shape(client, auth_headers):
    body = client.get('/v1/prices/stats?regions=NSW1', headers=auth_headers).json()
    if body['data']:
        r = body['data'][0]
        for k in ('region', 'mean', 'median', 'p10', 'p90', 'min', 'max', 'sample_count'):
            assert k in r, f'missing {k}'
        assert isinstance(r['sample_count'], int)
        for k in ('mean', 'median', 'p10', 'p90', 'min', 'max'):
            assert isinstance(r[k], (int, float))


def test_stats_quantile_ordering(client, auth_headers):
    body = client.get('/v1/prices/stats?regions=NSW1', headers=auth_headers).json()
    for r in body['data']:
        assert r['min'] <= r['p10'], f"min > p10 for {r['region']}"
        assert r['p10'] <= r['median'], f"p10 > median for {r['region']}"
        assert r['median'] <= r['p90'], f"median > p90 for {r['region']}"
        assert r['p90'] <= r['max'], f"p90 > max for {r['region']}"


def test_stats_multi_region(client, auth_headers):
    body = client.get('/v1/prices/stats?regions=NSW1,QLD1,VIC1', headers=auth_headers).json()
    assert body['meta']['regions'] == ['NSW1', 'QLD1', 'VIC1']
    seen = {r['region'] for r in body['data']}
    assert seen.issubset({'NSW1', 'QLD1', 'VIC1'})


def test_stats_invalid_region(client, auth_headers):
    resp = client.get('/v1/prices/stats?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400
