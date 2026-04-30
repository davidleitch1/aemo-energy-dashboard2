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


# ---------- /v1/futures/expectations ----------

def test_expectations_returns_200(client, auth_headers):
    r = client.get('/v1/futures/expectations?region=NSW1', headers=auth_headers)
    assert r.status_code == 200


def test_expectations_returns_401_without_auth(client):
    assert client.get('/v1/futures/expectations?region=NSW1').status_code == 401


def test_expectations_shape(client, auth_headers):
    body = client.get('/v1/futures/expectations?region=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    assert 'cal1' in body['data'] and 'cal2' in body['data']
    for k in ('region', 'cal1_year', 'cal2_year', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_expectations_invalid_region_returns_400(client, auth_headers):
    r = client.get('/v1/futures/expectations?region=TAS1', headers=auth_headers)
    assert r.status_code == 400


def test_expectations_point_shape(client, auth_headers):
    data = client.get('/v1/futures/expectations?region=NSW1', headers=auth_headers).json()['data']
    if data['cal1']:
        p = data['cal1'][0]
        assert 'date' in p and 'price' in p


# ---------- /v1/futures/contracts ----------

def test_contracts_returns_200(client, auth_headers):
    r = client.get('/v1/futures/contracts', headers=auth_headers)
    assert r.status_code == 200


def test_contracts_shape(client, auth_headers):
    body = client.get('/v1/futures/contracts', headers=auth_headers).json()
    assert 'data' in body and isinstance(body['data'], list)
    if body['data']:
        item = body['data'][0]
        for k in ('year', 'quarter', 'label'):
            assert k in item


def test_contracts_sorted_ascending(client, auth_headers):
    items = client.get('/v1/futures/contracts', headers=auth_headers).json()['data']
    keys = [(i['year'], i['quarter']) for i in items]
    assert keys == sorted(keys)


# ---------- /v1/futures/contract ----------

def test_contract_returns_200(client, auth_headers):
    r = client.get('/v1/futures/contract?year=2026&quarter=1', headers=auth_headers)
    assert r.status_code == 200


def test_contract_invalid_quarter_returns_422(client, auth_headers):
    r = client.get('/v1/futures/contract?year=2026&quarter=5', headers=auth_headers)
    assert r.status_code == 422


def test_contract_meta_shape(client, auth_headers):
    body = client.get('/v1/futures/contract?year=2026&quarter=1', headers=auth_headers).json()
    for k in ('year', 'quarter', 'label', 'regions', 'as_of'):
        assert k in body['meta']
    assert body['meta']['year'] == 2026
    assert body['meta']['quarter'] == 1


def test_contract_data_keyed_by_full_region_code(client, auth_headers):
    body = client.get('/v1/futures/contract?year=2026&quarter=1', headers=auth_headers).json()
    if body['data']:
        for region in body['data'].keys():
            assert region in {'NSW1', 'QLD1', 'SA1', 'VIC1'}, f'unexpected region key: {region}'
