"""Shape + parameter tests for GET /v1/gas/* endpoints.

Test fixture has no sttm_expost table, so behaviour verification happens
via live smoke testing on .71. These tests cover the request/response
contract and parameter validation.
"""
from __future__ import annotations


# ---------- /v1/gas/prices ----------

def test_prices_returns_200(client, auth_headers):
    resp = client.get('/v1/gas/prices?hub=AVG', headers=auth_headers)
    assert resp.status_code == 200


def test_prices_returns_401_without_auth(client):
    resp = client.get('/v1/gas/prices?hub=AVG')
    assert resp.status_code == 401


def test_prices_top_level_shape(client, auth_headers):
    body = client.get('/v1/gas/prices?hub=AVG', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('hub', 'years', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'
    assert body['meta']['hub'] == 'AVG'
    assert isinstance(body['meta']['years'], list)


def test_prices_point_shape(client, auth_headers):
    body = client.get('/v1/gas/prices?hub=AVG', headers=auth_headers).json()
    if body['data']:
        p = body['data'][0]
        for k in ('year', 'dayofyear', 'price'):
            assert k in p, f'missing {k}'
        assert isinstance(p['year'], int)
        assert isinstance(p['dayofyear'], int)
        assert 1 <= p['dayofyear'] <= 366
        assert isinstance(p['price'], (int, float))


def test_prices_accepts_each_hub(client, auth_headers):
    for hub in ('SYD', 'BRI', 'ADL', 'AVG'):
        resp = client.get(f'/v1/gas/prices?hub={hub}', headers=auth_headers)
        assert resp.status_code == 200, f'{hub}: {resp.status_code} {resp.text}'
        body = resp.json()
        assert body['meta']['hub'] == hub


def test_prices_invalid_hub(client, auth_headers):
    resp = client.get('/v1/gas/prices?hub=ZZZ', headers=auth_headers)
    assert resp.status_code == 422


# ---------- /v1/gas/demand ----------

def test_demand_returns_200(client, auth_headers):
    resp = client.get('/v1/gas/demand?hub=ALL', headers=auth_headers)
    assert resp.status_code == 200


def test_demand_returns_401_without_auth(client):
    resp = client.get('/v1/gas/demand?hub=ALL')
    assert resp.status_code == 401


def test_demand_top_level_shape(client, auth_headers):
    body = client.get('/v1/gas/demand?hub=ALL', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('hub', 'years', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'
    assert isinstance(body['meta']['years'], list)


def test_demand_point_shape(client, auth_headers):
    body = client.get('/v1/gas/demand?hub=ALL', headers=auth_headers).json()
    if body['data']:
        p = body['data'][0]
        for k in ('year', 'dayofyear', 'tj'):
            assert k in p, f'missing {k}'
        assert isinstance(p['year'], int)
        assert isinstance(p['dayofyear'], int)
        assert 1 <= p['dayofyear'] <= 366
        assert isinstance(p['tj'], (int, float))


def test_demand_accepts_each_hub(client, auth_headers):
    for hub in ('ALL', 'SYD', 'BRI', 'ADL'):
        resp = client.get(f'/v1/gas/demand?hub={hub}', headers=auth_headers)
        assert resp.status_code == 200, f'{hub}: {resp.status_code} {resp.text}'
        body = resp.json()
        assert body['meta']['hub'] == hub


def test_demand_invalid_hub(client, auth_headers):
    resp = client.get('/v1/gas/demand?hub=AVG', headers=auth_headers)
    assert resp.status_code == 422  # AVG only valid for prices, not demand
