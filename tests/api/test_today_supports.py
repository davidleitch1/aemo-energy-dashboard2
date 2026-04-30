"""Shape + validation tests for /v1/predispatch and /v1/notices.
Test fixture has predispatch table; notices comes back empty (no network)."""
from __future__ import annotations


# ---------- /v1/predispatch ----------

def test_predispatch_requires_auth(client):
    assert client.get('/v1/predispatch').status_code == 401


def test_predispatch_default_returns_200(client, auth_headers):
    r = client.get('/v1/predispatch', headers=auth_headers)
    assert r.status_code == 200, r.text


def test_predispatch_top_level_shape(client, auth_headers):
    body = client.get('/v1/predispatch', headers=auth_headers).json()
    assert 'data' in body and isinstance(body['data'], list)
    for k in ('region', 'run_time', 'horizon_slots', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_predispatch_invalid_region_returns_400(client, auth_headers):
    r = client.get('/v1/predispatch?region=ZZ1', headers=auth_headers)
    assert r.status_code == 400


def test_predispatch_each_region_accepted(client, auth_headers):
    for region in ('NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'):
        r = client.get(f'/v1/predispatch?region={region}', headers=auth_headers)
        assert r.status_code == 200, f'{region}: {r.text}'
        assert r.json()['meta']['region'] == region


def test_predispatch_row_shape_when_present(client, auth_headers):
    body = client.get('/v1/predispatch?region=NSW1', headers=auth_headers).json()
    if body['data']:
        row = body['data'][0]
        for k in ('settlementdate', 'price_forecast', 'demand_forecast',
                  'solar_forecast', 'wind_forecast'):
            assert k in row, f'missing row.{k}'


# ---------- /v1/notices ----------

def test_notices_requires_auth(client):
    assert client.get('/v1/notices').status_code == 401


def test_notices_default_returns_200(client, auth_headers):
    r = client.get('/v1/notices', headers=auth_headers)
    assert r.status_code == 200


def test_notices_top_level_shape(client, auth_headers):
    body = client.get('/v1/notices', headers=auth_headers).json()
    assert 'data' in body and isinstance(body['data'], list)
    for k in ('count', 'cached', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_notices_limit_param_validated(client, auth_headers):
    assert client.get('/v1/notices?limit=0',  headers=auth_headers).status_code == 422
    assert client.get('/v1/notices?limit=21', headers=auth_headers).status_code == 422


def test_notices_row_shape_when_present(client, auth_headers):
    body = client.get('/v1/notices', headers=auth_headers).json()
    if body['data']:
        row = body['data'][0]
        for k in ('id', 'type', 'type_description', 'creation_date',
                  'reason_short', 'reason_full'):
            assert k in row, f'missing row.{k}'
