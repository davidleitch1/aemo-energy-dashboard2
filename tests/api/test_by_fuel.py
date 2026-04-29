"""Shape tests for GET /v1/prices/by-fuel."""
from __future__ import annotations


def test_by_fuel_returns_200(client, auth_headers):
    resp = client.get('/v1/prices/by-fuel?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_by_fuel_returns_401_without_auth(client):
    resp = client.get('/v1/prices/by-fuel?regions=NSW1')
    assert resp.status_code == 401


def test_by_fuel_top_level_shape(client, auth_headers):
    body = client.get('/v1/prices/by-fuel?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'lookback_days', 'total_volume_mwh', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_by_fuel_row_shape(client, auth_headers):
    body = client.get('/v1/prices/by-fuel?regions=NSW1', headers=auth_headers).json()
    if body['data']:
        row = body['data'][0]
        for k in ('fuel', 'volume_mwh', 'share_pct', 'vwap', 'avg_mw'):
            assert k in row, f'missing fuel row.{k}'
        for k in ('volume_mwh', 'share_pct', 'vwap', 'avg_mw'):
            assert isinstance(row[k], (int, float)), f'{k} not numeric'


def test_by_fuel_sorted_by_volume_desc(client, auth_headers):
    body = client.get('/v1/prices/by-fuel?regions=NSW1,QLD1', headers=auth_headers).json()
    vols = [r['volume_mwh'] for r in body['data']]
    assert vols == sorted(vols, reverse=True), 'rows must be sorted by volume_mwh desc'


def test_by_fuel_shares_sum_to_100_within_tolerance(client, auth_headers):
    body = client.get('/v1/prices/by-fuel?regions=NSW1', headers=auth_headers).json()
    if body['data']:
        total_share = sum(r['share_pct'] for r in body['data'])
        assert abs(total_share - 100.0) < 0.5, f'share_pct sum {total_share} not ~100'


def test_by_fuel_invalid_region(client, auth_headers):
    resp = client.get('/v1/prices/by-fuel?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400
