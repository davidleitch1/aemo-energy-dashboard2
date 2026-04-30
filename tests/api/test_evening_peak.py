"""Shape + parameter tests for /v1/evening-peak.

Fixture has scada30 only for two batteries during a 24h window in late-April,
so no Coal/Gas/Wind/Solar generation rows. The endpoint should still return
a well-formed envelope with empty time-series + Battery/Net-Imports figures
where they apply, and behaviour against real data is verified live on .71.
"""
from __future__ import annotations


def test_evening_peak_requires_auth(client):
    assert client.get('/v1/evening-peak').status_code == 401


def test_evening_peak_default_returns_200(client, auth_headers):
    r = client.get('/v1/evening-peak', headers=auth_headers)
    assert r.status_code == 200, r.text


def test_evening_peak_top_level_shape(client, auth_headers):
    body = client.get('/v1/evening-peak', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('current', 'pcp', 'fuel_changes'):
        assert k in body['data'], f'missing data.{k}'
    for k in ('region', 'period_days', 'current_window', 'pcp_window', 'fuel_order', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_evening_peak_current_subobject_shape(client, auth_headers):
    cur = client.get('/v1/evening-peak', headers=auth_headers).json()['data']['current']
    for k in ('fuel_mix', 'price', 'totals'):
        assert k in cur, f'missing current.{k}'


def test_evening_peak_fuel_changes_match_waterfall_fuels(client, auth_headers):
    body = client.get('/v1/evening-peak', headers=auth_headers).json()
    fuels = [f['fuel'] for f in body['data']['fuel_changes']]
    assert fuels == ['Net Imports', 'Coal', 'Gas', 'Hydro', 'Wind', 'Solar', 'Battery']
    for row in body['data']['fuel_changes']:
        for k in ('fuel', 'current_mw', 'pcp_mw', 'delta_mw'):
            assert k in row, f'missing fuel_changes[].{k}'


def test_evening_peak_invalid_region_returns_400(client, auth_headers):
    r = client.get('/v1/evening-peak?region=ZZ1', headers=auth_headers)
    assert r.status_code == 400


def test_evening_peak_period_days_too_small_returns_422(client, auth_headers):
    r = client.get('/v1/evening-peak?period_days=3', headers=auth_headers)
    assert r.status_code == 422


def test_evening_peak_period_days_too_large_returns_422(client, auth_headers):
    r = client.get('/v1/evening-peak?period_days=400', headers=auth_headers)
    assert r.status_code == 422


def test_evening_peak_period_days_echoed(client, auth_headers):
    body = client.get('/v1/evening-peak?period_days=14', headers=auth_headers).json()
    assert body['meta']['period_days'] == 14


def test_evening_peak_accepts_each_region(client, auth_headers):
    for region in ('NEM', 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'):
        r = client.get(f'/v1/evening-peak?region={region}', headers=auth_headers)
        assert r.status_code == 200, f'{region}: {r.text}'
        assert r.json()['meta']['region'] == region


def test_evening_peak_fuel_order_listed(client, auth_headers):
    body = client.get('/v1/evening-peak', headers=auth_headers).json()
    assert body['meta']['fuel_order'] == [
        'Net Imports', 'Coal', 'Gas', 'Hydro', 'Wind', 'Solar', 'Rooftop Solar', 'Battery', 'Other'
    ]
