"""Shape + parameter tests for /v1/generation/comparison.

The fixture only contains late-April-2026 data, so the prior-year window
will return empty rows. Tests assert envelope shape + param validation,
not numeric values (those are verified live on .71).
"""
from __future__ import annotations


def test_comparison_requires_auth(client):
    assert client.get('/v1/generation/comparison').status_code == 401


def test_comparison_default_returns_200(client, auth_headers):
    r = client.get('/v1/generation/comparison', headers=auth_headers)
    assert r.status_code == 200, r.text


def test_comparison_top_level_shape(client, auth_headers):
    body = client.get('/v1/generation/comparison', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    assert 'rows' in body['data']
    for k in ('region', 'period', 'current_window', 'prior_window',
              'fuel_order', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_comparison_row_shape(client, auth_headers):
    body = client.get('/v1/generation/comparison', headers=auth_headers).json()
    assert body['data']['rows'], 'expected at least the total row'
    for row in body['data']['rows']:
        for k in ('fuel', 'group', 'is_spread',
                  'current_twh', 'prior_twh', 'delta_twh',
                  'current_vwap', 'prior_vwap', 'delta_vwap'):
            assert k in row, f'missing row.{k}'


def test_comparison_total_row_present(client, auth_headers):
    body = client.get('/v1/generation/comparison', headers=auth_headers).json()
    rows = body['data']['rows']
    assert rows[-1]['group'] == 'TOTAL', 'last row must be the total'
    assert rows[-1]['fuel'] == 'NEM total'


def test_comparison_total_label_per_region(client, auth_headers):
    body = client.get('/v1/generation/comparison?region=NSW1',
                      headers=auth_headers).json()
    assert body['data']['rows'][-1]['fuel'] == 'NSW1 total'


def test_comparison_battery_row_uses_spread(client, auth_headers):
    body = client.get('/v1/generation/comparison', headers=auth_headers).json()
    battery_rows = [r for r in body['data']['rows'] if r['group'] == 'Battery']
    if battery_rows:
        assert battery_rows[0]['is_spread'] is True
    other_rows = [r for r in body['data']['rows']
                  if r['group'] not in ('Battery', 'TOTAL')]
    for r in other_rows:
        assert r['is_spread'] is False, f"{r['fuel']} marked is_spread"


def test_comparison_invalid_region_returns_400(client, auth_headers):
    r = client.get('/v1/generation/comparison?region=ZZ1', headers=auth_headers)
    assert r.status_code == 400


def test_comparison_invalid_period_returns_400(client, auth_headers):
    r = client.get('/v1/generation/comparison?period=42d', headers=auth_headers)
    assert r.status_code == 400


def test_comparison_accepts_each_period(client, auth_headers):
    for period in ('7d', '30d', '90d', 'ytd', '1y'):
        r = client.get(f'/v1/generation/comparison?period={period}',
                       headers=auth_headers)
        assert r.status_code == 200, f'{period}: {r.text}'
        assert r.json()['meta']['period'] == period


def test_comparison_accepts_each_region(client, auth_headers):
    for region in ('NEM', 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'):
        r = client.get(f'/v1/generation/comparison?region={region}',
                       headers=auth_headers)
        assert r.status_code == 200, f'{region}: {r.text}'
        assert r.json()['meta']['region'] == region


def test_comparison_window_dates_iso(client, auth_headers):
    body = client.get('/v1/generation/comparison?period=ytd',
                      headers=auth_headers).json()
    cur = body['meta']['current_window']
    if cur is None:
        return  # empty fixture
    assert 'start' in cur and 'end' in cur and 'days' in cur
    # ISO 8601 starts with YYYY-MM-DD
    assert cur['start'][:4].isdigit()
    assert cur['end'][:4].isdigit()
    assert isinstance(cur['days'], (int, float))


def test_comparison_prior_window_one_year_back(client, auth_headers):
    body = client.get('/v1/generation/comparison?period=ytd',
                      headers=auth_headers).json()
    cur = body['meta']['current_window']
    prv = body['meta']['prior_window']
    if cur is None or prv is None:
        return
    cur_year = int(cur['end'][:4])
    prv_year = int(prv['end'][:4])
    assert prv_year == cur_year - 1, f'prior year {prv_year} not one back from {cur_year}'


def test_comparison_fuel_order_listed(client, auth_headers):
    body = client.get('/v1/generation/comparison', headers=auth_headers).json()
    assert body['meta']['fuel_order'][:8] == [
        'Coal', 'Wind', 'Rooftop solar', 'Utility solar', 'Hydro',
        'Gas', 'Battery', 'Biomass + other',
    ]
    assert body['meta']['fuel_order'][-1] == 'NEM total'
