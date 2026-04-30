"""Shape + value tests for GET /v1/batteries/overview.

Fixture: scada30 has TIB1 (SA1) and HPRG1 (VIC1) over the same 24h
window as prices30. Energy values are deterministic from the fixture
charge/discharge profile; revenue/cost values depend on fixture prices
so we assert ordering rather than exact dollars.
"""
from __future__ import annotations

WINDOW_FROM = '2026-04-28T13:50:00'
WINDOW_TO   = '2026-04-29T13:30:00'

# Fixture profile per extend_for_batteries.py:
#   TIB1  discharge: 6 slots x 200 MW = 1200 MW.30min => 600 MWh
#   TIB1  charge:    6 slots x -100 MW = -600 MW.30min => 300 MWh charged
#   HPRG1 discharge: 6 slots x 60 MW => 180 MWh
#   HPRG1 charge:    6 slots x -45 MW => 135 MWh
TIB1_DISCHARGE_MWH = 600.0
TIB1_CHARGE_MWH    = 300.0
HPRG1_DISCHARGE_MWH = 180.0
HPRG1_CHARGE_MWH    = 135.0


def _get(client, headers, **kw):
    qs = '&'.join(f'{k}={v}' for k, v in kw.items())
    return client.get(f'/v1/batteries/overview?{qs}', headers=headers)


def test_overview_requires_auth(client):
    r = client.get('/v1/batteries/overview')
    assert r.status_code == 401


def test_overview_default_returns_200_and_shape(client, auth_headers):
    r = _get(client, auth_headers, **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    assert r.status_code == 200, r.text
    body = r.json()
    assert 'data' in body and isinstance(body['data'], list)
    assert 'meta' in body
    for k in ('metric', 'metric_label', 'units', 'region', 'limit', 'from', 'to', 'as_of', 'total_count'):
        assert k in body['meta'], f'missing meta.{k}'


def test_overview_row_shape(client, auth_headers):
    r = _get(client, auth_headers, metric='discharge_energy', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    rows = r.json()['data']
    assert rows, 'expected at least one battery row'
    row = rows[0]
    for k in ('duid', 'site_name', 'owner', 'region', 'capacity_mw', 'storage_mwh', 'value'):
        assert k in row, f'missing row.{k}'


def test_overview_invalid_region_returns_400(client, auth_headers):
    r = _get(client, auth_headers, region='ZZ1', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    assert r.status_code == 400


def test_overview_unknown_metric_returns_422(client, auth_headers):
    r = _get(client, auth_headers, metric='solar_revenue', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    assert r.status_code == 422


def test_overview_discharge_energy_values_match_fixture(client, auth_headers):
    r = _get(client, auth_headers, metric='discharge_energy', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    by_duid = {row['duid']: row['value'] for row in r.json()['data']}
    assert by_duid['TIB1']  == TIB1_DISCHARGE_MWH
    assert by_duid['HPRG1'] == HPRG1_DISCHARGE_MWH


def test_overview_charge_energy_values_match_fixture(client, auth_headers):
    r = _get(client, auth_headers, metric='charge_energy', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    by_duid = {row['duid']: row['value'] for row in r.json()['data']}
    assert by_duid['TIB1']  == TIB1_CHARGE_MWH
    assert by_duid['HPRG1'] == HPRG1_CHARGE_MWH


def test_overview_orders_by_selected_metric_desc(client, auth_headers):
    r = _get(client, auth_headers, metric='discharge_energy', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    duids = [row['duid'] for row in r.json()['data']]
    assert duids == ['TIB1', 'HPRG1']


def test_overview_region_sa_only_returns_tib1(client, auth_headers):
    r = _get(client, auth_headers, region='SA1', metric='discharge_energy', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    duids = [row['duid'] for row in r.json()['data']]
    assert duids == ['TIB1']


def test_overview_region_nsw_returns_empty(client, auth_headers):
    r = _get(client, auth_headers, region='NSW1', metric='discharge_energy', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    assert r.json()['data'] == []
    assert r.json()['meta']['total_count'] == 0


def test_overview_limit_param(client, auth_headers):
    r = _get(client, auth_headers, metric='discharge_energy', limit=1, **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    assert len(r.json()['data']) == 1
    assert r.json()['meta']['limit'] == 1
    # total_count reports the unsliced size
    assert r.json()['meta']['total_count'] == 2


def test_overview_window_outside_data_returns_empty(client, auth_headers):
    r = _get(client, auth_headers, metric='discharge_energy',
             **{'from': '2025-01-01T00:00:00', 'to': '2025-01-02T00:00:00'})
    assert r.json()['data'] == []


def test_overview_meta_echoes_label_units_for_each_metric(client, auth_headers):
    expected_units = {
        'discharge_revenue': '$',
        'charge_cost':       '$',
        'discharge_price':   '$/MWh',
        'charge_price':      '$/MWh',
        'discharge_energy':  'MWh',
        'charge_energy':     'MWh',
        'price_spread':      '$/MWh',
    }
    for metric, units in expected_units.items():
        r = _get(client, auth_headers, metric=metric, **{'from': WINDOW_FROM, 'to': WINDOW_TO})
        assert r.status_code == 200, f'{metric}: {r.text}'
        assert r.json()['meta']['units'] == units.replace('\$','$'), metric
