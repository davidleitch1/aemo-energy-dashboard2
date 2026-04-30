"""Tests for the Fleet endpoints:
  GET /v1/batteries/fleet-timeseries (B4)
  GET /v1/batteries/fleet-tod        (B5)

Fixture has scada30 rows for TIB1 (SA1) and HPRG1 (VIC1) over a 24h
window aligned to prices30. Discharge during 17:00-19:00 (200 MW TIB1,
60 MW HPRG1), charge during 11:00-13:00, idle otherwise.
"""
from __future__ import annotations

WINDOW_FROM = '2026-04-28T13:50:00'
WINDOW_TO   = '2026-04-29T13:30:00'


def _ts_qs(**kw) -> str:
    return '&'.join(f'{k}={v}' for k, v in kw.items())


# ----------------------------------------------------------------------
# /fleet-timeseries
# ----------------------------------------------------------------------

def test_ts_requires_auth(client):
    assert client.get('/v1/batteries/fleet-timeseries').status_code == 401


def test_ts_default_returns_both_batteries(client, auth_headers):
    qs = _ts_qs(frequency='30m', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    assert r.status_code == 200
    duids = {row['duid'] for row in r.json()['data']}
    assert duids == {'TIB1', 'HPRG1'}


def test_ts_row_shape(client, auth_headers):
    qs = _ts_qs(frequency='30m', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    rows = r.json()['data']
    assert rows
    for k in ('duid', 't', 'value'):
        assert k in rows[0]


def test_ts_meta_shape(client, auth_headers):
    qs = _ts_qs(frequency='30m', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    meta = r.json()['meta']
    for k in ('frequency', 'duids', 'from', 'to', 'as_of', 'returned_rows', 'source_rows', 'downsampled'):
        assert k in meta, f'missing meta.{k}'
    assert sorted(meta['duids']) == ['HPRG1', 'TIB1']


def test_ts_battery_filter(client, auth_headers):
    qs = _ts_qs(frequency='30m', batteries='TIB1', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    duids = {row['duid'] for row in r.json()['data']}
    assert duids == {'TIB1'}


def test_ts_region_filter(client, auth_headers):
    qs = _ts_qs(frequency='30m', regions='SA1', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    duids = {row['duid'] for row in r.json()['data']}
    assert duids == {'TIB1'}


def test_ts_owner_filter(client, auth_headers):
    qs = _ts_qs(frequency='30m', owners='Neoen', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    duids = {row['duid'] for row in r.json()['data']}
    assert duids == {'HPRG1'}


def test_ts_invalid_frequency_returns_422(client, auth_headers):
    r = client.get('/v1/batteries/fleet-timeseries?frequency=15m', headers=auth_headers)
    assert r.status_code == 422


def test_ts_5m_returns_empty_against_30min_only_fixture(client, auth_headers):
    # Fixture has no scada5 rows — 5m source → empty.
    qs = _ts_qs(frequency='5m', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    assert r.json()['data'] == []


def test_ts_window_outside_data_returns_empty(client, auth_headers):
    qs = _ts_qs(frequency='30m', **{'from': '2025-01-01', 'to': '2025-01-02'})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    assert r.json()['data'] == []


def test_ts_empty_filter_intersection(client, auth_headers):
    qs = _ts_qs(frequency='30m', regions='VIC1', owners='Vena',
                **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    assert r.json()['data'] == []


def test_ts_daily_frequency_aggregates(client, auth_headers):
    qs = _ts_qs(frequency='D', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-timeseries?{qs}', headers=auth_headers)
    # Window crosses 2 calendar days (28 Apr + 29 Apr) → 2 buckets per battery
    rows = r.json()['data']
    by_duid = {}
    for row in rows:
        by_duid.setdefault(row['duid'], []).append(row)
    assert len(by_duid['TIB1']) <= 2 and len(by_duid['HPRG1']) <= 2


# ----------------------------------------------------------------------
# /fleet-tod
# ----------------------------------------------------------------------

def test_tod_requires_auth(client):
    assert client.get('/v1/batteries/fleet-tod').status_code == 401


def test_tod_returns_both_batteries(client, auth_headers):
    qs = _ts_qs(**{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-tod?{qs}', headers=auth_headers)
    assert r.status_code == 200
    duids = {row['duid'] for row in r.json()['data']}
    assert duids == {'TIB1', 'HPRG1'}


def test_tod_row_shape(client, auth_headers):
    qs = _ts_qs(**{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-tod?{qs}', headers=auth_headers)
    row = r.json()['data'][0]
    for k in ('duid', 'hour', 'value'):
        assert k in row
    assert isinstance(row['hour'], int)
    assert 0 <= row['hour'] <= 23


def test_tod_discharge_peak_hours(client, auth_headers):
    # TIB1 discharges 200MW during hours 17/18/19. Charge 11/12/13 = -100MW.
    qs = _ts_qs(batteries='TIB1', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-tod?{qs}', headers=auth_headers)
    by_hour = {row['hour']: row['value'] for row in r.json()['data']}
    for h in (17, 18, 19):
        assert by_hour[h] == 200.0, f'hour {h}: {by_hour[h]}'
    for h in (11, 12, 13):
        assert by_hour[h] < 0, f'hour {h} should be negative'


def test_tod_battery_filter(client, auth_headers):
    qs = _ts_qs(batteries='HPRG1', **{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-tod?{qs}', headers=auth_headers)
    duids = {row['duid'] for row in r.json()['data']}
    assert duids == {'HPRG1'}


def test_tod_meta_shape(client, auth_headers):
    qs = _ts_qs(**{'from': WINDOW_FROM, 'to': WINDOW_TO})
    r = client.get(f'/v1/batteries/fleet-tod?{qs}', headers=auth_headers)
    meta = r.json()['meta']
    for k in ('duids', 'from', 'to', 'as_of'):
        assert k in meta, f'missing meta.{k}'
