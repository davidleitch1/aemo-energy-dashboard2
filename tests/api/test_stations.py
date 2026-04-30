"""Tests for /v1/stations*. Fixture has 11 rows in duid_info covering
~6 stations and scada30 for TIB1/HPRG1 only."""
from __future__ import annotations


def test_stations_requires_auth(client):
    assert client.get('/v1/stations').status_code == 401


def test_stations_default_returns_200(client, auth_headers):
    r = client.get('/v1/stations', headers=auth_headers)
    assert r.status_code == 200


def test_stations_top_level_shape(client, auth_headers):
    body = client.get('/v1/stations', headers=auth_headers).json()
    assert 'data' in body and isinstance(body['data'], list)
    for k in ('count', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'
    assert body['meta']['count'] == len(body['data'])


def test_stations_row_shape(client, auth_headers):
    rows = client.get('/v1/stations', headers=auth_headers).json()['data']
    assert rows
    for k in ('station_name', 'region', 'owner', 'fuel', 'capacity_mw', 'duid_count'):
        assert k in rows[0], f'missing row.{k}'


def test_stations_ordered_by_capacity_desc(client, auth_headers):
    rows = client.get('/v1/stations', headers=auth_headers).json()['data']
    caps = [r['capacity_mw'] for r in rows]
    assert caps == sorted(caps, reverse=True)


def test_stations_region_filter(client, auth_headers):
    rows = client.get('/v1/stations?region=NSW1', headers=auth_headers).json()['data']
    assert all(r['region'] == 'NSW1' for r in rows)


def test_stations_invalid_region_returns_400(client, auth_headers):
    assert client.get('/v1/stations?region=ZZ1', headers=auth_headers).status_code == 400


def test_stations_groups_multiple_duids_into_single_row(client, auth_headers):
    """Mt Stuart has 3 DUIDs in fixture (MSTUART1/2/3); should appear once
    with capacity 145+130+120 = 395 MW and duid_count=3."""
    rows = client.get('/v1/stations?region=QLD1', headers=auth_headers).json()['data']
    by_name = {r['station_name']: r for r in rows}
    assert 'Mt Stuart' in by_name
    assert by_name['Mt Stuart']['duid_count'] == 3
    assert by_name['Mt Stuart']['capacity_mw'] == 395.0


# ---------- /v1/stations/time-series ----------

def test_ts_requires_auth(client):
    assert client.get('/v1/stations/time-series?station=Tarong').status_code == 401


def test_ts_unknown_station_returns_404(client, auth_headers):
    r = client.get('/v1/stations/time-series?station=NotAStation', headers=auth_headers)
    assert r.status_code == 404


def test_ts_meta_shape(client, auth_headers):
    """Tailem Bend exists in fixture as a battery; query against last few days."""
    r = client.get('/v1/stations/time-series?station=Tailem%20Bend&period_days=2',
                   headers=auth_headers)
    assert r.status_code == 200
    meta = r.json()['meta']
    for k in ('station_name', 'region', 'fuel', 'capacity_mw', 'frequency',
              'period_days', 'from', 'to', 'source_rows', 'returned_rows', 'downsampled'):
        assert k in meta, f'missing meta.{k}'


def test_ts_invalid_frequency_returns_422(client, auth_headers):
    r = client.get('/v1/stations/time-series?station=Tailem%20Bend&frequency=5m',
                   headers=auth_headers)
    assert r.status_code == 422


# ---------- /v1/stations/tod ----------

def test_tod_requires_auth(client):
    assert client.get('/v1/stations/tod?station=Tarong').status_code == 401


def test_tod_unknown_station_returns_404(client, auth_headers):
    r = client.get('/v1/stations/tod?station=NotAStation', headers=auth_headers)
    assert r.status_code == 404


def test_tod_meta_shape(client, auth_headers):
    r = client.get('/v1/stations/tod?station=Tailem%20Bend&period_days=2',
                   headers=auth_headers)
    assert r.status_code == 200
    meta = r.json()['meta']
    for k in ('station_name', 'capacity_mw', 'period_days', 'from', 'to', 'as_of'):
        assert k in meta, f'missing meta.{k}'
