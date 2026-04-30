"""Tests for the Fleet filter-sheet endpoints:
  GET /v1/batteries/owners — distinct owners across battery DUIDs
  GET /v1/batteries/list   — filtered DUIDs by regions[] x owners[]

Fixture has TIB1 (Tailem Bend, Vena, SA1, 235MW/470MWh) and HPRG1
(Hornsdale, Neoen, VIC1, 75MW/150MWh).
"""
from __future__ import annotations


# --- /owners ----------------------------------------------------------

def test_owners_requires_auth(client):
    assert client.get('/v1/batteries/owners').status_code == 401


def test_owners_returns_distinct_list(client, auth_headers):
    r = client.get('/v1/batteries/owners', headers=auth_headers)
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body['data'], list)
    assert set(body['data']) == {'Vena', 'Neoen'}
    assert len(body['data']) == len(set(body['data'])), 'list must be deduplicated'


def test_owners_meta_shape(client, auth_headers):
    r = client.get('/v1/batteries/owners', headers=auth_headers)
    body = r.json()
    assert body['meta']['count'] == 2
    assert 'as_of' in body['meta']


def test_owners_sorted_alphabetically(client, auth_headers):
    r = client.get('/v1/batteries/owners', headers=auth_headers)
    data = r.json()['data']
    assert data == sorted(data)


# --- /list ------------------------------------------------------------

def test_list_requires_auth(client):
    assert client.get('/v1/batteries/list').status_code == 401


def test_list_default_returns_all(client, auth_headers):
    r = client.get('/v1/batteries/list', headers=auth_headers)
    assert r.status_code == 200
    duids = [row['duid'] for row in r.json()['data']]
    assert set(duids) == {'TIB1', 'HPRG1'}


def test_list_row_shape(client, auth_headers):
    r = client.get('/v1/batteries/list', headers=auth_headers)
    row = r.json()['data'][0]
    for k in ('duid', 'site_name', 'owner', 'region', 'capacity_mw', 'storage_mwh', 'display'):
        assert k in row, f'missing row.{k}'


def test_list_display_string_format(client, auth_headers):
    r = client.get('/v1/batteries/list?regions=SA1', headers=auth_headers)
    row = r.json()['data'][0]
    assert row['duid'] == 'TIB1'
    assert row['display'] == 'Tailem Bend (TIB1) — 235 MW / 470 MWh (2.0h)'


def test_list_orders_by_capacity_desc(client, auth_headers):
    r = client.get('/v1/batteries/list', headers=auth_headers)
    duids = [row['duid'] for row in r.json()['data']]
    assert duids == ['TIB1', 'HPRG1']  # 235 MW > 75 MW


def test_list_region_filter_single(client, auth_headers):
    r = client.get('/v1/batteries/list?regions=SA1', headers=auth_headers)
    duids = [row['duid'] for row in r.json()['data']]
    assert duids == ['TIB1']


def test_list_region_filter_multi(client, auth_headers):
    r = client.get('/v1/batteries/list?regions=SA1,VIC1', headers=auth_headers)
    duids = sorted(row['duid'] for row in r.json()['data'])
    assert duids == ['HPRG1', 'TIB1']


def test_list_owner_filter(client, auth_headers):
    r = client.get('/v1/batteries/list?owners=Vena', headers=auth_headers)
    duids = [row['duid'] for row in r.json()['data']]
    assert duids == ['TIB1']


def test_list_region_owner_intersection_empty(client, auth_headers):
    # Vena's only battery is in SA1; intersect with VIC1 → no rows.
    r = client.get('/v1/batteries/list?regions=VIC1&owners=Vena', headers=auth_headers)
    assert r.json()['data'] == []
    assert r.json()['meta']['count'] == 0


def test_list_invalid_region_400(client, auth_headers):
    r = client.get('/v1/batteries/list?regions=ZZ1', headers=auth_headers)
    assert r.status_code == 400


def test_list_meta_echoes_filters(client, auth_headers):
    r = client.get('/v1/batteries/list?regions=SA1&owners=Vena', headers=auth_headers)
    meta = r.json()['meta']
    assert meta['regions'] == ['SA1']
    assert meta['owners'] == ['Vena']
    assert meta['count'] == 1
