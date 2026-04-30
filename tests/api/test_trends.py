"""Shape + parameter tests for GET /v1/trends/* endpoints.

The test fixture has only ~30 days of rooftop and a single generation
timestamp, so smoothing/aggregation behaviour is verified via live
smoke testing rather than against the fixture. These tests cover the
request/response contract.
"""
from __future__ import annotations


# ---------- /v1/trends/vre-production ----------

def test_vreprod_returns_200(client, auth_headers):
    resp = client.get('/v1/trends/vre-production?region=NEM&fuel=VRE', headers=auth_headers)
    assert resp.status_code == 200


def test_vreprod_returns_401_without_auth(client):
    resp = client.get('/v1/trends/vre-production?region=NEM&fuel=VRE')
    assert resp.status_code == 401


def test_vreprod_top_level_shape(client, auth_headers):
    body = client.get(
        '/v1/trends/vre-production?region=NEM&fuel=VRE', headers=auth_headers
    ).json()
    assert 'data' in body and 'meta' in body
    for k in ('region', 'fuel', 'years', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'
    assert isinstance(body['meta']['years'], list)
    assert len(body['meta']['years']) == 3


def test_vreprod_point_shape(client, auth_headers):
    body = client.get(
        '/v1/trends/vre-production?region=NEM&fuel=VRE', headers=auth_headers
    ).json()
    if body['data']:
        p = body['data'][0]
        for k in ('year', 'dayofyear', 'twh'):
            assert k in p, f'missing {k}'
        assert isinstance(p['year'], int)
        assert isinstance(p['dayofyear'], int)
        assert 1 <= p['dayofyear'] <= 366
        assert isinstance(p['twh'], (int, float))


def test_vreprod_invalid_region(client, auth_headers):
    resp = client.get(
        '/v1/trends/vre-production?region=ZZ1&fuel=VRE', headers=auth_headers
    )
    assert resp.status_code == 400


def test_vreprod_invalid_fuel(client, auth_headers):
    resp = client.get(
        '/v1/trends/vre-production?region=NEM&fuel=Coal', headers=auth_headers
    )
    assert resp.status_code == 422


def test_vreprod_accepts_each_valid_fuel(client, auth_headers):
    for fuel in ('VRE', 'Solar', 'Wind', 'Rooftop'):
        resp = client.get(
            f'/v1/trends/vre-production?region=NEM&fuel={fuel}', headers=auth_headers
        )
        assert resp.status_code == 200, f'{fuel}: {resp.status_code} {resp.text}'


# ---------- /v1/trends/vre-by-fuel ----------

def test_vrebyfuel_returns_200(client, auth_headers):
    resp = client.get('/v1/trends/vre-by-fuel?region=NEM', headers=auth_headers)
    assert resp.status_code == 200


def test_vrebyfuel_returns_401_without_auth(client):
    resp = client.get('/v1/trends/vre-by-fuel?region=NEM')
    assert resp.status_code == 401


def test_vrebyfuel_top_level_shape(client, auth_headers):
    body = client.get(
        '/v1/trends/vre-by-fuel?region=NEM', headers=auth_headers
    ).json()
    assert 'data' in body and 'meta' in body
    for k in ('region', 'fuels', 'from', 'to', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'
    assert set(body['meta']['fuels']) == {'Rooftop', 'Solar', 'Wind'}


def test_vrebyfuel_point_shape(client, auth_headers):
    body = client.get(
        '/v1/trends/vre-by-fuel?region=NEM', headers=auth_headers
    ).json()
    if body['data']:
        p = body['data'][0]
        for k in ('date', 'fuel', 'twh'):
            assert k in p, f'missing {k}'
        assert p['fuel'] in {'Rooftop', 'Solar', 'Wind'}


def test_vrebyfuel_invalid_region(client, auth_headers):
    resp = client.get(
        '/v1/trends/vre-by-fuel?region=ZZ1', headers=auth_headers
    )
    assert resp.status_code == 400


# ---------- /v1/trends/thermal-vs-renewables ----------

def test_thermren_returns_200(client, auth_headers):
    resp = client.get('/v1/trends/thermal-vs-renewables?region=NEM', headers=auth_headers)
    assert resp.status_code == 200


def test_thermren_returns_401_without_auth(client):
    resp = client.get('/v1/trends/thermal-vs-renewables?region=NEM')
    assert resp.status_code == 401


def test_thermren_top_level_shape(client, auth_headers):
    body = client.get(
        '/v1/trends/thermal-vs-renewables?region=NEM', headers=auth_headers
    ).json()
    assert 'data' in body and 'meta' in body
    for k in ('region', 'categories', 'from', 'to', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'
    assert set(body['meta']['categories']) == {'renewable', 'thermal'}


def test_thermren_point_shape(client, auth_headers):
    body = client.get(
        '/v1/trends/thermal-vs-renewables?region=NEM', headers=auth_headers
    ).json()
    if body['data']:
        p = body['data'][0]
        for k in ('date', 'category', 'twh'):
            assert k in p, f'missing {k}'
        assert p['category'] in {'renewable', 'thermal'}


def test_thermren_invalid_region(client, auth_headers):
    resp = client.get(
        '/v1/trends/thermal-vs-renewables?region=ZZ1', headers=auth_headers
    )
    assert resp.status_code == 400


def test_all_three_accept_all_six_regions(client, auth_headers):
    for region in ('NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'):
        for path in (
            f'/v1/trends/vre-production?region={region}&fuel=VRE',
            f'/v1/trends/vre-by-fuel?region={region}',
            f'/v1/trends/thermal-vs-renewables?region={region}',
        ):
            resp = client.get(path, headers=auth_headers)
            assert resp.status_code == 200, f'{path}: {resp.status_code}'
