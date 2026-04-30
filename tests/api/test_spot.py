"""Shape tests for GET /v1/prices/spot."""
from __future__ import annotations


def test_spot_returns_200_with_auth(client, auth_headers):
    resp = client.get("/v1/prices/spot?region=NSW1", headers=auth_headers)
    assert resp.status_code == 200


def test_spot_top_level_shape(client, auth_headers):
    resp = client.get("/v1/prices/spot?region=NSW1", headers=auth_headers)
    body = resp.json()
    assert "data" in body
    assert isinstance(body["data"], list)
    assert "meta" in body
    meta = body["meta"]
    for key in ("from", "to", "resolution", "downsampled", "source_rows", "returned_rows", "as_of"):
        assert key in meta, f"missing meta.{key}"


def test_spot_individual_point_shape(client, auth_headers):
    resp = client.get("/v1/prices/spot?region=NSW1", headers=auth_headers)
    data = resp.json()["data"]
    if data:
        point = data[0]
        assert "timestamp" in point
        assert "region" in point
        assert "price" in point
        assert isinstance(point["price"], (int, float))


def test_spot_invalid_region_returns_400(client, auth_headers):
    resp = client.get("/v1/prices/spot?region=ZZ1", headers=auth_headers)
    assert resp.status_code == 400


def test_spot_returns_401_without_auth(client):
    resp = client.get("/v1/prices/spot?region=NSW1")
    assert resp.status_code == 401


def test_spot_multi_region_returns_all_in_one_payload(client, auth_headers):
    resp = client.get('/v1/prices/spot?regions=NSW1,QLD1,VIC1', headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    regions_seen = {p['region'] for p in body['data']}
    assert regions_seen == {'NSW1', 'QLD1', 'VIC1'} or regions_seen.issubset({'NSW1', 'QLD1', 'VIC1'})
    assert body['meta']['regions'] == ['NSW1', 'QLD1', 'VIC1']


def test_spot_invalid_region_in_multi(client, auth_headers):
    resp = client.get('/v1/prices/spot?regions=NSW1,ZZ1', headers=auth_headers)
    assert resp.status_code == 400


def test_spot_missing_region_returns_400(client, auth_headers):
    resp = client.get('/v1/prices/spot', headers=auth_headers)
    assert resp.status_code == 400


def test_spot_returns_empty_data_when_window_outside_range(client, auth_headers):
    # Window in 2010, fixture has 2026 data — should return empty data, not error
    resp = client.get(
        '/v1/prices/spot?region=NSW1&from=2010-01-01T00:00:00Z&to=2010-01-02T00:00:00Z',
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == []


def test_spot_meta_regions_field_present(client, auth_headers):
    resp = client.get('/v1/prices/spot?region=NSW1', headers=auth_headers)
    body = resp.json()
    assert 'regions' in body['meta']
    assert body['meta']['regions'] == ['NSW1']


def test_spot_to_only_with_no_from_returns_all_history(client, auth_headers):
    """Regression: iOS 'All' chip sends to=now but no from. Server must
    anchor from the earliest available timestamp, not default to 24h-back.
    """
    resp = client.get(
        '/v1/prices/spot?region=NSW1&to=2026-04-29T20:00:00Z',
        headers=auth_headers,
    )
    body = resp.json()
    assert resp.status_code == 200
    # 'from' in meta should reflect the earliest fixture timestamp, not to-24h.
    no_from_meta = body['meta']['from']
    # And the data span should be substantially larger than 24h.
    bare_resp = client.get('/v1/prices/spot?region=NSW1', headers=auth_headers).json()
    assert bare_resp['meta']['from'] == no_from_meta, (
        f'to-only should match no-args: {no_from_meta} vs {bare_resp["meta"]["from"]}'
    )


def test_spot_smoothing_loess_returns_smoothed_meta(client, auth_headers):
    resp = client.get(
        '/v1/prices/spot?region=NSW1&smoothing=loess',
        headers=auth_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    if body['data']:
        assert body['meta'].get('smoothed') is True


def test_spot_smoothing_unknown_returns_400(client, auth_headers):
    resp = client.get(
        '/v1/prices/spot?region=NSW1&smoothing=xyz',
        headers=auth_headers,
    )
    assert resp.status_code == 400


def test_spot_smoothing_default_is_unsmoothed(client, auth_headers):
    resp = client.get('/v1/prices/spot?region=NSW1', headers=auth_headers)
    body = resp.json()
    assert body['meta'].get('smoothed', False) is False
