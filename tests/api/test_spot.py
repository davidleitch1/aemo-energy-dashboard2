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
