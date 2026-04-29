"""Shape tests for GET /v1/meta/freshness."""
from __future__ import annotations


def test_freshness_returns_200_with_auth(client, auth_headers):
    resp = client.get("/v1/meta/freshness", headers=auth_headers)
    assert resp.status_code == 200


def test_freshness_payload_shape(client, auth_headers):
    resp = client.get("/v1/meta/freshness", headers=auth_headers)
    body = resp.json()
    assert "data" in body
    assert "meta" in body
    assert "as_of" in body["meta"]
    assert "prices5" in body["data"]


def test_freshness_returns_401_without_auth_header(client):
    resp = client.get("/v1/meta/freshness")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "UNAUTHORIZED"


def test_freshness_validates_token_when_tokens_file_set(client, tmp_path, monkeypatch):
    """When API_TOKENS_FILE is configured, only listed tokens are accepted."""
    tokens_file = tmp_path / "tokens.yaml"
    tokens_file.write_text("real-token: alice\n")
    monkeypatch.setenv("API_TOKENS_FILE", str(tokens_file))

    import aemo_dashboard.api.auth as auth_module
    auth_module.reset_tokens_for_tests()

    bad = client.get("/v1/meta/freshness", headers={"Authorization": "Bearer wrong"})
    assert bad.status_code == 401

    good = client.get("/v1/meta/freshness", headers={"Authorization": "Bearer real-token"})
    assert good.status_code == 200

    auth_module.reset_tokens_for_tests()
