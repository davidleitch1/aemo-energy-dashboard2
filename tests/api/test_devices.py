"""POST /v1/devices/register — iOS APNs token registration.

The iOS app posts on first launch (and on token refresh) with a body
like:

    {
      "token": "<64-hex-char APNs device token>",
      "user_label": "David's iPhone",            // optional
      "categories": ["price", "new-duid"]        // optional, default all
    }

Server upserts into a JSON registry that the collector's ApnsPushSink
reads each cycle. Re-activates previously-deactivated tokens (when
APNs returned 410 and the sink set active=false; the device has
re-installed and the same or new token has been issued).

Auth: same bearer + CF Access pair as every other /v1 endpoint.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest


# Most tests need to control the tokens-file path. We do that via
# the env var APNS_TOKENS_PATH; the router reads it on each call so
# tests can swap per-test.


@pytest.fixture
def tokens_path(tmp_path, monkeypatch):
    p = tmp_path / 'apns_tokens.json'
    monkeypatch.setenv('APNS_TOKENS_PATH', str(p))
    return p


# ── Auth ─────────────────────────────────────────────────────────────


def test_devices_register_returns_401_without_auth(client, tokens_path):
    resp = client.post('/v1/devices/register', json={'token': 'A' * 64})
    assert resp.status_code == 401


# ── Happy path ───────────────────────────────────────────────────────


def test_register_writes_token_to_file(client, auth_headers, tokens_path):
    resp = client.post(
        '/v1/devices/register',
        json={'token': 'A' * 64,
              'user_label': "David's iPhone",
              'categories': ['price', 'new-duid']},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body['token'] == 'A' * 64
    assert body['active'] is True
    assert body['user_label'] == "David's iPhone"
    assert body['categories'] == ['price', 'new-duid']

    state = json.loads(tokens_path.read_text())
    assert ('A' * 64) in state
    rec = state['A' * 64]
    assert rec['active'] is True
    assert rec['registered_at']
    assert rec['last_seen_at']
    assert rec['user_label'] == "David's iPhone"


def test_register_creates_data_dir_if_missing(client, auth_headers, tokens_path):
    """The tokens file's parent directory might not exist on a fresh
    deployment; endpoint should mkdir parents."""
    nested = tokens_path.parent / 'subdir' / 'apns_tokens.json'
    os.environ['APNS_TOKENS_PATH'] = str(nested)
    try:
        resp = client.post(
            '/v1/devices/register',
            json={'token': 'B' * 64},
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert nested.exists()
    finally:
        os.environ['APNS_TOKENS_PATH'] = str(tokens_path)


# ── Idempotency ──────────────────────────────────────────────────────


def test_register_same_token_twice_is_idempotent(client, auth_headers, tokens_path):
    body1 = {'token': 'C' * 64, 'user_label': 'first'}
    body2 = {'token': 'C' * 64, 'user_label': 'second'}
    client.post('/v1/devices/register', json=body1, headers=auth_headers)
    resp2 = client.post('/v1/devices/register', json=body2, headers=auth_headers)
    assert resp2.status_code == 200
    state = json.loads(tokens_path.read_text())
    # One entry, latest label kept, last_seen_at refreshed
    assert len(state) == 1
    assert state['C' * 64]['user_label'] == 'second'


def test_re_register_reactivates_deactivated_token(client, auth_headers, tokens_path):
    """When APNs returns 410 the sink sets active=false. If the same
    device re-registers (e.g. after re-install), the endpoint must
    flip active back to true."""
    tokens_path.parent.mkdir(parents=True, exist_ok=True)
    tokens_path.write_text(json.dumps({
        'D' * 64: {
            'active': False,
            'user_label': 'David iPhone',
            'registered_at': '2026-01-01T00:00:00Z',
            'last_seen_at':  '2026-04-01T00:00:00Z',
        },
    }))
    resp = client.post(
        '/v1/devices/register',
        json={'token': 'D' * 64},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    state = json.loads(tokens_path.read_text())
    assert state['D' * 64]['active'] is True


# ── Validation ───────────────────────────────────────────────────────


def test_register_missing_token_returns_400(client, auth_headers, tokens_path):
    resp = client.post('/v1/devices/register', json={}, headers=auth_headers)
    assert resp.status_code in (400, 422)  # FastAPI sometimes returns 422


def test_register_empty_token_returns_400(client, auth_headers, tokens_path):
    resp = client.post(
        '/v1/devices/register',
        json={'token': ''},
        headers=auth_headers,
    )
    assert resp.status_code == 400


def test_register_token_too_short_returns_400(client, auth_headers, tokens_path):
    resp = client.post(
        '/v1/devices/register',
        json={'token': 'short'},
        headers=auth_headers,
    )
    assert resp.status_code == 400


def test_register_default_categories_when_omitted(client, auth_headers, tokens_path):
    """If categories isn't provided, default to all known categories."""
    resp = client.post(
        '/v1/devices/register',
        json={'token': 'E' * 64},
        headers=auth_headers,
    )
    body = resp.json()
    assert isinstance(body['categories'], list)
    assert 'price' in body['categories']
    assert 'new-duid' in body['categories']


def test_register_invalid_category_rejected(client, auth_headers, tokens_path):
    resp = client.post(
        '/v1/devices/register',
        json={'token': 'F' * 64, 'categories': ['price', 'bogus-category']},
        headers=auth_headers,
    )
    assert resp.status_code == 400
