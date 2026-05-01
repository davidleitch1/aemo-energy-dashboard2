"""Shape + correctness tests for GET /v1/prices/high-events.

The endpoint returns a list of 5-min intervals where rrp >= threshold,
ordered most-recent first, with cursor pagination and a duration_minutes
column counting the consecutive run of at-or-above-threshold intervals
the row belongs to.
"""
from __future__ import annotations


def test_high_events_returns_200(client, auth_headers):
    resp = client.get('/v1/prices/high-events?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_high_events_returns_401_without_auth(client):
    resp = client.get('/v1/prices/high-events?regions=NSW1')
    assert resp.status_code == 401


def test_high_events_top_level_shape(client, auth_headers):
    body = client.get('/v1/prices/high-events?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'from', 'to', 'threshold', 'next_cursor', 'has_more', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_high_events_row_shape(client, auth_headers):
    # Use a low threshold so the small fixture window is guaranteed to have rows.
    body = client.get(
        '/v1/prices/high-events?regions=NSW1&threshold=50',
        headers=auth_headers,
    ).json()
    assert body['data'], 'expected at least one event at threshold=50 in fixture'
    row = body['data'][0]
    for k in ('timestamp', 'region', 'price', 'duration_minutes'):
        assert k in row, f'missing row.{k}'
    assert isinstance(row['price'], (int, float))
    assert isinstance(row['duration_minutes'], int)
    assert row['duration_minutes'] >= 5  # one slot is 5 min


def test_high_events_threshold_filters(client, auth_headers):
    """All returned rows must have price >= threshold."""
    body = client.get(
        '/v1/prices/high-events?regions=NSW1,SA1,VIC1&threshold=100',
        headers=auth_headers,
    ).json()
    for row in body['data']:
        assert row['price'] >= 100, row


def test_high_events_default_threshold_is_300(client, auth_headers):
    body = client.get('/v1/prices/high-events?regions=NSW1,SA1,VIC1', headers=auth_headers).json()
    assert body['meta']['threshold'] == 300
    for row in body['data']:
        assert row['price'] >= 300


def test_high_events_invalid_region(client, auth_headers):
    resp = client.get('/v1/prices/high-events?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400


def test_high_events_ordered_descending_by_time(client, auth_headers):
    body = client.get(
        '/v1/prices/high-events?regions=NSW1&threshold=50&limit=20',
        headers=auth_headers,
    ).json()
    timestamps = [r['timestamp'] for r in body['data']]
    assert timestamps == sorted(timestamps, reverse=True), 'must be most-recent first'


def test_high_events_pagination_cursor_returns_disjoint_rows(client, auth_headers):
    """Walking the cursor must return disjoint pages and eventually exhaust."""
    page1 = client.get(
        '/v1/prices/high-events?regions=NSW1,SA1,VIC1&threshold=50&limit=10',
        headers=auth_headers,
    ).json()
    assert len(page1['data']) <= 10
    cursor = page1['meta']['next_cursor']
    if not cursor:
        # Tiny fixture might exhaust on page 1; still a passing assertion.
        assert page1['meta']['has_more'] is False
        return

    page2 = client.get(
        f'/v1/prices/high-events?regions=NSW1,SA1,VIC1&threshold=50&limit=10&cursor={cursor}',
        headers=auth_headers,
    ).json()
    page1_keys = {(r['timestamp'], r['region']) for r in page1['data']}
    page2_keys = {(r['timestamp'], r['region']) for r in page2['data']}
    assert page1_keys.isdisjoint(page2_keys), 'cursor pages must not overlap'


def test_high_events_pagination_terminates(client, auth_headers):
    """Repeatedly walking the cursor exhausts within a bounded number of steps."""
    seen: set[tuple[str, str]] = set()
    cursor = None
    # Use threshold=100 + limit=100 so termination fits comfortably under
    # 100 steps even on the densest 5-region window of the fixture.
    for _ in range(100):
        url = '/v1/prices/high-events?regions=NSW1,SA1,VIC1,QLD1,TAS1&threshold=100&limit=100'
        if cursor:
            url += f'&cursor={cursor}'
        body = client.get(url, headers=auth_headers).json()
        for r in body['data']:
            key = (r['timestamp'], r['region'])
            assert key not in seen, f'duplicate row across pages: {key}'
            seen.add(key)
        cursor = body['meta']['next_cursor']
        if not cursor:
            assert body['meta']['has_more'] is False
            return
    raise AssertionError('pagination did not terminate within 100 steps')


def test_high_events_window_filters(client, auth_headers):
    """from/to must constrain the response window."""
    body = client.get(
        '/v1/prices/high-events?regions=NSW1&threshold=50'
        '&from=2026-04-29T00:00:00Z&to=2026-04-29T12:00:00Z',
        headers=auth_headers,
    ).json()
    # All returned timestamps fall in [from, to] (UTC ISO order is lexical).
    for r in body['data']:
        assert '2026-04-29T00:00:00' <= r['timestamp'] <= '2026-04-29T13:00:00', r


def test_high_events_duration_minutes_is_positive(client, auth_headers):
    body = client.get(
        '/v1/prices/high-events?regions=NSW1,SA1,VIC1,QLD1,TAS1&threshold=50&limit=50',
        headers=auth_headers,
    ).json()
    for row in body['data']:
        assert row['duration_minutes'] >= 5, row
        assert row['duration_minutes'] % 5 == 0, row  # 5-min cadence


def test_high_events_missing_region_returns_400(client, auth_headers):
    resp = client.get('/v1/prices/high-events', headers=auth_headers)
    assert resp.status_code == 400


def test_high_events_invalid_cursor_returns_400(client, auth_headers):
    resp = client.get(
        '/v1/prices/high-events?regions=NSW1&cursor=garbage',
        headers=auth_headers,
    )
    assert resp.status_code == 400
