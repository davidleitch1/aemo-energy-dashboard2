"""Shape + correctness tests for GET /v1/prices/bands."""
from __future__ import annotations


def test_bands_returns_200(client, auth_headers):
    resp = client.get('/v1/prices/bands?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_bands_returns_401_without_auth(client):
    resp = client.get('/v1/prices/bands?regions=NSW1')
    assert resp.status_code == 401


def test_bands_top_level_shape(client, auth_headers):
    body = client.get('/v1/prices/bands?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'bins', 'from', 'to', 'resolution', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_bands_row_shape(client, auth_headers):
    body = client.get('/v1/prices/bands?regions=NSW1', headers=auth_headers).json()
    assert body['data'], 'expected at least one band row'
    row = body['data'][0]
    for k in ('region', 'lower', 'upper', 'count', 'share'):
        assert k in row, f'missing row.{k}'
    assert isinstance(row['count'], int)
    assert isinstance(row['share'], (int, float))
    assert row['lower'] < row['upper']


def test_bands_default_bins_count(client, auth_headers):
    body = client.get('/v1/prices/bands?regions=NSW1', headers=auth_headers).json()
    # Default has 11 boundary values → 10 bands. One row per region per band.
    assert body['meta']['bins'] == [-1000.0, 0.0, 25.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 5000.0, 17500.0]
    assert len(body['data']) == 10


def test_bands_shares_sum_to_one_per_region(client, auth_headers):
    body = client.get(
        '/v1/prices/bands?regions=NSW1,SA1,VIC1',
        headers=auth_headers,
    ).json()
    by_region: dict[str, float] = {}
    for row in body['data']:
        by_region.setdefault(row['region'], 0.0)
        by_region[row['region']] += row['share']
    for regid, total in by_region.items():
        assert abs(total - 1.0) < 0.01, f'{regid} shares sum to {total}, expected ~1.0'


def test_bands_counts_sum_matches_window(client, auth_headers):
    """Sum of band counts per region should equal the spot-price row count
    in the same window (modulo NULL prices, which are excluded)."""
    bands_body = client.get(
        '/v1/prices/bands?regions=NSW1&from=2026-04-29T00:00:00Z&to=2026-04-29T12:00:00Z',
        headers=auth_headers,
    ).json()
    band_total = sum(r['count'] for r in bands_body['data'] if r['region'] == 'NSW1')
    # Spot endpoint returns sampled rows, but for a 12h window prices5 is
    # under MAX_POINTS so source_rows tells us the underlying count.
    spot_body = client.get(
        '/v1/prices/spot?region=NSW1&from=2026-04-29T00:00:00Z&to=2026-04-29T12:00:00Z',
        headers=auth_headers,
    ).json()
    # source_rows on the spot endpoint counts joined rows in NSW1.
    assert band_total == spot_body['meta']['source_rows']


def test_bands_invalid_region_returns_400(client, auth_headers):
    resp = client.get('/v1/prices/bands?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400


def test_bands_missing_region_returns_400(client, auth_headers):
    resp = client.get('/v1/prices/bands', headers=auth_headers)
    assert resp.status_code == 400


def test_bands_custom_bins(client, auth_headers):
    body = client.get(
        '/v1/prices/bands?regions=NSW1&bins=0,50,100,200',
        headers=auth_headers,
    ).json()
    assert body['meta']['bins'] == [0.0, 50.0, 100.0, 200.0]
    assert len(body['data']) == 3  # one row per band per region


def test_bands_invalid_bins_non_ascending(client, auth_headers):
    resp = client.get(
        '/v1/prices/bands?regions=NSW1&bins=100,50,200',
        headers=auth_headers,
    )
    assert resp.status_code == 400


def test_bands_invalid_bins_too_few(client, auth_headers):
    resp = client.get(
        '/v1/prices/bands?regions=NSW1&bins=50',
        headers=auth_headers,
    )
    assert resp.status_code == 400


def test_bands_multi_region(client, auth_headers):
    body = client.get(
        '/v1/prices/bands?regions=NSW1,SA1,VIC1',
        headers=auth_headers,
    ).json()
    seen = {r['region'] for r in body['data']}
    assert seen.issubset({'NSW1', 'SA1', 'VIC1'})


def test_bands_window_outside_data_returns_empty(client, auth_headers):
    body = client.get(
        '/v1/prices/bands?regions=NSW1&from=2010-01-01T00:00:00Z&to=2010-01-02T00:00:00Z',
        headers=auth_headers,
    ).json()
    # No prices in that window → all band counts are 0 (or no data rows).
    for r in body['data']:
        assert r['count'] == 0
