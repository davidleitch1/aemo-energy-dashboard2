"""Shape tests for GET /v1/generation/mix."""
from __future__ import annotations


def test_mix_returns_200(client, auth_headers):
    resp = client.get('/v1/generation/mix?regions=NSW1', headers=auth_headers)
    assert resp.status_code == 200


def test_mix_returns_401_without_auth(client):
    resp = client.get('/v1/generation/mix?regions=NSW1')
    assert resp.status_code == 401


def test_mix_top_level_shape(client, auth_headers):
    body = client.get('/v1/generation/mix?regions=NSW1', headers=auth_headers).json()
    assert 'data' in body and 'meta' in body
    for k in ('regions', 'resolution', 'fuels', 'as_of'):
        assert k in body['meta'], f'missing meta.{k}'


def test_mix_point_shape(client, auth_headers):
    body = client.get('/v1/generation/mix?regions=NSW1', headers=auth_headers).json()
    if body['data']:
        p = body['data'][0]
        for k in ('timestamp', 'fuel', 'mw'):
            assert k in p, f'missing {k}'
        assert isinstance(p['mw'], (int, float))


def test_mix_no_biomass(client, auth_headers):
    body = client.get('/v1/generation/mix?regions=NSW1,QLD1,VIC1,SA1,TAS1', headers=auth_headers).json()
    fuels = {p['fuel'] for p in body['data']}
    assert 'Biomass' not in fuels
    # CCGT/OCGT/Gas other should be merged into 'Gas'
    assert 'CCGT' not in fuels
    assert 'OCGT' not in fuels


def test_mix_invalid_region(client, auth_headers):
    resp = client.get('/v1/generation/mix?regions=ZZ1', headers=auth_headers)
    assert resp.status_code == 400


def test_mix_resolution_auto_picks_buckets(client, auth_headers):
    # 1y window should pick daily buckets — anything finer crams 1000+ points
    # into a phone-width chart and the diurnal cycle compresses into noise.
    resp = client.get(
        '/v1/generation/mix?regions=NSW1&from=2025-04-01T00:00:00Z&to=2026-04-01T00:00:00Z',
        headers=auth_headers,
    )
    assert resp.json()['meta']['resolution'] == '1d'


def test_mix_resolution_thresholds(client, auth_headers):
    """Verify the bucket ladder: 24h→5min, 7d→30min, 30d→1h, >31d→1d."""
    cases = [
        ('2026-04-29T00:00:00Z', '2026-04-29T20:00:00Z', '5min'),   # 20h
        ('2026-04-22T20:00:00Z', '2026-04-29T20:00:00Z', '30min'),  # 7d
        ('2026-03-30T20:00:00Z', '2026-04-29T20:00:00Z', '1h'),     # 30d
        ('2025-04-29T20:00:00Z', '2026-04-29T20:00:00Z', '1d'),     # 365d
    ]
    for from_, to, expected in cases:
        body = client.get(
            f'/v1/generation/mix?regions=NSW1&from={from_}&to={to}',
            headers=auth_headers,
        ).json()
        assert body['meta']['resolution'] == expected, (
            f'window {from_}..{to}: expected {expected}, got {body["meta"]["resolution"]}'
        )


def test_mix_5min_emits_rooftop_at_every_util_stamp(client, auth_headers):
    """Regression: at 5-min resolution, Rooftop must be densified onto every
    5-min stamp where utility-scale fuels exist. Otherwise Swift Charts'
    stacked AreaMarks step the whole stack up at every 30-min boundary,
    creating the vertical-needle 'spike' pattern.

    The fixture has 5-min utility data spanning 2026-04-28 20:00 to 2026-04-29
    20:00 (NEM time = UTC+10). Pick a midday-NEM window where rooftop > 0.
    """
    # Midday NEM = ~02:00 UTC. Use 2026-04-29 01:30Z..03:30Z (i.e. 11:30..13:30 NEM).
    body = client.get(
        '/v1/generation/mix?regions=NSW1,QLD1,VIC1,SA1,TAS1'
        '&from=2026-04-29T01:30:00Z&to=2026-04-29T03:30:00Z',
        headers=auth_headers,
    ).json()
    assert body['meta']['resolution'] == '5min'

    util_stamps = {p['timestamp'] for p in body['data']
                   if p['fuel'] not in ('Rooftop', 'Battery Charging',
                                        'Transmission Imports', 'Transmission Exports')
                   and p['mw'] > 0}
    rooftop_stamps = {p['timestamp'] for p in body['data'] if p['fuel'] == 'Rooftop'}

    # Every 5-min util stamp in the window must have a corresponding Rooftop point.
    missing = util_stamps - rooftop_stamps
    assert not missing, f'Rooftop missing at {len(missing)} util stamps, e.g. {sorted(missing)[:5]}'

    # And we should have a non-trivial number — the window covers two hours,
    # so 24 5-min util stamps and the same number of rooftop tiles.
    assert len(rooftop_stamps) >= 20, f'expected >=20 rooftop stamps, got {len(rooftop_stamps)}'


def test_mix_5min_rooftop_constant_within_30min_window(client, auth_headers):
    """The densification tiles each 30-min source row across six 5-min stamps,
    so the six tiles within a [HH:00, HH:25] or [HH:30, HH:55] window must
    share the same MW value (no interpolation, just step).
    """
    body = client.get(
        '/v1/generation/mix?regions=NSW1,QLD1,VIC1,SA1,TAS1'
        '&from=2026-04-29T01:30:00Z&to=2026-04-29T03:30:00Z',
        headers=auth_headers,
    ).json()
    rooftop = [(p['timestamp'], p['mw']) for p in body['data'] if p['fuel'] == 'Rooftop']

    # Group by 30-min bucket (zero out minutes mod 30)
    from collections import defaultdict
    groups: dict[str, set[float]] = defaultdict(set)
    for ts, mw in rooftop:
        # ts is 'YYYY-MM-DDTHH:MM:SSZ'; round MM down to 00 or 30
        hh, mm = ts[11:13], int(ts[14:16])
        bucket_key = ts[:11] + hh + ':' + ('00' if mm < 30 else '30')
        groups[bucket_key].add(round(mw, 1))

    # Each 30-min bucket should have <=1 distinct MW value.
    bad = {k: v for k, v in groups.items() if len(v) > 1}
    assert not bad, f'rooftop varies within 30-min windows: {bad}'
