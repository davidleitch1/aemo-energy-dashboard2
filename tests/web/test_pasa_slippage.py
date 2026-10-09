"""MT-PASA episodes, mothballed units and return-date slippage."""
from __future__ import annotations

import pandas as pd
import pytest

from aemo_dashboard.web import pasa_data as pm

D = pd.Timestamp


def _units():
    return pd.DataFrame({
        "duid": ["COAL1", "COAL2", "GAS1", "HYD1", "SMALL"],
        "site_name": ["Coal Stn", "Coal Two", "Gas Stn", "Hydro Stn", "Peaker"],
        "region": ["NSW1", "QLD1", "VIC1", "TAS1", "SA1"],
        "fuel": ["Coal", "Coal", "CCGT", "Water", "OCGT"],
        "capacity_mw": [700.0, 600.0, 300.0, 100.0, 60.0],
    })


def _view(pub, specs):
    """specs: (duid, first_day, last_day, avail, state) -> daily rows."""
    rows = []
    for duid, d0, d1, avail, state in specs:
        for day in pd.date_range(d0, d1):
            rows.append({"PUBLISH_DATETIME": D(pub), "DUID": duid, "DAY": day,
                         "PASAAVAILABILITY": avail, "PASAUNITSTATE": state,
                         "PASARECALLTIME": ""})
    return pd.DataFrame(rows)


def _hist(views):
    """Each publish restates every unit's days (as MT-PASA does): all clear,
    then the listed outages override."""
    base = [_clear(d) for d in ["COAL1", "COAL2", "GAS1", "HYD1"]]
    out = []
    for p, s in views:
        v = _view(p, base + list(s))
        out.append(v.drop_duplicates(["DUID", "DAY"], keep="last"))
    return pd.concat(out, ignore_index=True)


def _out(duid, d0, d1, avail=0, state="OUTAGEPLANBASIC"):
    return (duid, d0, d1, avail, state)


def _clear(duid, d0="2026-09-01", d1="2027-03-01"):
    return (duid, d0, d1, 700, "NODERATINGS")


# ---------------------------------------------------------------- episodes

def test_outage_days_filter_state_and_threshold():
    view = _view("2026-10-09 18:00", [
        _out("COAL1", "2026-10-10", "2026-10-12"),
        ("COAL1", "2026-10-13", "2026-10-13", 0, "MOTHBALLED"),
        ("GAS1", "2026-10-10", "2026-10-10", 280, "DERATINGPLANBASIC"),  # 20 MW
        ("GAS1", "2026-10-11", "2026-10-11", 200, "DERATINGUNPLANFORCED"),
        ("SMALL", "2026-10-10", "2026-10-10", 0, "OUTAGEPLANBASIC"),     # 60 MW ok
        ("WIND9", "2026-10-10", "2026-10-10", 0, "OUTAGEPLANBASIC"),     # not scheduled
    ])
    d = pm.outage_days(view, _units(), 50)
    assert set(d["duid"]) == {"COAL1", "GAS1", "SMALL"}
    assert len(d[d.duid == "COAL1"]) == 3
    g = d[d.duid == "GAS1"]
    assert list(g["day"]) == [D("2026-10-11")] and g.iloc[0]["type"] == "Unplanned"


def test_episodes_merge_gap_of_one_day_only():
    view = _view("2026-10-09 18:00", [
        _out("COAL1", "2026-10-10", "2026-10-12"),
        _out("COAL1", "2026-10-14", "2026-10-15"),     # one-day gap: merge
        _out("COAL1", "2026-10-18", "2026-10-19"),     # two-day gap: new episode
    ])
    e = pm.episodes(pm.outage_days(view, _units(), 50)).sort_values("start")
    assert len(e) == 2
    assert e.iloc[0]["start"] == D("2026-10-10") and e.iloc[0]["end"] == D("2026-10-15")
    assert e.iloc[0]["return_date"] == D("2026-10-16")
    assert e.iloc[0]["n_days"] == 6


def test_episode_mw_is_max_over_episode():
    view = _view("2026-10-09 18:00", [
        ("COAL1", "2026-10-10", "2026-10-11", 400, "DERATINGPLANBASIC"),
        ("COAL1", "2026-10-12", "2026-10-12", 0, "OUTAGEPLANBASIC"),
    ])
    e = pm.episodes(pm.outage_days(view, _units(), 50))
    assert len(e) == 1 and e.iloc[0]["mw"] == 700.0


def test_mothballed_by_first_available_day():
    view = _view("2026-10-09 18:00", [
        ("SMALL", "2026-10-11", "2026-10-20", 0, "MOTHBALLED"),
        ("COAL2", "2026-10-11", "2026-10-20", 0, "RETIRED"),
        ("COAL1", "2026-10-11", "2026-10-20", 0, "OUTAGEPLANBASIC"),
        ("HYD1", "2026-10-11", "2026-10-20", 0, "MOTHBALLED"),
    ])
    m = pm.mothballed(view, _units(), today=D("2026-10-10"), threshold=50)
    assert set(m["duid"]) == {"SMALL", "COAL2", "HYD1"}
    assert m.set_index("duid").loc["SMALL", "mw"] == 60.0


# ---------------------------------------------------------------- publishes

def test_weekly_publishes_last_of_iso_week_plus_latest():
    h = _hist([
        ("2026-09-07 06:00", [_clear("COAL1")]), ("2026-09-08 18:00", [_clear("COAL1")]),
        ("2026-09-14 18:00", [_clear("COAL1")]), ("2026-09-16 12:00", [_clear("COAL1")]),
        ("2026-09-18 18:00", [_clear("COAL1")]),
    ])
    pubs = pm.weekly_publishes(h, n_weeks=26)
    assert pubs == [D("2026-09-08 18:00"), D("2026-09-18 18:00")]
    assert pm.weekly_publishes(h, n_weeks=1) == [D("2026-09-18 18:00")]


# ---------------------------------------------------------------- slippage

def _slip_history():
    # weekly publishes A..E and a latest live publish
    pubs = ["2026-09-04 18:00", "2026-09-11 18:00", "2026-09-18 18:00",
            "2026-09-25 18:00", "2026-10-02 18:00", "2026-10-09 18:00"]
    coal_ends = ["2026-10-25", "2026-10-30", "2026-11-05", "2026-11-09",
                 "2026-11-12", "2026-11-16"]
    coal_starts = ["2026-10-20"] * 5 + ["2026-10-24"]   # start moves, overlap holds
    views = []
    for i, p in enumerate(pubs):
        specs = [_out("COAL1", coal_starts[i], coal_ends[i])]
        if i == 0:
            specs.append(_out("COAL1", "2026-12-01", "2026-12-05"))  # other episode
        if i >= 4:
            specs.append(_out("COAL2", "2026-10-15", "2026-10-20"))
        if i == 5:
            specs.append(_out("GAS1", "2026-10-12", "2026-10-14"))
            specs.append(_out("HYD1", "2026-10-12", "2026-10-24"))
        if i < 5:
            specs.append(_out("HYD1", "2026-10-12", "2026-10-31"))
        views.append((p, specs))
    return _hist(views)


@pytest.fixture(scope="module")
def slip():
    h = _slip_history()
    pubs = pm.weekly_publishes(h, 26)
    table, paths = pm.slippage_table(h, _units(), pubs, window_days=30,
                                     threshold=50)
    return table.set_index("duid"), paths, pubs


def test_slipping_outage(slip):
    t, _, _ = slip
    r = t.loc["COAL1"]
    assert r["start"] == D("2026-10-24")
    assert r["return_now"] == D("2026-11-17")
    assert r["ret_1w"] == D("2026-11-13")      # publish of 2 Oct
    assert r["ret_4w"] == D("2026-10-31")      # publish of 11 Sep
    assert r["first_listed"] == D("2026-09-04")
    assert r["orig_return"] == D("2026-10-26")
    assert r["slip_days"] == 22
    assert bool(r["moved_later"]) and not bool(r["is_new"])


def test_only_overlapping_episode_matched(slip):
    t, _, _ = slip
    # the Dec episode in the first publish does not feed the Oct episode
    assert t.loc["COAL1", "orig_return"] == D("2026-10-26")
    assert len(t.loc[["COAL1"]]) == 1          # Dec episode is beyond 30 days


def test_new_outage(slip):
    t, _, _ = slip
    r = t.loc["GAS1"]
    assert bool(r["is_new"]) and r["slip_days"] == 0
    assert r["first_listed"] == D("2026-10-09")
    assert pd.isna(r["ret_1w"]) and bool(r["new_1w"])


def test_outage_listed_one_week(slip):
    t, _, _ = slip
    r = t.loc["COAL2"]
    assert r["first_listed"] == D("2026-10-02")
    assert pd.isna(r["ret_4w"]) and bool(r["new_4w"])
    assert r["slip_days"] == 0 and not bool(r["moved_later"])


def test_early_return(slip):
    t, _, _ = slip
    r = t.loc["HYD1"]
    assert r["orig_return"] == D("2026-11-01")
    assert r["return_now"] == D("2026-10-25")
    assert r["slip_days"] == -7 and not bool(r["moved_later"])


def test_sorted_by_slip_descending(slip):
    t, _, _ = slip
    assert list(t["slip_days"]) == sorted(t["slip_days"], reverse=True)


def test_paths_contain_publish_return_points(slip):
    _, paths, _ = slip
    p = paths[paths.duid == "COAL1"].sort_values("publish")
    assert list(p["return_date"])[0] == D("2026-10-26")
    assert list(p["return_date"])[-1] == D("2026-11-17")
    assert len(p) == 6


def test_open_ended_outage_has_no_slip():
    pubs = ["2026-09-04 18:00", "2026-10-09 18:00"]
    views = [(pubs[0], [_out("COAL1", "2026-10-12", "2027-03-01")]),
             (pubs[1], [_out("COAL1", "2026-10-12", "2027-03-01")])]
    h = _hist(views)
    t, _ = pm.slippage_table(h, _units(), [D(x) for x in pubs], 30, 50)
    r = t.iloc[0]
    assert bool(r["open_ended"]) and pd.isna(r["slip_days"])


# ---------------------------------------------------------------- extended

def _ext_view():
    return _view("2026-10-09 18:00", [
        _clear("COAL1", "2026-10-10", "2027-12-31"),
        _out("COAL1", "2026-10-20", "2026-10-26"),                 # 7 days: kept
        _out("COAL2", "2026-11-01", "2026-11-05"),                 # 5 days: dropped
        _out("GAS1", "2026-09-20", "2026-10-12", state="OUTAGEUNPLANFORCED"),  # overlaps today
        _out("HYD1", "2027-11-01", "2027-11-30"),                  # beyond 12 months
        _out("SMALL", "2026-12-01", "2026-12-31", avail=50),       # 10 MW: below threshold
    ])


def test_extended_episodes_filters():
    today = D("2026-10-10")
    e = pm.extended_episodes(_ext_view(), _units(), today, min_days=7,
                             horizon_days=365, threshold=50)
    assert set(e["duid"]) == {"COAL1", "GAS1"}
    assert e.set_index("duid").loc["GAS1", "type"] == "Unplanned"


def test_weekly_mw_out_by_fuel():
    today = D("2026-10-10")
    view = _view("2026-10-09 18:00", [
        _out("COAL1", "2026-10-10", "2026-10-16"),        # 700 MW all of week 1
        _out("GAS1", "2026-10-10", "2026-10-12"),         # 300 MW for 3 of 7 days
    ])
    days = pm.outage_days(view, _units(), 50)
    w = pm.weekly_mw_out(days, _units(), today, weeks=4)
    assert len(w) == 4
    assert w.loc[today, "Coal"] == 700.0
    assert w.loc[today, "Gas"] == pytest.approx(300 * 3 / 7)
    assert w.iloc[1].sum() == 0.0


def test_weekly_mw_out_region_filter():
    today = D("2026-10-10")
    view = _view("2026-10-09 18:00", [_out("COAL1", "2026-10-10", "2026-10-16"),
                                      _out("COAL2", "2026-10-10", "2026-10-16")])
    days = pm.outage_days(view, _units(), 50)
    w = pm.weekly_mw_out(days, _units(), today, weeks=2, region="QLD1")
    assert w.loc[today, "Coal"] == 600.0


# ---------------------------------------------------------------- review fixes

def test_first_listed_bound_flags(slip):
    t, _, _ = slip
    assert bool(t.loc["COAL1", "first_is_bound"])      # in the earliest publish
    assert not bool(t.loc["COAL2", "first_is_bound"])  # first seen 2 Oct


def test_start_bound_when_outage_predates_history():
    pubs = [D("2026-09-04 18:00"), D("2026-10-09 18:00")]
    views = [(str(p), [_out("COAL1", "2026-09-01", "2026-10-20")]) for p in pubs]
    h = _hist(views)
    t, _ = pm.slippage_table(h, _units(), pubs, 30, 50)
    assert bool(t.iloc[0]["start_bound"])


def _open_hist():
    pubs = [D("2026-09-04 18:00"), D("2026-09-11 18:00"), D("2026-09-18 18:00"),
            D("2026-09-25 18:00"), D("2026-10-02 18:00"), D("2026-10-09 18:00")]
    views = []
    for i, p in enumerate(pubs):
        specs = [_out("HYD1", "2026-10-12", "2027-03-01")]           # always open
        if i < 5:
            specs.append(_out("COAL1", "2026-10-12", "2026-10-20"))
        else:
            specs.append(_out("COAL1", "2026-10-12", "2027-03-01"))  # turns open
        if i == 5:
            specs.append(_out("GAS1", "2026-10-12", "2026-10-14"))
        views.append((str(p), specs))
    return _hist(views), pubs


def test_newly_open_ended():
    h, pubs = _open_hist()
    t, _ = pm.slippage_table(h, _units(), pubs, 30, 50)
    t = t.set_index("duid")
    c = t.loc["COAL1"]
    assert bool(c["open_ended"]) and bool(c["newly_open"])
    assert c["pre_open_return"] == D("2026-10-21")
    assert bool(c["moved_later"])
    hy = t.loc["HYD1"]
    assert bool(hy["open_ended"]) and not bool(hy["newly_open"])
    assert not bool(hy["moved_later"])


def test_sort_order_moved_then_newly_open_then_rest_then_long_open():
    h, pubs = _open_hist()
    t, _ = pm.slippage_table(h, _units(), pubs, 30, 50)
    order = list(t["duid"])
    assert order.index("COAL1") < order.index("GAS1") < order.index("HYD1")
    assert order[-1] == "HYD1"


def test_changed_flag():
    h, pubs = _open_hist()
    t, _ = pm.slippage_table(h, _units(), pubs, 30, 50)
    t = t.set_index("duid")
    assert bool(t.loc["COAL1", "changed"]) and bool(t.loc["GAS1", "changed"])
    assert not bool(t.loc["HYD1", "changed"])


def test_horizon_column_and_chart_slip():
    h, pubs = _open_hist()
    t, _ = pm.slippage_table(h, _units(), pubs, 30, 50)
    t = t.set_index("duid")
    assert t.loc["COAL1", "horizon"] == D("2027-03-01")
    # capped at the horizon: return 2027-03-02 -> 2027-03-01
    assert t.loc["COAL1", "slip_chart"] == (D("2027-03-01") - D("2026-10-21")).days


# ---------------------------------------------------------------- extended rows

def test_extended_one_row_per_unit_helper():
    eps = pd.DataFrame({
        "duid": ["A", "A", "B"], "site_name": ["a", "a", "b"],
        "start": [D("2026-11-01"), D("2027-02-01"), D("2026-10-15")],
        "mw": [100.0, 300.0, 150.0]})
    rows = pm.unit_rows(eps, min_mw=120)
    assert list(rows["duid"]) == ["B", "A"]                # sorted by first start
    assert rows.set_index("duid").loc["A", "mw"] == 300.0  # max over episodes
    assert pm.unit_rows(eps, min_mw=200)["duid"].tolist() == ["A"]
