"""High Impact Outages (transmission): selection and consolidation."""
from __future__ import annotations

import pandas as pd
import pytest

from aemo_dashboard.web import pasa_transmission as tx

NOW = pd.Timestamp("2026-10-10 12:00")
D = pd.Timestamp


def _df(rows):
    cols = ["Region", "NSP", "Start", "Finish", "Network Asset", "Status",
            "Unplanned?", "Inter-Regional", "report_date"]
    out = pd.DataFrame(rows, columns=cols)
    return out


def _row(region="NSW", start="2026-10-11", finish="2026-10-12", asset="Line A",
         status="Planned - SUBMIT", unpl=None, inter=None, rd="2026-10-06"):
    return (region, "NSP", D(start), D(finish), asset, status, unpl, inter, D(rd))


def test_latest_report_only():
    df = _df([_row(rd="2026-09-29", asset="old"), _row(rd="2026-10-06", asset="new")])
    out = tx.latest_report(df)
    assert list(out["Network Asset"]) == ["new"]
    assert tx.report_date(df) == D("2026-10-06")


def test_in_progress_by_dates_and_status():
    df = _df([
        _row(start="2026-10-09", finish="2026-10-11", asset="by dates"),
        _row(start="2026-10-20", finish="2026-10-21", asset="by status",
             status="In Progress - PTP"),
        _row(start="2026-10-20", finish="2026-10-21", asset="future"),
        _row(region=None, start="2026-10-09", finish="2026-10-11", asset="no region"),
    ])
    out = tx.in_progress(df, NOW)
    assert set(out["Network Asset"]) == {"by dates", "by status"}


def test_unplanned():
    df = _df([_row(unpl="T", finish="2026-10-11", asset="u"),
              _row(unpl="T", start="2026-10-01", finish="2026-10-05", asset="finished"),
              _row(unpl=None, asset="p")])
    assert set(tx.unplanned(df, NOW)["Network Asset"]) == {"u"}


def test_upcoming_30_days_excludes_withdrawn():
    df = _df([_row(start="2026-10-20", asset="in"),
              _row(start="2026-12-20", asset="far"),
              _row(start="2026-10-05", asset="past"),
              _row(start="2026-10-20", status="Withdrawn", asset="w")])
    assert set(tx.upcoming(df, NOW, 30)["Network Asset"]) == {"in"}


def test_inter_regional():
    df = _df([_row(inter="T", asset="ic"), _row(asset="not"),
              _row(inter="T", status="Cancelled", asset="c"),
              _row(inter="T", start="2026-09-01", finish="2026-09-02", asset="past")])
    assert set(tx.inter_regional(df, NOW)["Network Asset"]) == {"ic"}


def test_consolidate_merges_within_two_days():
    df = _df([_row(start="2026-11-01", finish="2026-11-01"),
              _row(start="2026-11-02", finish="2026-11-02"),
              _row(start="2026-11-04", finish="2026-11-04"),
              _row(start="2026-11-20", finish="2026-11-20")])
    out = tx.consolidate(df)
    assert len(out) == 2
    first = out.sort_values("Start").iloc[0]
    assert first["Start"] == D("2026-11-01") and first["Finish"] == D("2026-11-04")
    assert "(3 periods)" in first["Network Asset"]
    assert out.sort_values("Start").iloc[1]["Network Asset"] == "Line A"


def test_region_filter():
    df = _df([_row(region="NSW"), _row(region="SA")])
    assert list(tx.filter_region(df, "SA1")["Region"]) == ["SA"]
    assert len(tx.filter_region(df, "NEM")) == 2
