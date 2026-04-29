"""GET /v1/meta/freshness — latest update times across the data sources."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from ..db import get_connection, nem_naive_to_utc

router = APIRouter()


def _utc_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return nem_naive_to_utc(dt).isoformat().replace("+00:00", "Z")


@router.get("/meta/freshness")
async def freshness() -> dict:
    conn = get_connection()
    try:
        row = conn.execute("SELECT MAX(settlementdate) FROM prices5").fetchone()
    finally:
        conn.close()

    latest_prices = row[0] if row and row[0] is not None else None

    return {
        "data": {
            "prices5": _utc_iso(latest_prices),
        },
        "meta": {
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }
