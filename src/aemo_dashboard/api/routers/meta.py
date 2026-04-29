"""GET /v1/meta/freshness — latest update times across the data sources."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from ..db import get_connection

router = APIRouter()


@router.get("/meta/freshness")
async def freshness() -> dict:
    conn = get_connection()
    row = conn.execute("SELECT MAX(settlementdate) FROM prices5").fetchone()
    latest_prices = row[0] if row and row[0] is not None else None

    return {
        "data": {
            "prices5": latest_prices.isoformat() + "Z" if latest_prices else None,
        },
        "meta": {
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }
