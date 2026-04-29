"""GET /v1/prices/spot — spot price time series."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection, nem_naive_to_utc, utc_to_nem_naive

router = APIRouter()

VALID_REGIONS = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


def _utc_iso(dt: datetime) -> str:
    return nem_naive_to_utc(dt).isoformat().replace("+00:00", "Z")


@router.get("/prices/spot")
async def spot_price(
    region: str = Query(..., min_length=2, max_length=8),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    resolution: str = Query("auto"),
) -> dict:
    region = region.upper()
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REGION", "message": f"Unknown region: {region}"},
        )

    # Default window: last 24h ending NOW (real time, in UTC).
    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(hours=24))
    )

    # DuckDB rows are NEM-naive; convert query params to NEM-naive too.
    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT settlementdate, regionid, rrp
            FROM prices5
            WHERE regionid = ?
              AND settlementdate >= ?
              AND settlementdate <= ?
            ORDER BY settlementdate
            """,
            [region, from_nem, to_nem],
        ).fetchall()
    finally:
        conn.close()

    data = [
        {"timestamp": _utc_iso(r[0]), "region": r[1], "price": r[2]}
        for r in rows
    ]

    return {
        "data": data,
        "meta": {
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "resolution": "5min",
            "downsampled": False,
            "source_rows": len(data),
            "returned_rows": len(data),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }
