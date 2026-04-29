"""GET /v1/prices/spot — spot price time series."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection

router = APIRouter()

VALID_REGIONS = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


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

    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    if to is None:
        to = now_naive
    else:
        to = to.replace(tzinfo=None) if to.tzinfo else to
    if from_ is None:
        from_ = to - timedelta(hours=24)
    else:
        from_ = from_.replace(tzinfo=None) if from_.tzinfo else from_

    conn = get_connection()
    rows = conn.execute(
        """
        SELECT settlementdate, regionid, rrp
        FROM prices5
        WHERE regionid = ?
          AND settlementdate >= ?
          AND settlementdate <= ?
        ORDER BY settlementdate
        """,
        [region, from_, to],
    ).fetchall()

    data = [
        {"timestamp": r[0].isoformat() + "Z", "region": r[1], "price": r[2]}
        for r in rows
    ]

    return {
        "data": data,
        "meta": {
            "from": from_.isoformat() + "Z",
            "to": to.isoformat() + "Z",
            "resolution": "5min",
            "downsampled": False,
            "source_rows": len(data),
            "returned_rows": len(data),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }
