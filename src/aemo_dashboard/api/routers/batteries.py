"""GET /v1/batteries/* — battery dispatch & revenue analytics.

Endpoints (filled in across phases B1-B5):

  /overview          — system-wide top-N by metric (B1)
  /owners            — distinct owners across battery DUIDs (B3)
  /list              — DUIDs filtered by regions[] x owners[] (B3)
  /fleet-timeseries  — per-DUID series with LTTB downsampling (B4)
  /fleet-tod         — hour-of-day average per DUID (B5)

All numeric metrics derive from scada30 (settlementdate, duid, scadavalue)
joined to prices30 (settlementdate, regionid, rrp), with battery metadata
from duid_info (DUID, Site Name, Owner, Region, Capacity(MW), Storage(MWh),
Fuel='Battery Storage'). Discharge = scadavalue > 0; charge = scadavalue < 0.
Energy = MW * 0.5 hrs (30-min cadence).
"""
from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()
