"""Web dashboard module — HTMX + Plotly.js + FastAPI front end.

Runs alongside the iOS-facing JSON API in `aemo_dashboard.api`. Both share
the same DuckDB at `aemo_readonly.duckdb` and the same upstream collectors.

Entry point: `aemo_dashboard.web.app:app` (FastAPI instance). Run with
`run_web_dashboard.sh` at the repo root, or directly via
`uvicorn aemo_dashboard.web.app:app --port 8090`.
"""
