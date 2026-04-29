# AEMO Mobile API — Subdirectory Guide

FastAPI read-only service powering the Nem Analyst iPhone app.

## Run locally

```sh
cd /Users/davidleitch/aemo_production/aemo-energy-dashboard2
source .venv/bin/activate
PYTHONPATH=src uvicorn aemo_dashboard.api.main:app --host 127.0.0.1 --port 8002 --reload
```

## Tests

```sh
cd /Users/davidleitch/aemo_production/aemo-energy-dashboard2
PYTHONPATH=src .venv/bin/pytest tests/api/
```

## Endpoints

- `GET /v1/meta/freshness`
- `GET /v1/prices/spot?region=NSW1[&from=...&to=...&resolution=auto]`

Future endpoints catalogued in §4.3 of the implementation plan (47 total).

## Auth

Bearer token in `Authorization: Bearer <token>` header. Tokens loaded from `$API_TOKENS_FILE` (YAML mapping `token: name`). If unset, any non-empty token is accepted (dev mode).

Cloudflare Access service-token pair is the outer envelope (`CF-Access-Client-Id` / `CF-Access-Client-Secret`); FastAPI never sees an unauthenticated request.

## DB

Read-only DuckDB at `/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb`. Override with `$AEMO_DUCKDB_PATH`.

## TDD discipline

Every new endpoint follows: shape test → 🔴 → implement → 🟢. Tests in `tests/api/`. CI runs pytest on push.

## Plan

`~/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/ios_app_research/implementation_plan.md` — §4 (backend) and §10 (milestones).
