#!/bin/bash
# Run the HTMX + Plotly.js web dashboard (new redesign).
# Reuses the same .venv as the rest of the dashboard.
# Binds 0.0.0.0:8090 so it's reachable from anywhere on the LAN.
# For strict localhost: ssh -L 8090:127.0.0.1:8090 <host>, then localhost:8090.
cd "$(dirname "$0")"
exec .venv/bin/uvicorn aemo_dashboard.web.app:app \
    --host 0.0.0.0 --port 8090 --log-level warning
