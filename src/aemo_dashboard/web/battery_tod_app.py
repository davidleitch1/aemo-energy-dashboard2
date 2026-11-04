"""Standalone test server for the battery time-of-day view.

Run from src/aemo_dashboard/web:
    uvicorn battery_tod_app:app --port 5097
then open http://192.168.68.71:5097/
"""
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

import battery_tod as bt

app = FastAPI()

CSS = """
:root { --paper:#fffcf0; --ink:#100f0f; --muted:#878580; --border:#e6e4d9;
        --accent:#5e409d; }
body { margin:0; background:var(--paper); color:var(--ink);
       font-family: Inter, -apple-system, sans-serif; }
main { max-width:1200px; margin:0 auto; padding:16px; }
h1 { font-size:20px; font-weight:600; margin:4px 0 12px; }
.controls { display:flex; flex-wrap:wrap; gap:16px; margin-bottom:10px; }
.group { display:flex; gap:4px; align-items:center; }
.group .lbl { color:var(--muted); font-size:12px; margin-right:4px; }
.pill { border:1px solid var(--border); background:var(--paper);
        color:var(--ink); padding:4px 11px; border-radius:14px; font-size:13px;
        cursor:pointer; text-decoration:none; }
.pill.active { background:var(--accent); border-color:var(--accent);
               color:#fffcf0; }
.sub, .note { color:var(--muted); font-size:12px; margin:6px 0; }
.strip { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
         gap:8px; margin:10px 0 4px; }
.tile { border:1px solid var(--border); border-radius:6px; padding:8px 10px; }
.tile .k { color:var(--muted); font-size:11px; }
.tile .v { font-size:17px; font-weight:600; margin-top:2px; }
.placeholder { padding:40px; text-align:center; color:var(--muted); }
.htmx-request #content { opacity:.5; }
"""


def _pills(name: str, options, active: str, region: str, rng: str) -> str:
    out = []
    for slug, label in options:
        r, g = (slug, rng) if name == "region" else (region, slug)
        cls = "pill active" if slug == active else "pill"
        out.append(f'<a class="{cls}" href="/?region={r}&range={g}" '
                   f'hx-get="/content?region={r}&range={g}" '
                   f'hx-target="#body" hx-push-url="/?region={r}&range={g}">'
                   f'{label}</a>')
    return "".join(out)


def _body(region: str, rng: str) -> str:
    return (
        '<div class="controls">'
        f'<div class="group"><span class="lbl">Region</span>'
        f'{_pills("region", bt.REGIONS, region, region, rng)}</div>'
        f'<div class="group"><span class="lbl">Period</span>'
        f'{_pills("range", bt.RANGES, rng, region, rng)}</div></div>'
        f'<div id="content">{bt.content_html(region, rng)}</div>'
    )


def _clean(region: str, rng: str) -> tuple[str, str]:
    region = region if region in dict(bt.REGIONS) else "NEM"
    rng = rng if rng in dict(bt.RANGES) else "30d"
    return region, rng


@app.get("/", response_class=HTMLResponse)
def page(region: str = Query("NEM"), range: str = Query("30d")):
    region, rng = _clean(region, range)
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Battery time of day</title>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>{CSS}</style></head><body><main>
<h1>Battery charge and discharge by time of day</h1>
<div id="body">{_body(region, rng)}</div>
</main></body></html>"""


@app.get("/content", response_class=HTMLResponse)
def content(region: str = Query("NEM"), range: str = Query("30d")):
    region, rng = _clean(region, range)
    return _body(region, rng)
