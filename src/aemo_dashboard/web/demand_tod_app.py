"""Standalone test server for the demand time-of-day view.

Run from src/aemo_dashboard/web:
    uvicorn demand_tod_app:app --host 0.0.0.0 --port 5096
then open http://192.168.68.71:5096/
"""
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse

import demand_tod as dt

app = FastAPI()

CSS = """
:root { --paper:#fffcf0; --ink:#100f0f; --muted:#878580; --border:#e6e4d9;
        --accent:#205ea6; }
body { margin:0; background:var(--paper); color:var(--ink);
       font-family: Inter, -apple-system, sans-serif; }
main { max-width:1280px; margin:0 auto; padding:16px; }
h1 { font-size:20px; font-weight:600; margin:4px 0 12px; }
form { display:flex; flex-wrap:wrap; gap:10px 20px; margin-bottom:8px;
       align-items:center; }
.group { display:flex; gap:4px; align-items:center; flex-wrap:wrap; }
.group .lbl { color:var(--muted); font-size:12px; margin-right:4px; }
.pill { border:1px solid var(--border); padding:4px 11px; border-radius:14px;
        font-size:13px; cursor:pointer; user-select:none; }
.pill input { display:none; }
.pill:has(input:checked) { background:var(--accent); border-color:var(--accent);
                           color:#fffcf0; }
.chk { font-size:13px; display:flex; gap:5px; align-items:center;
       cursor:pointer; }
.sub, .note { color:var(--muted); font-size:12px; margin:6px 0; }
.note { line-height:1.45; }
table.summ { border-collapse:collapse; font-size:13px; margin:10px 0;
             font-variant-numeric: tabular-nums; }
table.summ th { color:var(--muted); font-weight:500; font-size:11px;
                text-align:right; padding:4px 12px;
                border-bottom:1px solid var(--border); }
table.summ td { text-align:right; padding:4px 12px;
                border-bottom:1px solid var(--border); }
table.summ td:first-child { text-align:left; }
table.summ .u { color:var(--muted); font-size:11px; }
.placeholder { padding:40px; text-align:center; color:var(--muted); }
.htmx-request#content, .htmx-request #content { opacity:.5; }
@media (max-width:700px) { table.summ { font-size:11px; }
  table.summ th, table.summ td { padding:3px 5px; } }
"""

BASES = [("op", "Operational demand"),
         ("total", "Total demand (incl. rooftop PV)")]
ADDERS = [("chg", "Include battery charging"),
          ("pump", "Include pump load"),
          ("dis", "Subtract battery discharge")]


def _parse(region, rng, base, chg, pump, dis, v):
    region = region if region in dict(dt.REGIONS) else "NEM"
    rng = rng if rng in dict(dt.RANGES) else "30d"
    if not v:   # first visit: defaults
        return region, rng, ["op", "total"], True, False, False
    bases = [b for b in (base or []) if b in dict(BASES)] or ["op"]
    return region, rng, bases, bool(chg), bool(pump), bool(dis)


def _form(region, rng, bases, flags) -> str:
    def radios(name, opts, active):
        return "".join(
            f'<label class="pill"><input type="radio" name="{name}" '
            f'value="{s}"{" checked" if s == active else ""}>{l}</label>'
            for s, l in opts)
    base_ck = "".join(
        f'<label class="chk"><input type="checkbox" name="base" value="{s}"'
        f'{" checked" if s in bases else ""}>{l}</label>' for s, l in BASES)
    add_ck = "".join(
        f'<label class="chk"><input type="checkbox" name="{s}" value="1"'
        f'{" checked" if flags[s] else ""}>{l}</label>' for s, l in ADDERS)
    return (
        '<form hx-get="/" hx-target="#content" hx-trigger="change" '
        'hx-push-url="true" hx-indicator="#content">'
        '<input type="hidden" name="v" value="1">'
        f'<div class="group"><span class="lbl">Region</span>'
        f'{radios("region", dt.REGIONS, region)}</div>'
        f'<div class="group"><span class="lbl">Period</span>'
        f'{radios("range", dt.RANGES, rng)}</div>'
        f'<div class="group"><span class="lbl">Demand</span>{base_ck}</div>'
        f'<div class="group"><span class="lbl">Storage</span>{add_ck}</div>'
        '</form>')


@app.get("/", response_class=HTMLResponse)
def page(request: Request, region: str = Query("NEM"),
         range: str = Query("30d"), base: list[str] | None = Query(None),
         chg: str | None = None, pump: str | None = None,
         dis: str | None = None, v: str | None = None):
    region, rng, bases, c, p, d = _parse(region, range, base, chg, pump,
                                         dis, v)
    content = dt.content_html(region, rng, bases, c, p, d)
    if request.headers.get("HX-Request"):
        return content
    flags = {"chg": c, "pump": p, "dis": d}
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Demand time of day</title>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>{CSS}</style></head><body><main>
<h1>Demand by time of day, this year against last</h1>
{_form(region, rng, bases, flags)}
<div id="content">{content}</div>
</main></body></html>"""
