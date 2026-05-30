"""Live web dashboard for the telemetry lake — a read-only Quack client.

Zero external dependencies: just Python's stdlib http.server + duckdb. The
dashboard ATTACHes to the same Quack server the producers write to, and runs
every aggregation through remote.query(...) so the work happens SERVER-SIDE.
Only small result sets travel over the wire — not the raw rows.

    python dashboard.py
    # then open http://127.0.0.1:8800

The page polls /api/metrics ~once a second and redraws. You watch the lake
fill up live while N producer processes fan in behind it.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

import config


# A single client connection, shared across request threads behind a lock.
_con = None
_lock = threading.Lock()


def get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        c = duckdb.connect()
        c.execute("LOAD quack;")
        c.execute("CREATE SECRET (TYPE quack, TOKEN ?);", [config.QUACK_TOKEN])
        c.execute(
            f"ATTACH '{config.QUACK_URI}' AS remote (DISABLE_SSL {str(config.DISABLE_SSL).lower()});"
        )
        _con = c
    return _con


def rq(inner_sql: str):
    """Run inner_sql SERVER-SIDE via remote.query and fetch the small result.

    Dollar-quoting ($q$...$q$) lets the inner SQL contain single quotes
    (e.g. date_trunc('second', ...)) without escaping gymnastics.
    """
    sql = f"FROM remote.query($q${inner_sql}$q$)"
    with _lock:
        cur = get_con().execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def collect_metrics() -> dict:
    total = rq("SELECT count(*) AS n FROM telemetry")[0]["n"]

    by_device = rq("""
        SELECT device_id, count(*) AS events
        FROM telemetry GROUP BY device_id ORDER BY events DESC
    """)
    by_region = rq("""
        SELECT region, count(*) AS events
        FROM telemetry GROUP BY region ORDER BY events DESC
    """)
    by_metric = rq("""
        SELECT metric,
               round(avg(value), 1) AS avg_v,
               round(min(value), 1) AS min_v,
               round(max(value), 1) AS max_v
        FROM telemetry
        WHERE ingest_ts > now() - INTERVAL 30 SECOND
        GROUP BY metric ORDER BY metric
    """)
    # Per-second ingest throughput over the last ~20s (the fan-in curve).
    throughput = rq("""
        SELECT CAST(epoch(date_trunc('second', ingest_ts)) AS BIGINT) AS sec,
               count(*) AS events
        FROM telemetry
        WHERE ingest_ts > now() - INTERVAL 20 SECOND
        GROUP BY sec ORDER BY sec
    """)
    # End-to-end lag: server ingest_ts minus device event_ts, in ms.
    lag = rq("""
        SELECT round(avg(epoch(ingest_ts) - epoch(event_ts)) * 1000, 1) AS lag_ms,
               count(DISTINCT device_id) AS active_devices
        FROM telemetry
        WHERE ingest_ts > now() - INTERVAL 5 SECOND
    """)[0]
    recent = rq("""
        SELECT device_id, region, metric, value,
               strftime(event_ts, '%H:%M:%S') AS t
        FROM telemetry ORDER BY ingest_ts DESC LIMIT 12
    """)

    inst = throughput[-1]["events"] if throughput else 0
    return {
        "total": total,
        "instant_rate": inst,
        "active_devices": lag.get("active_devices") or 0,
        "lag_ms": lag.get("lag_ms"),
        "by_device": by_device,
        "by_region": by_region,
        "by_metric": by_metric,
        "throughput": throughput,
        "recent": recent,
    }


INDEX_HTML = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><title>Quack Telemetry Lake</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{--bg:#0b1020;--card:#141a30;--ink:#e6ecff;--mut:#8aa0c8;--acc:#37d3a0;--acc2:#5b8cff;}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--ink);
    font:14px/1.5 ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial}
  header{padding:18px 24px;border-bottom:1px solid #1f2a4a;display:flex;
    align-items:baseline;gap:14px;flex-wrap:wrap}
  h1{font-size:18px;margin:0} .sub{color:var(--mut)}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--acc);
    display:inline-block;margin-right:6px;box-shadow:0 0 8px var(--acc)}
  .wrap{padding:20px 24px;display:grid;gap:16px;
    grid-template-columns:repeat(4,1fr)}
  .card{background:var(--card);border:1px solid #1f2a4a;border-radius:12px;padding:16px}
  .kpi .big{font-size:34px;font-weight:700;letter-spacing:-1px}
  .kpi .lbl{color:var(--mut);font-size:12px;text-transform:uppercase;letter-spacing:.5px}
  .col-2{grid-column:span 2} .col-4{grid-column:span 4}
  .bar{height:22px;background:linear-gradient(90deg,var(--acc2),var(--acc));
    border-radius:5px;min-width:2px}
  .row{display:grid;grid-template-columns:120px 1fr 64px;align-items:center;gap:10px;margin:6px 0}
  .row .v{text-align:right;color:var(--mut);font-variant-numeric:tabular-nums}
  table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
  th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #1f2a4a}
  th{color:var(--mut);font-weight:600;font-size:12px}
  .spark{display:flex;align-items:flex-end;gap:3px;height:90px}
  .spark .s{flex:1;background:linear-gradient(180deg,var(--acc2),var(--acc));
    border-radius:3px 3px 0 0;min-height:2px}
  h3{margin:0 0 10px;font-size:13px;color:var(--mut);text-transform:uppercase;letter-spacing:.5px}
  code{background:#0d1428;padding:1px 6px;border-radius:5px;color:var(--acc)}
</style></head><body>
<header>
  <h1><span class="dot"></span>Quack Telemetry Lake</h1>
  <span class="sub">N writer processes → one DuckDB → <code>remote.query()</code> server-side aggregation</span>
  <span class="sub" id="clock" style="margin-left:auto"></span>
</header>
<div class="wrap">
  <div class="card kpi"><div class="lbl">Total events</div><div class="big" id="total">—</div></div>
  <div class="card kpi"><div class="lbl">Ingest / sec</div><div class="big" id="rate">—</div></div>
  <div class="card kpi"><div class="lbl">Active devices (5s)</div><div class="big" id="dev">—</div></div>
  <div class="card kpi"><div class="lbl">Ingest lag</div><div class="big" id="lag">—</div></div>

  <div class="card col-4"><h3>Ingest throughput — last 20s</h3><div class="spark" id="spark"></div></div>

  <div class="card col-2"><h3>Events per device (fan-in)</h3><div id="byDevice"></div></div>
  <div class="card col-2"><h3>Events per region</h3><div id="byRegion"></div></div>

  <div class="card col-2"><h3>Live metric rollup — last 30s</h3>
    <table id="byMetric"><thead><tr><th>metric</th><th>avg</th><th>min</th><th>max</th></tr></thead><tbody></tbody></table>
  </div>
  <div class="card col-2"><h3>Most recent events</h3>
    <table id="recent"><thead><tr><th>time</th><th>device</th><th>region</th><th>metric</th><th>value</th></tr></thead><tbody></tbody></table>
  </div>
</div>
<script>
const $=id=>document.getElementById(id);
function bars(el,data,key,valKey){
  const max=Math.max(1,...data.map(d=>d[valKey]));
  el.innerHTML=data.map(d=>`<div class="row"><div>${d[key]}</div>
    <div class="bar" style="width:${Math.max(2,100*d[valKey]/max)}%"></div>
    <div class="v">${d[valKey].toLocaleString()}</div></div>`).join('');
}
async function tick(){
  try{
    const m=await (await fetch('/api/metrics')).json();
    $('total').textContent=m.total.toLocaleString();
    $('rate').textContent=m.instant_rate.toLocaleString();
    $('dev').textContent=m.active_devices;
    $('lag').textContent=(m.lag_ms==null?'—':m.lag_ms+' ms');
    bars($('byDevice'),m.by_device,'device_id','events');
    bars($('byRegion'),m.by_region,'region','events');
    const tmax=Math.max(1,...m.throughput.map(d=>d.events));
    $('spark').innerHTML=m.throughput.map(d=>
      `<div class="s" style="height:${100*d.events/tmax}%" title="${d.events}/s"></div>`).join('');
    $('byMetric').querySelector('tbody').innerHTML=m.by_metric.map(r=>
      `<tr><td>${r.metric}</td><td>${r.avg_v??'—'}</td><td>${r.min_v??'—'}</td><td>${r.max_v??'—'}</td></tr>`).join('');
    $('recent').querySelector('tbody').innerHTML=m.recent.map(r=>
      `<tr><td>${r.t}</td><td>${r.device_id}</td><td>${r.region}</td><td>${r.metric}</td><td>${r.value}</td></tr>`).join('');
    $('clock').textContent='updated '+new Date().toLocaleTimeString();
  }catch(e){$('clock').textContent='waiting for server…';}
}
setInterval(tick,1000); tick();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet — don't spam the console per request
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/metrics"):
            try:
                body = json.dumps(collect_metrics(), default=str).encode()
                self._send(200, body, "application/json")
            except Exception as e:
                self._send(503, json.dumps({"error": str(e)}).encode(), "application/json")
        else:
            self._send(200, INDEX_HTML.encode(), "text/html; charset=utf-8")


def main() -> None:
    srv = ThreadingHTTPServer((config.DASHBOARD_HOST, config.DASHBOARD_PORT), Handler)
    url = f"http://{config.DASHBOARD_HOST}:{config.DASHBOARD_PORT}"
    print(f"[dashboard] live at {url}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
