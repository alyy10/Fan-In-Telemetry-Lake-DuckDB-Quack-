# Fan-In-Telemetry-Lake-DuckDB-Quack-
A live telemetry lake where many independent processes write into one DuckDB database at the same time, with a real-time web dashboard reading off the same store.
This is the thing classic embedded DuckDB *cannot* do — a single process
holds the write lock on the file. [Quack](https://duckdb.org/quack/) (DuckDB's
client-server protocol, beta in v1.5.2) flips it around: one DuckDB process
becomes a server that serializes writes, and any number of client processes
attach over HTTP and write concurrently.

<img width="1000" height="400" alt="t" src="https://github.com/user-attachments/assets/a06654b5-f208-4fa8-9981-36b185e2d255" />

## Run it

Requires `duckdb` (v1.5.2+) — already includes the Quack extension, which
auto-installs on first use. No other dependencies; the dashboard is pure
Python stdlib.

```bash
# one command: server + 4 producers + dashboard, opens your browser
python run_demo.py

# scale the fan-in up
python run_demo.py --producers 8 --rate 25
```

Then watch http://127.0.0.1:8800 — total events, ingest/sec, the per-second
throughput curve, per-device and per-region fan-in bars, a live metric rollup,
and the most recent events streaming in.
<img width="1600" height="687" alt="lnkdn2" src="https://github.com/user-attachments/assets/721c31e8-e0bf-429d-9979-417213912c38" />
<img width="1600" height="553" alt="lnkdn3" src="https://github.com/user-attachments/assets/4aa3fbfd-282a-4ec2-8e7a-6218caee088a" />

### Or run the pieces by hand (best way to *see* the concurrency)

Open four terminals:

```bash
# terminal 1 — the server (owns the file, serializes writes)
python server.py

# terminals 2 & 3 — two independent writer processes
python producer.py --device dev-01 --region us-east   --rate 20
python producer.py --device dev-02 --region eu-central --rate 20

# terminal 4 — the dashboard (a read-only Quack client)
python dashboard.py
```

Two separate OS processes writing to one DuckDB file, live. Kill a producer,
start three more — the lake just keeps filling.

## How it works

| File | Role |
|------|------|
| `config.py`   | Shared URI (`quack:localhost`), token, db path, dashboard port. |
| `server.py`   | Opens `telemetry.db`, calls `quack_serve(...)` (non-blocking), stays alive, prints an ingest-rate heartbeat. The **single writer-of-record**. |
| `producer.py` | A Quack **client**. `ATTACH`es the server and `INSERT`s batches. Run as many as you like — that's the fan-in. |
| `dashboard.py`| A read-only Quack client + stdlib `http.server`. Every metric is computed with `remote.query(...)` so aggregation happens **server-side** and only small results cross the wire. |
| `run_demo.py` | Orchestrates the whole thing as child processes. |

### The data

```sql
telemetry(device_id, region, metric, value, event_ts, ingest_ts)
```

`event_ts` is the device's own clock; `ingest_ts` is stamped with `now()` at
write time. The dashboard's "ingest lag" is `ingest_ts - event_ts`.

## Quack gotchas this project hit (so you don't have to)

These cost real debugging time and are worth knowing — Quack is **beta**:

1. **`quack_serve` named args don't bind from prepared-statement `?`.**
   `CALL quack_serve(?, disable_ssl=?, ...)` silently leaves SSL **on**, so
   plain-HTTP clients then fail to attach. Build the `CALL` as a literal string
   instead (see `server.py`). For localhost we pass `disable_ssl=true`; in
   production put TLS in front via a reverse proxy.

2. **Column `DEFAULT` expressions break client `ATTACH`.**
   A served table with `DEFAULT nextval(...)` or `DEFAULT now()` makes the
   client fail catalog sync during `ATTACH` with a *misleading*
   `Catalog "remote" does not exist!`. Fix: plain columns, no defaults — compute
   those values inside the `INSERT` (`..., now())`).

3. **You can't drive a server-side `SEQUENCE` from a client INSERT.**
   `nextval('seq')` (even qualified `remote.seq`) resolves against the *client's*
   catalog and errors. We dropped the sequence and order "recent" by `ingest_ts`.

## Where this maps in the real world

This is the architecture people often over-build with Kafka + a warehouse when
the write volume is *moderate and analytical*: multi-process app/service
telemetry, IoT edge collection, parallel job instrumentation. Quack's sweet spot
is exactly that — not microsecond OLTP.

