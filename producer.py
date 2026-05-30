"""Telemetry producer — one simulated device/service emitting readings.

This is a Quack *client*. It runs in its own process and ATTACHes to the
server. The whole point of the demo: you can start as MANY of these as you
like, all writing into the same DuckDB at the same time. That's the thing
embedded DuckDB could never do.

    python producer.py --device dev-01 --region us-east --rate 20

--rate is approximate events per second. Each tick inserts a small batch,
which is the Quack sweet spot (moderate-frequency analytical writes).
"""
import argparse
import math
import random
import time
from datetime import datetime

import duckdb

import config


def connect_client() -> duckdb.DuckDBPyConnection:
    """A fresh in-memory client that treats the remote lake like a local table."""
    con = duckdb.connect()
    con.execute("LOAD quack;")
    con.execute("CREATE SECRET (TYPE quack, TOKEN ?);", [config.QUACK_TOKEN])
    con.execute(
        f"ATTACH '{config.QUACK_URI}' AS remote (DISABLE_SSL {str(config.DISABLE_SSL).lower()});"
    )
    return con


def synth_value(metric: str, t: float) -> float:
    """Make each metric look plausible: a baseline + slow wave + jitter."""
    base = {
        "cpu_pct": 45, "mem_pct": 60, "temp_c": 38,
        "req_latency_ms": 120, "disk_io_mbps": 80,
    }[metric]
    amp = {
        "cpu_pct": 30, "mem_pct": 20, "temp_c": 8,
        "req_latency_ms": 90, "disk_io_mbps": 50,
    }[metric]
    wave = amp * math.sin(t / 7.0)
    jitter = random.uniform(-amp * 0.25, amp * 0.25)
    return round(max(0.0, base + wave + jitter), 2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--device", required=True, help="device id, e.g. dev-01")
    ap.add_argument("--region", default=random.choice(config.REGIONS))
    ap.add_argument("--rate", type=float, default=15.0, help="approx events/sec")
    ap.add_argument("--seconds", type=float, default=0.0, help="0 = run forever")
    args = ap.parse_args()

    con = connect_client()
    print(f"[{args.device}] attached to {config.QUACK_URI} ({args.region}) @ ~{args.rate}/s", flush=True)

    batch_interval = 0.5                       # write twice a second
    per_batch = max(1, round(args.rate * batch_interval))
    start = time.time()
    sent = 0

    # now() stamps the ingest time at write; event_ts is the device's own clock.
    insert_sql = (
        "INSERT INTO remote.telemetry (device_id, region, metric, value, event_ts, ingest_ts) "
        "VALUES (?, ?, ?, ?, ?, now())"
    )

    try:
        while True:
            t = time.time() - start
            rows = []
            for _ in range(per_batch):
                metric = random.choice(config.METRICS)
                rows.append(
                    [args.device, args.region, metric, synth_value(metric, t), datetime.now()]
                )
            # executemany sends the batch; the server serializes the commit.
            con.executemany(insert_sql, rows)
            sent += len(rows)

            if args.seconds and t >= args.seconds:
                break
            time.sleep(batch_interval)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[{args.device}] done — sent {sent} events", flush=True)


if __name__ == "__main__":
    main()
