"""Quack telemetry server — the single writer-of-record.

This is the ONE process that owns telemetry.db on disk. With classic embedded
DuckDB, that file lock would block every other process from writing. Quack
flips it around: this process becomes a server, holds the mutable state, and
*serializes* writes coming from any number of remote client processes.

Run it standalone (one terminal) or let run_demo.py launch it for you:

    python server.py
"""
import time
import duckdb

import config


# NOTE on the schema: Quack (beta) cannot ATTACH a remote table whose columns
# carry DEFAULT expressions — the client fails to bind them during catalog sync
# and reports a misleading 'Catalog "remote" does not exist'. So we use a plain
# table with NO defaults and stamp ingest_ts via now() inside each INSERT.
SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry (
    device_id  VARCHAR,
    region     VARCHAR,
    metric     VARCHAR,
    value      DOUBLE,
    event_ts   TIMESTAMP,   -- when the device produced the reading
    ingest_ts  TIMESTAMP    -- stamped (now()) at write time by the producer
);
"""


def main() -> None:
    # Open the on-disk database. This connection is the serialization point:
    # every remote write funnels through the server's single writer.
    con = duckdb.connect(config.DB_PATH)
    con.execute("LOAD quack;")
    con.execute(SCHEMA)

    # Start serving. quack_serve is NON-blocking — it spins up a background
    # HTTP server thread and returns, so we keep the process alive ourselves.
    # NOTE: build the CALL literally. Named args like disable_ssl do NOT bind
    # from prepared-statement parameters — passing them as `?` silently leaves
    # SSL enabled, and clients then fail to attach over plain HTTP.
    con.execute(
        f"CALL quack_serve('{config.QUACK_URI}', "
        f"disable_ssl={str(config.DISABLE_SSL).lower()}, "
        f"token='{config.QUACK_TOKEN}');"
    )
    print("SERVER_UP", flush=True)
    print(
        f"[server] serving {config.QUACK_URI} (port 9494) -> {config.DB_PATH}",
        flush=True,
    )

    # Heartbeat: report how fast the lake is filling up from all the writers.
    last_count = 0
    last_t = time.time()
    try:
        while True:
            time.sleep(2.0)
            total = con.execute("SELECT count(*) FROM telemetry").fetchone()[0]
            now = time.time()
            rate = (total - last_count) / (now - last_t) if now > last_t else 0.0
            print(
                f"[server] rows={total:>8}  ingest={rate:7.1f}/s",
                flush=True,
            )
            last_count, last_t = total, now
    except KeyboardInterrupt:
        print("\n[server] stopping...", flush=True)
        try:
            con.execute("CALL quack_stop(?);", [config.QUACK_URI])
        except Exception:
            pass


if __name__ == "__main__":
    main()
