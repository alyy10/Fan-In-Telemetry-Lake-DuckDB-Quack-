"""One command to see the whole thing: server + N producers + live dashboard.

    python run_demo.py                 # 4 producers, opens the dashboard
    python run_demo.py --producers 8 --rate 25 --no-browser

It starts the Quack server, waits until it's actually serving, fans in several
producer processes (each its own OS process — real concurrent writers), then
launches the dashboard and opens your browser. Ctrl+C tears everything down.
"""
import argparse
import os
import subprocess
import sys
import time
import webbrowser

import config

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def spawn(script, *script_args):
    return subprocess.Popen(
        [PY, os.path.join(HERE, script), *map(str, script_args)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--producers", type=int, default=4)
    ap.add_argument("--rate", type=float, default=15.0, help="events/sec per producer")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--keep-db", action="store_true", help="don't wipe telemetry.db first")
    args = ap.parse_args()

    if not args.keep_db:
        for suffix in ("", ".wal"):
            p = config.DB_PATH + suffix
            if os.path.exists(p):
                os.remove(p)

    procs = []
    try:
        # 1) Server — wait for the SERVER_UP handshake before anything attaches.
        print("[demo] starting Quack server...")
        server = spawn("server.py")
        procs.append(("server", server))
        for line in server.stdout:
            print("   ", line.rstrip())
            if "SERVER_UP" in line:
                break
        time.sleep(0.5)

        # 2) Producers — the fan-in. Each gets a distinct identity.
        print(f"[demo] starting {args.producers} producers...")
        for i in range(args.producers):
            dev = f"dev-{i+1:02d}"
            region = config.REGIONS[i % len(config.REGIONS)]
            p = spawn("producer.py", "--device", dev, "--region", region, "--rate", args.rate)
            procs.append((dev, p))

        # 3) Dashboard.
        print("[demo] starting dashboard...")
        dash = spawn("dashboard.py")
        procs.append(("dashboard", dash))
        time.sleep(1.0)
        url = f"http://{config.DASHBOARD_HOST}:{config.DASHBOARD_PORT}"
        print(f"\n[demo] ✅ up. Dashboard: {url}")
        print("[demo] press Ctrl+C to stop everything.\n")
        if not args.no_browser:
            webbrowser.open(url)

        # Drain the server's heartbeat to the console so you see ingest rate.
        for line in server.stdout:
            print("   ", line.rstrip())

    except KeyboardInterrupt:
        print("\n[demo] shutting down...")
    finally:
        for name, p in reversed(procs):
            if p.poll() is None:
                p.terminate()
        for name, p in reversed(procs):
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()
        print("[demo] all processes stopped.")


if __name__ == "__main__":
    main()
