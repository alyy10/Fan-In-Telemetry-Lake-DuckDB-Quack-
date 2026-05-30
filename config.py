"""Shared configuration for the Quack fan-in telemetry lake.

Everything (server, producers, dashboard) imports from here so the URI,
token, and on-disk database path never drift between processes.
"""
import os

# --- Quack connection -------------------------------------------------------
# The server listens on port 9494 by default (Quack's default — the year
# Netscape Navigator was released). The URI is what clients ATTACH to.
QUACK_URI = "quack:localhost"
QUACK_TOKEN = "telemetry_demo_token"

# Local demo only: skip TLS. In production you'd put nginx + Let's Encrypt in
# front of the server (see the DuckDB "Securing Quack with a Reverse Proxy" doc)
# and drop DISABLE_SSL.
DISABLE_SSL = True

# --- Storage ----------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "telemetry.db")

# --- Dashboard --------------------------------------------------------------
DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = 8800

# --- Simulated fleet --------------------------------------------------------
# Each producer process picks one of these identities (round-robin in the
# orchestrator). Region + device_id end up as dimensions in the lake.
REGIONS = ["us-east", "us-west", "eu-central", "ap-south"]
METRICS = ["cpu_pct", "mem_pct", "temp_c", "req_latency_ms", "disk_io_mbps"]
