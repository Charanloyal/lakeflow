#!/usr/bin/env python3
"""
Register Debezium PostgreSQL CDC Source Connector with Kafka Connect
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "debezium", "postgres-source-connector.json"
)

def register_connector():
    print(f"Connecting to Kafka Connect at {CONNECT_URL}...")
    if not os.path.exists(CONNECTOR_CONFIG_PATH):
        print(f"ERROR: Connector config not found at {CONNECTOR_CONFIG_PATH}")
        sys.exit(1)

    with open(CONNECTOR_CONFIG_PATH, "r") as f:
        payload = json.load(f)

    connector_name = payload.get("name", "lakeflow-postgres-source")
    target_url = f"{CONNECT_URL}/connectors/{connector_name}/config"

    # Wait for Kafka Connect to be healthy
    max_retries = 20
    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(f"{CONNECT_URL}/connectors")
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status == 200:
                    print("Kafka Connect is online and responsive.")
                    break
        except Exception:
            print(f"Waiting for Kafka Connect to become ready (attempt {attempt}/{max_retries})...")
            time.sleep(3)
    else:
        print("ERROR: Kafka Connect is unreachable. Please verify container status.")
        sys.exit(1)

    # Submit or update connector config (PUT idempotent)
    req = urllib.request.Request(
        target_url,
        data=json.dumps(payload["config"]).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT"
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"SUCCESS: Connector '{connector_name}' registered/updated!")
            print(json.dumps(data, indent=2))
            return 0
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        print(f"HTTP Error {e.code}: {err_msg}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(register_connector())
