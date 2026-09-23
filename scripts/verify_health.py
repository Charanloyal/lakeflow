#!/usr/bin/env python3
"""
Automated Health Verification Probes for Data Platform Lab
Tests network socket and HTTP health endpoints across all 12 services.
"""

import socket
import sys
import time
import urllib.request
import urllib.error

SERVICES = [
    {"name": "PostgreSQL", "type": "socket", "host": "localhost", "port": 5432},
    {"name": "Apache Kafka (KRaft)", "type": "socket", "host": "localhost", "port": 9092},
    {"name": "Kafka Connect (Debezium)", "type": "http", "url": "http://localhost:8083/connectors"},
    {"name": "Apache Spark Master UI", "type": "http", "url": "http://localhost:8080"},
    {"name": "Apache Spark Worker UI", "type": "http", "url": "http://localhost:8081"},
    {"name": "MinIO S3 API", "type": "http", "url": "http://localhost:9000/minio/health/live"},
    {"name": "MinIO Web Console", "type": "http", "url": "http://localhost:9001"},
    {"name": "Trino SQL Engine", "type": "http", "url": "http://localhost:8082/v1/info"},
    {"name": "Redis Store", "type": "socket", "host": "localhost", "port": 6379},
    {"name": "Apache Airflow", "type": "http", "url": "http://localhost:8888/health"},
    {"name": "Prometheus Metrics", "type": "http", "url": "http://localhost:9090/-/healthy"},
    {"name": "Grafana Dashboards", "type": "http", "url": "http://localhost:3000/api/health"},
]

def check_socket(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def check_http(url: str, timeout: float = 4.0) -> tuple[bool, int]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "DataPlatformLab-HealthProbe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status in (200, 204), response.status
    except urllib.error.HTTPError as e:
        return e.code in (200, 204), e.code
    except Exception:
        return False, 0

def main():
    print("=" * 80)
    print(" DATA PLATFORM LAB - SERVICE HEALTH AUDIT")
    print("=" * 80)
    print(f"{'SERVICE':<30} | {'TYPE':<8} | {'TARGET':<30} | {'STATUS'}")
    print("-" * 80)

    healthy_count = 0
    total = len(SERVICES)

    for svc in SERVICES:
        start_time = time.time()
        if svc["type"] == "socket":
            target = f"{svc['host']}:{svc['port']}"
            ok = check_socket(svc["host"], svc["port"])
        else:
            target = svc["url"]
            ok, _ = check_http(svc["url"])

        elapsed_ms = (time.time() - start_time) * 1000
        status_str = f"ONLINE ({elapsed_ms:.1f}ms)" if ok else "OFFLINE"
        if ok:
            healthy_count += 1

        print(f"{svc['name']:<30} | {svc['type']:<8} | {target:<30} | {status_str}")

    print("=" * 80)
    print(f"SUMMARY: {healthy_count}/{total} services healthy.")
    print("=" * 80)

    return 0 if healthy_count == total else 1

if __name__ == "__main__":
    sys.exit(main())
