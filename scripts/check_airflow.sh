#!/usr/bin/env bash
set -euo pipefail

OUT1="diagnostics/airflow_status.txt"
OUT2="/tmp/airflow_status.txt"
mkdir -p diagnostics

{
  echo "=== Timestamp ==="
  date

  echo "\n=== docker compose ps ==="
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then
      docker compose -f compose.yaml ps || true
    elif command -v docker-compose >/dev/null 2>&1; then
      docker-compose -f compose.yaml ps || true
    else
      echo 'docker compose not available'
    fi
  else
    echo 'docker not installed'
  fi

  echo "\n=== docker ps (airflow containers) ==="
  if command -v docker >/dev/null 2>&1; then
    docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' | grep -i airflow || true
  fi

  echo "\n=== airflow-api-server logs (last 200 lines) ==="
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then
      docker compose -f compose.yaml logs --tail 200 airflow-api-server 2>/dev/null || echo 'no logs or service not present'
    elif command -v docker-compose >/dev/null 2>&1; then
      docker-compose -f compose.yaml logs --tail 200 airflow-api-server 2>/dev/null || echo 'no logs or service not present'
    fi
  fi

  echo "\n=== listeners on port 8080 ==="
  ss -tuln | grep -E ':8080\b' || true
  if command -v lsof >/dev/null 2>&1; then
    lsof -i :8080 || true
  fi

  echo "\n=== processes mentioning airflow/gunicorn/uvicorn ==="
  ps aux | grep -E 'airflow|gunicorn|uvicorn' | grep -v grep || true

  echo "\n=== cgroup of this process ==="
  cat /proc/self/cgroup || true

  echo "\n=== End of diagnostics ==="
} | tee "$OUT1" | tee "$OUT2" >/dev/null

echo "Wrote diagnostics to $OUT1 and $OUT2"
