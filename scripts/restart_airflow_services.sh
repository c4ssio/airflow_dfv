#!/bin/bash
# Restart Airflow services related to task execution
# This is useful when troubleshooting JWT token issues or other task-related problems

set -euo pipefail

echo "Restarting Airflow services..."
echo ""

# Restart worker (runs the actual tasks)
echo "🔄 Restarting airflow-worker..."
docker compose restart airflow-worker

# Restart API server (handles XCom pushes and API requests)
echo "🔄 Restarting airflow-api-server..."
docker compose restart airflow-api-server

# Optionally restart scheduler (if tasks are stuck in queued state)
echo "🔄 Restarting airflow-scheduler..."
docker compose restart airflow-scheduler

echo ""
echo "✓ Services restarted"
echo ""
echo "To view logs:"
echo "  docker compose logs -f airflow-worker"
echo "  docker compose logs -f airflow-api-server"

