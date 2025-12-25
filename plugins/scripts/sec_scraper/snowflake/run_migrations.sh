#!/bin/bash
# Run all pending migrations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"

cd "$PROJECT_ROOT"

# Use the venv if it exists
if [ -d "venv" ]; then
    venv/bin/python "$SCRIPT_DIR/deploy_migrations.py" "$@"
else
    echo "Error: Virtual environment 'venv' not found."
    echo "Please run ../../../../scripts/setup_venv.sh first."
    exit 1
fi

