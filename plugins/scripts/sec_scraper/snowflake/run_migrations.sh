#!/bin/bash
# Run all pending migrations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"

cd "$PROJECT_ROOT"

# Use the venv if it exists
if [ -d ".venv" ]; then
    .venv/bin/python "$SCRIPT_DIR/deploy_migrations.py" "$@"
else
    python3 "$SCRIPT_DIR/deploy_migrations.py" "$@"
fi

