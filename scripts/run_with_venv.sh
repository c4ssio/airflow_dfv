#!/bin/bash
# Helper script to run Python commands with the project's virtual environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"

# Activate venv if it exists
if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
    echo "✓ Activated virtual environment: $VENV_DIR"
else
    echo "✗ Virtual environment not found at $VENV_DIR"
    echo "  Create it with: python3 -m venv .venv"
    exit 1
fi

# Run the command passed as arguments
exec "$@"

