#!/bin/bash
# Setup script to create and configure the virtual environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"

echo "Setting up virtual environment for airflow_dfv..."

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists at $VENV_DIR"
fi

# Activate venv
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
    echo "Installing requirements from requirements.txt..."
    pip install -r "$PROJECT_ROOT/requirements.txt"
else
    echo "⚠ requirements.txt not found"
fi

echo ""
echo "✓ Virtual environment setup complete!"
echo ""
echo "To activate it manually:"
echo "  source venv/bin/activate"
echo ""
echo "Or use the helper script:"
echo "  ./scripts/run_with_venv.sh python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py"

