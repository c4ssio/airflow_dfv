# Scripts

Helper scripts for managing the Airflow DFV project.

## Available Scripts

### `setup_venv.sh`
Creates and configures the Python virtual environment for the project.

**Usage:**
```bash
./scripts/setup_venv.sh
```

**What it does:**
- Creates a `venv` directory if it doesn't exist
- Activates the virtual environment
- Upgrades pip to the latest version
- Installs all dependencies from `requirements.txt`

**When to use:**
- First time setting up the project
- After adding new dependencies to `requirements.txt`
- If the virtual environment gets corrupted

---

### `run_with_venv.sh`
Runs Python commands using the project's virtual environment.

**Usage:**
```bash
./scripts/run_with_venv.sh <command>
```

**Examples:**
```bash
# Run a Python script
./scripts/run_with_venv.sh python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py

# Run Python with arguments
./scripts/run_with_venv.sh python -m pytest tests/

# Run any command that needs the venv
./scripts/run_with_venv.sh pip list
```

**What it does:**
- Checks if the `venv` directory exists
- Activates the virtual environment
- Executes the provided command with the venv active

**When to use:**
- Running Python scripts that need project dependencies
- Running migration scripts
- Any command that requires the virtual environment

---

### `check_airflow.sh`
Collects diagnostic information about the Airflow deployment.

**Usage:**
```bash
./scripts/check_airflow.sh
```

**What it does:**
- Checks Docker Compose service status
- Lists running Airflow containers
- Shows recent API server logs (last 200 lines)
- Checks if port 8080 is in use
- Lists processes related to Airflow
- Saves diagnostics to `diagnostics/airflow_status.txt` and `/tmp/airflow_status.txt`

**When to use:**
- Troubleshooting Airflow connectivity issues
- Checking if services are running
- Gathering information for bug reports
- Diagnosing port conflicts

---

### `restart_airflow_services.sh`
Restarts Airflow services related to task execution.

**Usage:**
```bash
./scripts/restart_airflow_services.sh
```

**What it does:**
- Restarts `airflow-worker` (runs the actual tasks)
- Restarts `airflow-api-server` (handles XCom pushes and API requests)
- Restarts `airflow-scheduler` (schedules tasks)

**When to use:**
- After fixing JWT token configuration issues
- When tasks are stuck or not executing
- After making changes to environment variables
- When experiencing authentication errors
- If tasks are stuck in "queued" state

**Note:** This only restarts the services - it doesn't stop and start them. For a more thorough restart, use:
```bash
docker compose stop airflow-worker airflow-api-server airflow-scheduler
docker compose start airflow-worker airflow-api-server airflow-scheduler
```

---

## Quick Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `setup_venv.sh` | Create/update virtual environment | Initial setup, after dependency changes |
| `run_with_venv.sh` | Run commands with venv | Running Python scripts, migrations |
| `check_airflow.sh` | Collect diagnostics | Troubleshooting, debugging |
| `restart_airflow_services.sh` | Restart Airflow services | After config changes, stuck tasks |

## Prerequisites

- **Docker & Docker Compose**: Required for Airflow-related scripts
- **Python 3**: Required for virtual environment scripts
- **Bash**: All scripts require bash shell

## Notes

- All scripts assume you're running from the project root directory
- Scripts use `set -euo pipefail` for error handling (except `setup_venv.sh` which uses `set -e`)
- The virtual environment is created in the `venv/` directory (not `.venv/`)

