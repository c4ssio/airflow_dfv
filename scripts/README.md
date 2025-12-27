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

### `check_worker_memory.sh`
Monitors memory usage by process in the Airflow worker container.

**Usage:**
```bash
./scripts/check_worker_memory.sh
```

**What it does:**
- Shows container-level memory statistics
- Lists top 10 processes by memory usage (RSS)
- Provides detailed memory breakdown by process ID
- Groups memory usage by process type (e.g., celery, python, airflow)
- Shows Python/Celery-specific memory details
- Displays system memory info and cgroup memory limits

**When to use:**
- Investigating memory leaks or high memory usage
- Identifying which tasks/processes are consuming the most memory
- Monitoring memory usage during task execution
- Debugging out-of-memory errors

**Note:** This script works without the `ps` command by reading from `/proc` directly, so it works with the base Airflow image without requiring additional packages.

**Optional: Install `procps` for simpler commands**

If you prefer using `ps` commands, you can install `procps` in your Dockerfile. This won't affect your Postgres database (it's a separate container). To do this:

1. Update `Dockerfile`:
```dockerfile
FROM apache/airflow:3.1.5

USER root
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

2. Rebuild the image:
```bash
docker compose build
docker compose up -d
```

**Quick one-liners (alternative to the script):**
```bash
# Get container name first
CONTAINER=$(docker ps --format '{{.Names}}' | grep airflow-worker)

# Container memory stats (updates every second)
docker stats $CONTAINER

# If procps is installed, you can also use:
# docker exec $CONTAINER ps aux --sort=-%mem | head -11
```

---

### `monitor_worker_memory.sh`
Monitors memory usage over time to identify memory leaks and growth patterns.

**Usage:**
```bash
./scripts/monitor_worker_memory.sh [interval_seconds] [count]
```

**Examples:**
```bash
# Monitor for 1 minute (10 second intervals, 6 samples)
./scripts/monitor_worker_memory.sh 10 6

# Monitor for 5 minutes (30 second intervals, 10 samples)
./scripts/monitor_worker_memory.sh 30 10

# Quick check: 20 seconds total (5 second intervals, 4 samples)
./scripts/monitor_worker_memory.sh 5 4
```

**What it does:**
- Takes multiple memory snapshots at specified intervals
- Tracks memory usage per worker type (MainProcess, ForkPoolWorker-1, etc.)
- Calculates min, max, and average memory per worker type
- Saves a detailed timeline to `diagnostics/memory_timeline_YYYYMMDD_HHMMSS.txt`
- Provides analysis and recommendations

**When to use:**
- Investigating memory leaks (run during task execution)
- Understanding memory growth patterns
- Identifying which worker types consume the most memory
- Before and after optimizing tasks to measure improvement

**Interpreting results:**
- **MAX >> MIN**: Indicates memory growth during task execution (potential leak)
- **Consistent growth over time**: Workers not releasing memory between tasks
- **High average**: Consider reducing worker concurrency or optimizing tasks

---

## Quick Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `setup_venv.sh` | Create/update virtual environment | Initial setup, after dependency changes |
| `run_with_venv.sh` | Run commands with venv | Running Python scripts, migrations |
| `check_airflow.sh` | Collect diagnostics | Troubleshooting, debugging |
| `restart_airflow_services.sh` | Restart Airflow services | After config changes, stuck tasks |
| `check_worker_memory.sh` | Snapshot of worker memory usage | Quick memory check, current state |
| `monitor_worker_memory.sh` | Track memory over time | Memory leak detection, growth analysis |

## Prerequisites

- **Docker & Docker Compose**: Required for Airflow-related scripts
- **Python 3**: Required for virtual environment scripts
- **Bash**: All scripts require bash shell

## Notes

- All scripts assume you're running from the project root directory
- Scripts use `set -euo pipefail` for error handling (except `setup_venv.sh` which uses `set -e`)
- The virtual environment is created in the `venv/` directory (not `.venv/`)

