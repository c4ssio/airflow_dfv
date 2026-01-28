## Scripts Overview
These scripts help manage Airflow services and diagnose runtime issues.

## What Each Script Does
- `setup_venv.sh`: create/upgrade the local Python venv.
- `run_with_venv.sh`: run commands inside the venv.
- `check_airflow.sh`: collect diagnostics for Airflow services and ports.
- `restart_airflow_services.sh`: restart scheduler, worker, API server.
- `check_worker_memory.sh`: snapshot worker memory usage.
- `monitor_worker_memory.sh`: track memory usage over time.
- `cleanup_removed_tasks.py`: remove task metadata no longer present.

## Usage Notes
- These scripts assume you run from the repo root.
- They are intended for operational diagnostics, not for data processing.
