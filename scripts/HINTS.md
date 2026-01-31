## Scripts Overview
These scripts help manage Airflow services and run ad-hoc diagnostics.

## What Each Script Does
- **adhoc/** (see `scripts/adhoc/README.md`): manual scripts – ticker price coverage SQL, Yahoo missing-tickers diagnostic.
- `setup_venv.sh`: create/upgrade the local Python venv.
- `run_with_venv.sh`: run commands inside the venv.
- `check_airflow.sh`: collect diagnostics for Airflow services and ports.
- `restart_airflow_services.sh`: restart scheduler, worker, API server.
- `check_worker_memory.sh`: snapshot worker memory usage.
- `monitor_worker_memory.sh`: track memory usage over time.
- `cleanup_removed_tasks.py`: remove task metadata no longer present in the DAG.

## Usage Notes
- Run from the repo root.
- Operational scripts are for Airflow/memory diagnostics; ad-hoc scripts are for data/price coverage checks.
