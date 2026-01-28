"""SEC scraper task implementations.

Convention:
- One file per Airflow `@task` wrapper when possible, e.g. `fetch_company_ciks.py`.
- Only create a per-task folder when a task needs multiple submodules.
"""

