# Ad-hoc scripts

Manually run scripts for diagnostics and one-off checks. Run from the **repo root** unless noted.

## check_ticker_price_coverage.sql

Counts how many tracked tickers (from `submissions_ticker_mapping`, latest ingest, likely common stock) have at least one row in `ticker_prices_daily`.

**Run:**
```bash
docker compose exec -T postgres psql -U airflow -d sec_data -v ON_ERROR_STOP=1 < scripts/adhoc/check_ticker_price_coverage.sql
```

Output: `tickers_tracked`, `tickers_with_price`, `tickers_missing_price`.

---

## check_yahoo_missing.py

For tickers that have no price in `ticker_prices_daily`, calls Yahoo Finance (yfinance) and classifies each response (has_data / empty / error). Use this to see why some tickers are missing prices.

**Run (from repo root; Postgres must be reachable, e.g. host localhost or port-forward):**
```bash
PYTHONPATH=plugins python scripts/adhoc/check_yahoo_missing.py
```

**Optional env:**
- `POSTGRES_CONFIG_PATH` – path to `postgres.yaml` (default: `config/postgres.yaml`). Use a config with `host: localhost` when running from the host.
- `SEC_PRICE_DATE` – date to check (YYYY-MM-DD; default: today UTC).

Requires: `yfinance`, `psycopg2-binary`, `pyyaml` (same as the main pipeline).
