-- Coverage: how many tracked tickers have at least one price in ticker_prices_daily?
-- Run from repo root: docker compose exec -T postgres psql -U airflow -d sec_data -v ON_ERROR_STOP=1 < scripts/adhoc/check_ticker_price_coverage.sql

WITH latest_ingest AS (
  SELECT MAX(ingest_date) AS d FROM sec_raw.submissions
),
tracked AS (
  SELECT DISTINCT m.ticker
  FROM sec_raw.submissions_ticker_mapping m, latest_ingest l
  WHERE m.ingest_date = l.d
    AND m.is_likely_common_stock = TRUE
    AND m.ticker IS NOT NULL AND m.ticker <> ''
),
with_price AS (
  SELECT DISTINCT ticker FROM sec_raw.ticker_prices_daily
)
SELECT
  (SELECT COUNT(*) FROM tracked) AS tickers_tracked,
  (SELECT COUNT(*) FROM tracked t JOIN with_price p ON t.ticker = p.ticker) AS tickers_with_price,
  (SELECT COUNT(*) FROM tracked t WHERE NOT EXISTS (SELECT 1 FROM with_price p WHERE p.ticker = t.ticker)) AS tickers_missing_price;
