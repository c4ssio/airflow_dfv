-- Daily ticker prices for cheapness evaluation
-- Stores one row per (ticker, price_date) with OHLCV.
--
-- Notes:
-- - We intentionally key by ticker + price_date (not CIK) because tickers can map
--   to different issuers over time; join to `submissions_ticker_mapping` by ticker
--   and ingest_date when doing analyses.
-- - `source` records the upstream provider (e.g., "stooq").
-- - Uses an upsert-friendly PK and a couple of common indexes.

CREATE TABLE IF NOT EXISTS sec_raw.ticker_prices_daily (
    ticker TEXT NOT NULL,
    price_date DATE NOT NULL,

    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,

    currency TEXT,
    source TEXT NOT NULL DEFAULT 'unknown',

    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, price_date)
);

CREATE INDEX IF NOT EXISTS idx_ticker_prices_daily_price_date
    ON sec_raw.ticker_prices_daily(price_date);

CREATE INDEX IF NOT EXISTS idx_ticker_prices_daily_ticker
    ON sec_raw.ticker_prices_daily(ticker);

COMMENT ON TABLE sec_raw.ticker_prices_daily IS
'Daily OHLCV by ticker for valuation/cheapness analysis.';

