-- Helper view to normalize ticker/exchange mapping from submissions table
-- This view expands the parallel arrays into individual rows for easier querying
-- PostgreSQL version using jsonb_array_elements_text and WITH ORDINALITY

CREATE OR REPLACE VIEW sec_raw.submissions_ticker_mapping AS
SELECT
    s.cik,
    s.entity_name,
    s.ingest_date,
    t.ticker,
    e.exchange,
    -- Determine if this is likely the primary/common stock ticker
    -- (first ticker in array is typically primary)
    CASE
        WHEN t.idx = 0 THEN TRUE
        ELSE FALSE
    END AS is_primary_ticker,
    -- Heuristic: tickers without suffixes (like -B, -P, -A with dash) are usually common stock
    CASE
        WHEN t.ticker NOT LIKE '%-%' THEN TRUE
        WHEN t.ticker LIKE '%-B' OR t.ticker LIKE '%-P' THEN FALSE
        WHEN t.ticker LIKE '%-A' THEN FALSE
        ELSE TRUE
    END AS is_likely_common_stock,
    -- Extract share class indicator from ticker suffix
    CASE
        WHEN t.ticker LIKE '%-B' THEN 'Class B'
        WHEN t.ticker LIKE '%-P' THEN 'Preferred'
        WHEN t.ticker LIKE '%-A' THEN 'Class A'
        WHEN t.ticker LIKE '%B' AND t.ticker LIKE '%-%' THEN 'Class B'
        WHEN t.ticker LIKE '%P' AND t.ticker LIKE '%-%' THEN 'Preferred'
        WHEN t.ticker LIKE '%A' AND t.ticker LIKE '%-%' THEN 'Class A'
        ELSE 'Common'
    END AS share_class_indicator,
    s.created_at
FROM
    sec_raw.submissions s,
    LATERAL (
        SELECT value::text AS ticker, ordinality - 1 AS idx
        FROM jsonb_array_elements_text(s.tickers) WITH ORDINALITY
    ) t,
    LATERAL (
        SELECT value::text AS exchange, ordinality - 1 AS idx
        FROM jsonb_array_elements_text(s.exchanges) WITH ORDINALITY
    ) e
WHERE
    t.idx = e.idx;  -- Ensure parallel array alignment

COMMENT ON VIEW sec_raw.submissions_ticker_mapping IS
'Normalized view of ticker/exchange mappings from submissions table. Expands parallel arrays into individual rows.';
