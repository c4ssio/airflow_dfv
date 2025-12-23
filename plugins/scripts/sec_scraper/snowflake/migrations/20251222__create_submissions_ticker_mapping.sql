-- Helper view to normalize ticker/exchange mapping from submissions table
-- This view expands the parallel arrays into individual rows for easier querying
-- 
-- Usage:
--   SELECT * FROM sec_raw.submissions_ticker_mapping WHERE cik = '0000001750';
--   SELECT * FROM sec_raw.submissions_ticker_mapping WHERE ticker = 'UNM';

CREATE OR REPLACE VIEW sec_raw.submissions_ticker_mapping AS
SELECT
    s.cik,
    s.entity_name,
    s.ingest_date,
    t.value AS ticker,
    e.value AS exchange,
    -- Determine if this is likely the primary/common stock ticker
    -- (first ticker in array is typically primary)
    CASE 
        WHEN t.index = 0 THEN TRUE 
        ELSE FALSE 
    END AS is_primary_ticker,
    -- Heuristic: tickers without suffixes (like -B, -P, -A with dash) are usually common stock
    -- Note: Tickers ending in A without a dash (like TSLA, META) are common stock, not Class A
    CASE 
        WHEN t.value NOT LIKE '%-%' THEN TRUE  -- No dash = likely common stock
        WHEN t.value LIKE '%-B' OR t.value LIKE '%-P' THEN FALSE  -- Has dash with B/P = preferred/class B
        WHEN t.value LIKE '%-A' THEN FALSE  -- Has dash with A = Class A
        ELSE TRUE  -- Other patterns with dash, default to common
    END AS is_likely_common_stock,
    -- Extract share class indicator from ticker suffix
    -- Only classify as Class A/B/Preferred if there's a dash separator
    CASE 
        WHEN t.value LIKE '%-B' THEN 'Class B'
        WHEN t.value LIKE '%-P' THEN 'Preferred'
        WHEN t.value LIKE '%-A' THEN 'Class A'
        WHEN t.value LIKE '%B' AND t.value LIKE '%-%' THEN 'Class B'  -- Ends in B and has dash elsewhere
        WHEN t.value LIKE '%P' AND t.value LIKE '%-%' THEN 'Preferred'  -- Ends in P and has dash elsewhere
        WHEN t.value LIKE '%A' AND t.value LIKE '%-%' THEN 'Class A'  -- Ends in A and has dash elsewhere
        ELSE 'Common'  -- Default: common stock (includes TSLA, META, etc.)
    END AS share_class_indicator,
    s.created_at
FROM 
    sec_raw.submissions s,
    LATERAL FLATTEN(input => s.tickers) t,
    LATERAL FLATTEN(input => s.exchanges) e
WHERE 
    t.index = e.index  -- Ensure parallel array alignment
;

COMMENT ON VIEW sec_raw.submissions_ticker_mapping IS 
'Normalized view of ticker/exchange mappings from submissions table. Expands parallel arrays into individual rows. 
Multiple tickers typically represent different share classes (all current), not historical changes.
Use is_primary_ticker or is_likely_common_stock to identify the main trading ticker.';

-- Example queries:
-- 
-- Get all tickers for a company:
-- SELECT ticker, exchange, is_primary_ticker, share_class_indicator
-- FROM sec_raw.submissions_ticker_mapping
-- WHERE cik = '0000001750'
-- ORDER BY is_primary_ticker DESC, ticker;
--
-- Find the primary/common stock ticker:
-- SELECT ticker, exchange
-- FROM sec_raw.submissions_ticker_mapping
-- WHERE cik = '0000001750'
--   AND is_likely_common_stock = TRUE
-- LIMIT 1;
--
-- Get all companies trading on a specific exchange:
-- SELECT DISTINCT cik, entity_name, ticker
-- FROM sec_raw.submissions_ticker_mapping
-- WHERE exchange = 'NYSE'
-- ORDER BY entity_name;

