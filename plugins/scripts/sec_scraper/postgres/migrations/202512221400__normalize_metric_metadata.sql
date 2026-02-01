-- Normalize metric metadata to reduce redundancy in companyfacts_facts
-- This migration:
-- 1. Renames us_gaap_metric_abbreviations to metric_metadata
-- 2. Adds label column (was metric_label in facts)
-- 3. Restructures to be the canonical source for metric info
-- 4. Removes redundant columns from companyfacts_facts
-- 5. Creates a view that joins them together

-- Step 1: Create the new metric_metadata table
CREATE TABLE IF NOT EXISTS sec_raw.metric_metadata (
    taxonomy TEXT NOT NULL,  -- 'dei' or 'us-gaap'
    metric_name TEXT NOT NULL,  -- e.g., 'Assets', 'Revenues'
    label TEXT,  -- Human-readable label
    description TEXT,  -- Detailed description
    abbreviation TEXT,  -- Short form for display
    category TEXT,  -- e.g., 'Balance Sheet', 'Income Statement'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (taxonomy, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_metric_metadata_abbrev ON sec_raw.metric_metadata(abbreviation);

COMMENT ON TABLE sec_raw.metric_metadata IS
'Canonical metadata for SEC EDGAR metrics. Populated during ingestion from companyfacts data.';

-- Step 2: Migrate data from us_gaap_metric_abbreviations if it exists
INSERT INTO sec_raw.metric_metadata (taxonomy, metric_name, label, description, abbreviation, category)
SELECT
    'us-gaap' as taxonomy,
    metric_name,
    NULL as label,  -- will be populated from facts during ingestion
    description,
    abbreviation,
    category
FROM sec_raw.us_gaap_metric_abbreviations
ON CONFLICT (taxonomy, metric_name) DO NOTHING;

-- Step 3: Create new facts table without redundant text fields
-- We'll create it as companyfacts_facts_normalized first, then swap
CREATE TABLE IF NOT EXISTS sec_raw.companyfacts_facts_v2 (
    cik TEXT NOT NULL,
    ingest_date DATE NOT NULL,
    taxonomy TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    unit TEXT NOT NULL,
    period_end DATE,
    value NUMERIC,
    accession_number TEXT NOT NULL,
    fiscal_year INTEGER,
    fiscal_period TEXT,
    form_type TEXT,
    filed_date DATE,
    frame TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (cik, ingest_date, taxonomy, metric_name, unit, period_end, accession_number)
);

CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_v2_cik ON sec_raw.companyfacts_facts_v2(cik);
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_v2_ingest ON sec_raw.companyfacts_facts_v2(ingest_date);
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_v2_metric ON sec_raw.companyfacts_facts_v2(taxonomy, metric_name);
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_v2_period ON sec_raw.companyfacts_facts_v2(period_end);

COMMENT ON TABLE sec_raw.companyfacts_facts_v2 IS
'Normalized facts table. Join with metric_metadata for labels/descriptions.';

-- Step 4: Create view that joins facts with metadata
CREATE OR REPLACE VIEW sec_raw.companyfacts_facts_full AS
SELECT
    f.cik,
    f.ingest_date,
    f.taxonomy,
    f.metric_name,
    f.unit,
    f.period_end,
    f.value,
    f.accession_number,
    f.fiscal_year,
    f.fiscal_period,
    f.form_type,
    f.filed_date,
    f.frame,
    f.created_at,
    m.label AS metric_label,
    m.description AS metric_description,
    COALESCE(m.abbreviation, f.metric_name) AS metric_abbrev,
    m.category AS metric_category
FROM sec_raw.companyfacts_facts_v2 f
LEFT JOIN sec_raw.metric_metadata m
    ON f.taxonomy = m.taxonomy AND f.metric_name = m.metric_name;

COMMENT ON VIEW sec_raw.companyfacts_facts_full IS
'Facts with metric metadata joined. Use this for queries needing labels/descriptions.';

-- Step 5: Rename tables (swap old for new)
-- Drop the old view first
DROP VIEW IF EXISTS sec_raw.companyfacts_facts_with_abbrev;

-- Rename old facts table to backup
ALTER TABLE IF EXISTS sec_raw.companyfacts_facts RENAME TO companyfacts_facts_old;

-- Rename new table to be the primary
ALTER TABLE sec_raw.companyfacts_facts_v2 RENAME TO companyfacts_facts;

-- Drop old indexes if they exist (from original table)
DROP INDEX IF EXISTS sec_raw.idx_companyfacts_facts_cik;
DROP INDEX IF EXISTS sec_raw.idx_companyfacts_facts_ingest_date;
DROP INDEX IF EXISTS sec_raw.idx_companyfacts_facts_metric;
DROP INDEX IF EXISTS sec_raw.idx_companyfacts_facts_period;

-- Rename new indexes
ALTER INDEX IF EXISTS sec_raw.idx_companyfacts_facts_v2_cik RENAME TO idx_companyfacts_facts_cik;
ALTER INDEX IF EXISTS sec_raw.idx_companyfacts_facts_v2_ingest RENAME TO idx_companyfacts_facts_ingest_date;
ALTER INDEX IF EXISTS sec_raw.idx_companyfacts_facts_v2_metric RENAME TO idx_companyfacts_facts_metric;
ALTER INDEX IF EXISTS sec_raw.idx_companyfacts_facts_v2_period RENAME TO idx_companyfacts_facts_period;

-- Step 6: Create backwards-compatible view with old name
CREATE OR REPLACE VIEW sec_raw.companyfacts_facts_with_abbrev AS
SELECT * FROM sec_raw.companyfacts_facts_full;

COMMENT ON VIEW sec_raw.companyfacts_facts_with_abbrev IS
'Backwards-compatible alias for companyfacts_facts_full.';
