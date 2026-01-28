-- PostgreSQL DDL for SEC EDGAR companyfacts.json data
-- This schema stores company financial facts (US-GAAP metrics) from the SEC EDGAR API
-- Source: https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json

-- Main company facts metadata table
CREATE TABLE IF NOT EXISTS sec_raw.companyfacts_metadata (
    -- Primary key
    cik TEXT NOT NULL,  -- Central Index Key - 10-digit zero-padded identifier
    entity_name TEXT,  -- Legal company name

    -- Audit fields
    ingest_date DATE,  -- Date when this data was ingested
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when record was created

    PRIMARY KEY (cik, ingest_date)
);

COMMENT ON TABLE sec_raw.companyfacts_metadata IS 'Metadata for company facts. Links to companyfacts_facts table.';

-- Facts table - stores individual metric values
-- This is normalized: one row per metric value per company per period
CREATE TABLE IF NOT EXISTS sec_raw.companyfacts_facts (
    -- Primary key columns
    cik TEXT NOT NULL,  -- Central Index Key - links to companyfacts_metadata
    ingest_date DATE NOT NULL,  -- Date when this data was ingested
    taxonomy TEXT NOT NULL,  -- Taxonomy namespace: "dei" or "us-gaap"
    metric_name TEXT NOT NULL,  -- Metric identifier (e.g., "Assets", "Revenues")
    unit TEXT NOT NULL,  -- Unit of measure (e.g., "USD", "shares")

    -- Time series data fields
    period_end DATE,  -- End date of the reporting period
    value NUMERIC,  -- Numeric value for this metric
    accession_number TEXT,  -- SEC accession number for the filing
    fiscal_year INTEGER,  -- Fiscal year (e.g., 2025)
    fiscal_period TEXT,  -- Fiscal period: "FY", "Q1", "Q2", "Q3", "Q4", etc.
    form_type TEXT,  -- SEC form type (e.g., "10-K", "10-Q")
    filed_date DATE,  -- Date the filing was submitted to SEC
    frame TEXT,  -- Fiscal period frame identifier

    -- Metric metadata (from the fact definition)
    metric_label TEXT,  -- Human-readable label for the metric
    metric_description TEXT,  -- Detailed description of the metric

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when record was created

    PRIMARY KEY (cik, ingest_date, taxonomy, metric_name, unit, period_end, accession_number)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_cik ON sec_raw.companyfacts_facts(cik);
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_ingest_date ON sec_raw.companyfacts_facts(ingest_date);
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_metric ON sec_raw.companyfacts_facts(taxonomy, metric_name);
CREATE INDEX IF NOT EXISTS idx_companyfacts_facts_period ON sec_raw.companyfacts_facts(period_end);

COMMENT ON TABLE sec_raw.companyfacts_facts IS 'Individual metric values from SEC EDGAR company facts. One row per metric value per reporting period.';

-- Alternative: Store entire facts structure as JSONB (for preserving exact structure)
CREATE TABLE IF NOT EXISTS sec_raw.companyfacts_variant (
    cik TEXT NOT NULL,  -- Central Index Key
    entity_name TEXT,  -- Legal company name
    facts JSONB,  -- Complete facts structure as JSON
    ingest_date DATE,  -- Date when this data was ingested
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when record was created

    PRIMARY KEY (cik, ingest_date)
);

COMMENT ON TABLE sec_raw.companyfacts_variant IS 'Company facts stored as JSONB. Use this for preserving exact JSON structure.';
