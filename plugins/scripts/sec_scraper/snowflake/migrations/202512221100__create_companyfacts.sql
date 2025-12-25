-- Snowflake DDL for SEC EDGAR companyfacts.json data
-- This schema stores company financial facts (US-GAAP metrics) from the SEC EDGAR API
-- Source: https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json
--
-- Structure: Each company has multiple facts (metrics) organized by taxonomy (dei, us-gaap).
-- Each fact has time series data with different units (USD, shares, etc.)

-- Main company facts metadata table
CREATE OR REPLACE TABLE sec_raw.companyfacts_metadata (
    -- Primary key
    cik STRING NOT NULL COMMENT 'Central Index Key - 10-digit zero-padded identifier',
    entity_name STRING COMMENT 'Legal company name',
    
    -- Audit fields
    ingest_date DATE COMMENT 'Date when this data was ingested',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when this record was created',
    
    PRIMARY KEY (cik, ingest_date)
)
COMMENT = 'Metadata for company facts. Links to companyfacts_facts table.';

-- Facts table - stores individual metric values
-- This is normalized: one row per metric value per company per period
CREATE OR REPLACE TABLE sec_raw.companyfacts_facts (
    -- Primary key
    cik STRING NOT NULL COMMENT 'Central Index Key - links to companyfacts_metadata',
    ingest_date DATE NOT NULL COMMENT 'Date when this data was ingested',
    taxonomy STRING NOT NULL COMMENT 'Taxonomy namespace: "dei" (Document and Entity Information) or "us-gaap" (US Generally Accepted Accounting Principles)',
    metric_name STRING NOT NULL COMMENT 'Metric identifier (e.g., "Assets", "Revenues", "NetIncomeLoss", "EarningsPerShareBasic")',
    unit STRING NOT NULL COMMENT 'Unit of measure (e.g., "USD", "shares", "USD/shares")',
    
    -- Time series data fields
    period_end DATE COMMENT 'End date of the reporting period (YYYY-MM-DD)',
    value NUMBER COMMENT 'Numeric value for this metric',
    accession_number STRING COMMENT 'SEC accession number for the filing containing this value (format: CIK-YY-XXXXXX)',
    fiscal_year INTEGER COMMENT 'Fiscal year (e.g., 2025)',
    fiscal_period STRING COMMENT 'Fiscal period: "FY" (full year), "Q1", "Q2", "Q3", "Q4", "H1", "H2", "M1" through "M12"',
    form_type STRING COMMENT 'SEC form type (e.g., "10-K", "10-Q", "8-K")',
    filed_date DATE COMMENT 'Date the filing was submitted to SEC (YYYY-MM-DD)',
    frame STRING COMMENT 'Fiscal period frame identifier (e.g., "CY2025Q3I" for calendar year 2025 Q3 interim)',
    
    -- Metric metadata (from the fact definition)
    metric_label STRING COMMENT 'Human-readable label for the metric (e.g., "Assets", "Revenues")',
    metric_description STRING COMMENT 'Detailed description of what this metric represents',
    
    -- Audit fields
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when this record was created',
    
    PRIMARY KEY (cik, ingest_date, taxonomy, metric_name, unit, period_end, accession_number)
)
COMMENT = 'Individual metric values from SEC EDGAR company facts. One row per metric value per reporting period.';

-- Note: Snowflake doesn't support CREATE INDEX. Use clustering keys for performance:
-- ALTER TABLE sec_raw.companyfacts_facts CLUSTER BY (cik, period_end);
-- ALTER TABLE sec_raw.companyfacts_facts CLUSTER BY (taxonomy, metric_name);

-- Metric Abbreviations:
-- For easier querying, use the us_gaap_metric_abbreviations table and the 
-- companyfacts_facts_with_abbrev view. Common abbreviations include:
--   EPS_Basic, EPS_Diluted, NI (Net Income), REV (Revenue), ASSETS, EQUITY,
--   CFO (Cash Flow from Operations), ROE, ROA, etc.
-- See schemas/sec_raw/us_gaap_metric_abbreviations.sql for the full mapping.

-- Notes on common US-GAAP metrics:
-- Common metric names include (there are 464+ possible metrics):
--   Assets, AssetsCurrent, AssetsNoncurrent
--   Liabilities, LiabilitiesCurrent, LiabilitiesNoncurrent
--   StockholdersEquity
--   Revenues, SalesRevenueNet, SalesRevenueServicesNet
--   NetIncomeLoss, NetIncomeLossAvailableToCommonStockholdersBasic
--   EarningsPerShareBasic, EarningsPerShareDiluted
--   CashAndCashEquivalentsAtCarryingValue
--   NetCashProvidedByUsedInOperatingActivities
--   WeightedAverageNumberOfSharesOutstandingBasic
--   WeightedAverageNumberOfDilutedSharesOutstanding
--
-- Common units:
--   USD: US Dollars (most financial metrics)
--   shares: Share counts
--   USD/shares: Per-share amounts (e.g., EPS)
--   pure: Unitless ratios
--
-- Common fiscal periods:
--   FY: Full fiscal year
--   Q1, Q2, Q3, Q4: Quarters
--   H1, H2: Half years
--   M1-M12: Months
--
-- Common form types in facts:
--   10-K: Annual report
--   10-Q: Quarterly report
--   8-K: Current report
--   10-K/A, 10-Q/A: Amended reports

-- Alternative: If you prefer to store the entire facts structure as VARIANT
-- (useful for preserving exact structure and handling dynamic metrics):
CREATE OR REPLACE TABLE sec_raw.companyfacts_variant (
    cik STRING NOT NULL COMMENT 'Central Index Key',
    entity_name STRING COMMENT 'Legal company name',
    facts VARIANT COMMENT 'Complete facts structure as JSON. Contains "dei" and "us-gaap" objects, each with metric names as keys. Each metric has: label, description, units (object with unit names as keys, each containing arrays of time series entries)',
    ingest_date DATE COMMENT 'Date when this data was ingested',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when this record was created',
    
    PRIMARY KEY (cik, ingest_date)
)
COMMENT = 'Company facts stored as VARIANT. Use this if you want to preserve the exact JSON structure and query using Snowflake JSON functions.';

