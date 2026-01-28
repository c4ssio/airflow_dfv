-- PostgreSQL DDL for SEC EDGAR submissions.json data
-- This table stores company submission metadata from the SEC EDGAR API
-- Source: https://data.sec.gov/submissions/CIK##########.json

CREATE TABLE IF NOT EXISTS sec_raw.submissions (
    -- Primary key
    cik TEXT NOT NULL,  -- Central Index Key - 10-digit zero-padded identifier

    -- Company identification
    entity_name TEXT,  -- Legal company name
    entity_type TEXT,  -- Entity type: "operating" or "other"
    ein TEXT,  -- Employer Identification Number (9 digits)
    lei TEXT,  -- Legal Entity Identifier (20 characters, ISO 17442)

    -- Company classification
    sic TEXT,  -- Standard Industrial Classification code (4 digits)
    sic_description TEXT,  -- SIC industry description
    category TEXT,  -- Filer category
    owner_org TEXT,  -- Owner organization code and description

    -- Company details
    description TEXT,  -- Company business description
    website TEXT,  -- Company website URL
    investor_website TEXT,  -- Investor relations website URL
    phone TEXT,  -- Company phone number

    -- Incorporation details
    state_of_incorporation TEXT,  -- State of incorporation code (2-letter)
    state_of_incorporation_description TEXT,  -- State of incorporation full name
    fiscal_year_end TEXT,  -- Fiscal year end date (MMDD format)

    -- Trading information (parallel arrays stored as JSONB)
    tickers JSONB DEFAULT '[]'::jsonb,  -- List of stock ticker symbols
    exchanges JSONB DEFAULT '[]'::jsonb,  -- List of stock exchanges

    -- Insider transaction flags
    insider_transaction_for_issuer_exists INTEGER,  -- 1 if exists, 0 otherwise
    insider_transaction_for_owner_exists INTEGER,  -- 1 if exists, 0 otherwise

    -- Flags and metadata
    flags TEXT,  -- Additional flags
    former_names JSONB DEFAULT '[]'::jsonb,  -- Array of former company names

    -- Addresses (stored as JSONB to preserve structure)
    addresses JSONB,  -- JSON object with "mailing" and "business" addresses

    -- Filings metadata (stored as JSONB)
    filings JSONB,  -- JSON object with "recent" and "files" keys

    -- Audit fields
    ingest_date DATE,  -- Date when this data was ingested
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when record was created

    PRIMARY KEY (cik, ingest_date)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_submissions_ingest_date ON sec_raw.submissions(ingest_date);
CREATE INDEX IF NOT EXISTS idx_submissions_entity_name ON sec_raw.submissions(entity_name);
CREATE INDEX IF NOT EXISTS idx_submissions_sic ON sec_raw.submissions(sic);

COMMENT ON TABLE sec_raw.submissions IS 'SEC EDGAR company submissions metadata. Contains company information and filing history.';
