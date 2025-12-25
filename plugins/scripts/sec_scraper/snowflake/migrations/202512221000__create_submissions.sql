-- Snowflake DDL for SEC EDGAR submissions.json data
-- This table stores company submission metadata from the SEC EDGAR API
-- Source: https://data.sec.gov/submissions/CIK##########.json

CREATE OR REPLACE TABLE sec_raw.submissions (
    -- Primary key
    cik STRING NOT NULL COMMENT 'Central Index Key - 10-digit zero-padded identifier (e.g., "0000001750")',
    
    -- Company identification
    entity_name STRING COMMENT 'Legal company name (e.g., "AAR CORP")',
    entity_type STRING COMMENT 'Entity type: "operating" or "other"',
    ein STRING COMMENT 'Employer Identification Number (9 digits)',
    lei STRING COMMENT 'Legal Entity Identifier (20 characters, ISO 17442)',
    
    -- Company classification
    sic STRING COMMENT 'Standard Industrial Classification code (4 digits)',
    sic_description STRING COMMENT 'SIC industry description (e.g., "Aircraft & Parts")',
    category STRING COMMENT 'Filer category. Possible values: "", "Accelerated filer", "Accelerated filer<br>Smaller reporting company", "Large accelerated filer", "Non-accelerated filer<br>Smaller reporting company"',
    owner_org STRING COMMENT 'Owner organization code and description (e.g., "04 Manufacturing")',
    
    -- Company details
    description STRING COMMENT 'Company business description',
    website STRING COMMENT 'Company website URL',
    investor_website STRING COMMENT 'Investor relations website URL',
    phone STRING COMMENT 'Company phone number (digits only, no formatting)',
    
    -- Incorporation details
    state_of_incorporation STRING COMMENT 'State of incorporation code (2-letter, e.g., "DE")',
    state_of_incorporation_description STRING COMMENT 'State of incorporation full name',
    fiscal_year_end STRING COMMENT 'Fiscal year end date (MMDD format, e.g., "0531" for May 31)',
    
    -- Trading information
    -- NOTE: tickers and exchanges are parallel arrays (same length, matching indices)
    -- Example: tickers = ["UNM", "UNMA"], exchanges = ["NYSE", "NYSE"]
    -- This means UNM trades on NYSE and UNMA trades on NYSE
    -- Multiple tickers typically represent different share classes (common vs preferred/class B)
    -- The first ticker is usually the primary/common stock ticker
    tickers ARRAY COMMENT 'List of stock ticker symbols (e.g., ["AIR"]). Parallel array with exchanges - use same index to map ticker to exchange. Multiple tickers usually indicate different share classes (common vs preferred), not historical changes. All listed tickers are typically current/active.',
    exchanges ARRAY COMMENT 'List of stock exchanges where tickers trade (e.g., ["NYSE"]). Parallel array with tickers - use same index to map exchange to ticker. Common values: "NYSE", "NASDAQ", "AMEX", "OTC"',
    
    -- Insider transaction flags
    insider_transaction_for_issuer_exists INTEGER COMMENT '1 if insider transactions exist for issuer, 0 otherwise',
    insider_transaction_for_owner_exists INTEGER COMMENT '1 if insider transactions exist for owner, 0 otherwise',
    
    -- Flags and metadata
    flags STRING COMMENT 'Additional flags (typically empty)',
    former_names ARRAY COMMENT 'Array of former company names',
    
    -- Addresses (stored as VARIANT to preserve structure)
    addresses VARIANT COMMENT 'JSON object with "mailing" and "business" addresses. Each contains: street1, street2, city, stateOrCountry, zipCode, stateOrCountryDescription, isForeignLocation, foreignStateTerritory, country, countryCode',
    
    -- Filings metadata (stored as VARIANT - contains arrays of recent filings)
    filings VARIANT COMMENT 'JSON object with "recent" and "files" keys. "recent" contains parallel arrays: accessionNumber, filingDate, reportDate, acceptanceDateTime, act, form, fileNumber, filmNumber, items, core_type, size, isXBRL, isInlineXBRL, primaryDocument, primaryDocDescription',
    
    -- Audit fields
    ingest_date DATE COMMENT 'Date when this data was ingested',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when this record was created',
    
    PRIMARY KEY (cik, ingest_date)
)
COMMENT = 'SEC EDGAR company submissions metadata. Contains company information and filing history.';

-- Notes on ticker/exchange mapping:
-- - tickers[] and exchanges[] are parallel arrays (always same length)
-- - To map ticker to exchange: exchanges[tickers.index(ticker)]
-- - Multiple tickers are typically:
--   * Different share classes (common stock vs preferred/class B)
--   * Example: ["UNM", "UNMA"] - both are current, UNM is common, UNMA is preferred
--   * Example: ["UHAL", "UHAL-B"] - both are current, UHAL is common, UHAL-B is class B
-- - The first ticker in the array is typically the primary/common stock ticker
-- - All tickers listed are typically current/active (not historical)
-- - For historical ticker changes (e.g., due to rebranding), check filings with form "25-NSE" (Notice of Suspension)
-- - To determine which ticker is most commonly traded, you may need external market data (volume, market cap)

-- Notes on filings.recent structure:
-- - form: SEC form type (e.g., "10-K", "10-Q", "8-K", "3", "4", "144", "424B5", "DEF 14A", etc.)
--   There are 100+ possible form types. Common ones: 10-K, 10-Q, 8-K, 3, 4, 144, 424B5, DEF 14A, SCHEDULE 13G
-- - act: Securities Act type. Possible values: "", "33", "34", "40", "IC"
-- - core_type: Core document type, similar to form but may differ
-- - filingDate: Date in YYYY-MM-DD format
-- - reportDate: Report period end date in YYYY-MM-DD format (may be empty)
-- - acceptanceDateTime: ISO 8601 timestamp when SEC accepted the filing
-- - accessionNumber: Unique filing identifier (format: CIK-YY-XXXXXX)
-- - isXBRL: 1 if filing contains XBRL, 0 otherwise
-- - isInlineXBRL: 1 if filing contains inline XBRL, 0 otherwise
-- - size: File size in bytes
-- - items: Comma-separated list of item numbers (e.g., "1.01,2.03,8.01,9.01")
-- - primaryDocument: Filename of primary document
-- - primaryDocDescription: Description of primary document type

