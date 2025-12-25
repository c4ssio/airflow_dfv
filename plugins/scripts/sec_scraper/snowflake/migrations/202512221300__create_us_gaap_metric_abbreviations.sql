-- Mapping table for US-GAAP metric abbreviations
-- Provides common abbreviations for frequently used financial metrics
-- These abbreviations are based on standard financial/accounting terminology

CREATE OR REPLACE TABLE sec_raw.us_gaap_metric_abbreviations (
    metric_name STRING NOT NULL PRIMARY KEY COMMENT 'Full US-GAAP metric name (e.g., "EarningsPerShareBasic")',
    abbreviation STRING NOT NULL COMMENT 'Common abbreviation (e.g., "EPS_Basic")',
    category STRING COMMENT 'Metric category: "Income Statement", "Balance Sheet", "Cash Flow", "Per Share", "Ratios"',
    description STRING COMMENT 'Brief description of what the metric represents',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    UNIQUE (abbreviation)
)
COMMENT = 'Mapping of US-GAAP metric names to common abbreviations for easier querying and readability.';

-- Insert common metric abbreviations
INSERT INTO sec_raw.us_gaap_metric_abbreviations (metric_name, abbreviation, category, description) VALUES
-- Income Statement Metrics
('Revenues', 'REV', 'Income Statement', 'Total revenues'),
('SalesRevenueNet', 'SALES', 'Income Statement', 'Net sales revenue'),
('NetIncomeLoss', 'NI', 'Income Statement', 'Net income (loss)'),
('NetIncomeLossAvailableToCommonStockholdersBasic', 'NI_Common', 'Income Statement', 'Net income available to common stockholders (basic)'),
('OperatingIncomeLoss', 'OI', 'Income Statement', 'Operating income (loss)'),
('GrossProfit', 'GP', 'Income Statement', 'Gross profit'),
('CostOfGoodsAndServicesSold', 'COGS', 'Income Statement', 'Cost of goods and services sold'),
('OperatingExpenses', 'OPEX', 'Income Statement', 'Operating expenses'),
('SellingGeneralAndAdministrativeExpense', 'SG&A', 'Income Statement', 'Selling, general and administrative expenses'),
('ResearchAndDevelopmentExpense', 'R&D', 'Income Statement', 'Research and development expense'),
('InterestExpense', 'INT_EXP', 'Income Statement', 'Interest expense'),
('InterestIncome', 'INT_INC', 'Income Statement', 'Interest income'),
('IncomeTaxExpenseBenefit', 'TAX', 'Income Statement', 'Income tax expense (benefit)'),
('EarningsBeforeInterestTaxesDepreciationAndAmortization', 'EBITDA', 'Income Statement', 'EBITDA'),
('EarningsBeforeInterestAndTaxes', 'EBIT', 'Income Statement', 'EBIT'),

-- Balance Sheet Metrics
('Assets', 'ASSETS', 'Balance Sheet', 'Total assets'),
('AssetsCurrent', 'CURR_ASSETS', 'Balance Sheet', 'Current assets'),
('AssetsNoncurrent', 'NONCURR_ASSETS', 'Balance Sheet', 'Noncurrent assets'),
('Liabilities', 'LIAB', 'Balance Sheet', 'Total liabilities'),
('LiabilitiesCurrent', 'CURR_LIAB', 'Balance Sheet', 'Current liabilities'),
('LiabilitiesNoncurrent', 'NONCURR_LIAB', 'Balance Sheet', 'Noncurrent liabilities'),
('StockholdersEquity', 'EQUITY', 'Balance Sheet', 'Stockholders equity'),
('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'EQUITY_TOTAL', 'Balance Sheet', 'Total stockholders equity including noncontrolling interest'),
('CashAndCashEquivalentsAtCarryingValue', 'CASH', 'Balance Sheet', 'Cash and cash equivalents'),
('AccountsReceivableNetCurrent', 'AR', 'Balance Sheet', 'Accounts receivable (net, current)'),
('InventoryNet', 'INV', 'Balance Sheet', 'Inventory (net)'),
('PropertyPlantAndEquipmentNet', 'PP&E', 'Balance Sheet', 'Property, plant and equipment (net)'),
('Goodwill', 'GW', 'Balance Sheet', 'Goodwill'),
('IntangibleAssetsNetExcludingGoodwill', 'INTANG', 'Balance Sheet', 'Intangible assets (net, excluding goodwill)'),
('AccountsPayableCurrent', 'AP', 'Balance Sheet', 'Accounts payable (current)'),
('LongTermDebt', 'LTD', 'Balance Sheet', 'Long-term debt'),
('ShortTermDebt', 'STD', 'Balance Sheet', 'Short-term debt'),
('TotalDebt', 'DEBT', 'Balance Sheet', 'Total debt'),

-- Cash Flow Metrics
('NetCashProvidedByUsedInOperatingActivities', 'CFO', 'Cash Flow', 'Cash flow from operations'),
('NetCashProvidedByUsedInInvestingActivities', 'CFI', 'Cash Flow', 'Cash flow from investing'),
('NetCashProvidedByUsedInFinancingActivities', 'CFF', 'Cash Flow', 'Cash flow from financing'),
('CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecrease', 'CF_TOTAL', 'Cash Flow', 'Total change in cash and equivalents'),

-- Per Share Metrics
('EarningsPerShareBasic', 'EPS_Basic', 'Per Share', 'Basic earnings per share'),
('EarningsPerShareDiluted', 'EPS_Diluted', 'Per Share', 'Diluted earnings per share'),
('EarningsPerShareBasicAndDiluted', 'EPS', 'Per Share', 'Earnings per share (basic and diluted)'),
('BookValuePerShare', 'BVPS', 'Per Share', 'Book value per share'),
('CashFlowPerShare', 'CFPS', 'Per Share', 'Cash flow per share'),

-- Share Count Metrics
('WeightedAverageNumberOfSharesOutstandingBasic', 'SHARES_Basic', 'Per Share', 'Weighted average shares outstanding (basic)'),
('WeightedAverageNumberOfDilutedSharesOutstanding', 'SHARES_Diluted', 'Per Share', 'Weighted average shares outstanding (diluted)'),
('EntityCommonStockSharesOutstanding', 'SHARES_OUT', 'Per Share', 'Common shares outstanding'),

-- Ratios and Other Metrics
('PriceEarningsRatio', 'P/E', 'Ratios', 'Price-to-earnings ratio'),
('ReturnOnAssets', 'ROA', 'Ratios', 'Return on assets'),
('ReturnOnEquity', 'ROE', 'Ratios', 'Return on equity'),
('CurrentRatio', 'CURR_RATIO', 'Ratios', 'Current ratio (current assets / current liabilities)'),
('DebtToEquityRatio', 'D/E', 'Ratios', 'Debt-to-equity ratio'),
('GrossProfitMargin', 'GP_MARGIN', 'Ratios', 'Gross profit margin'),
('OperatingMargin', 'OP_MARGIN', 'Ratios', 'Operating margin'),
('NetProfitMargin', 'NET_MARGIN', 'Ratios', 'Net profit margin'),

-- Additional Common Metrics
('DepreciationDepletionAndAmortization', 'DDA', 'Income Statement', 'Depreciation, depletion and amortization'),
('AmortizationOfIntangibleAssets', 'AMORT', 'Income Statement', 'Amortization of intangible assets'),
('StockBasedCompensationExpense', 'SBC', 'Income Statement', 'Stock-based compensation expense'),
('CapitalExpenditures', 'CAPEX', 'Cash Flow', 'Capital expenditures'),
('FreeCashFlow', 'FCF', 'Cash Flow', 'Free cash flow'),
('WorkingCapital', 'WC', 'Balance Sheet', 'Working capital (current assets - current liabilities)')
;

-- Create a view that joins companyfacts_facts with abbreviations
CREATE OR REPLACE VIEW sec_raw.companyfacts_facts_with_abbrev AS
SELECT
    f.*,
    COALESCE(a.abbreviation, f.metric_name) AS metric_abbrev,
    a.category AS metric_category,
    a.description AS abbreviation_description
FROM 
    sec_raw.companyfacts_facts f
    LEFT JOIN sec_raw.us_gaap_metric_abbreviations a
        ON f.metric_name = a.metric_name
;

-- Note: Snowflake doesn't support COMMENT ON VIEW. View description:
-- Company facts with metric abbreviations. Use metric_abbrev for shorter column names in queries.
-- Example: SELECT cik, period_end, metric_abbrev, value FROM ... WHERE metric_abbrev = 'EPS_Basic'

-- Example queries:
--
-- Get EPS using abbreviation:
-- SELECT cik, period_end, value AS eps
-- FROM sec_raw.companyfacts_facts_with_abbrev
-- WHERE metric_abbrev = 'EPS_Basic'
--   AND cik = '0000001750'
-- ORDER BY period_end DESC;
--
-- Get all income statement metrics with abbreviations:
-- SELECT DISTINCT metric_abbrev, metric_name, description
-- FROM sec_raw.companyfacts_facts_with_abbrev
-- WHERE metric_category = 'Income Statement'
-- ORDER BY metric_abbrev;
--
-- Get common financial metrics for a company:
-- SELECT 
--     period_end,
--     MAX(CASE WHEN metric_abbrev = 'REV' THEN value END) AS revenue,
--     MAX(CASE WHEN metric_abbrev = 'NI' THEN value END) AS net_income,
--     MAX(CASE WHEN metric_abbrev = 'EPS_Basic' THEN value END) AS eps,
--     MAX(CASE WHEN metric_abbrev = 'ASSETS' THEN value END) AS assets,
--     MAX(CASE WHEN metric_abbrev = 'EQUITY' THEN value END) AS equity
-- FROM sec_raw.companyfacts_facts_with_abbrev
-- WHERE cik = '0000001750'
--   AND taxonomy = 'us-gaap'
-- GROUP BY period_end
-- ORDER BY period_end DESC;

