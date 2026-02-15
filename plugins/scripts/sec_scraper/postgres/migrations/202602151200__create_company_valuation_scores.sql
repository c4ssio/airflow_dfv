-- Company valuation scores: Berkshire-style cheapness/quality composite
-- Stores per-company scores based on fundamentals vs price.
--
-- The score combines valuation (cheapness) and quality factors in the
-- spirit of Buffett/Munger value investing:
--   Valuation: earnings yield, FCF yield, price-to-book
--   Quality:   ROE, operating margin, debt/equity, earnings consistency
--
-- Each factor is scored 0-100 and weighted into a composite score.
-- Higher composite = better combination of cheap + quality.

CREATE TABLE IF NOT EXISTS sec_raw.company_valuation_scores (
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    score_date DATE NOT NULL,

    -- Price & market data used
    close_price NUMERIC,
    shares_outstanding BIGINT,
    market_cap NUMERIC,

    -- Raw fundamental inputs (most recent annual)
    net_income NUMERIC,
    total_equity NUMERIC,
    total_assets NUMERIC,
    total_liabilities NUMERIC,
    revenue NUMERIC,
    operating_income NUMERIC,
    free_cash_flow NUMERIC,
    fiscal_year INTEGER,

    -- Computed ratios
    earnings_yield NUMERIC,         -- net_income / market_cap
    fcf_yield NUMERIC,              -- free_cash_flow / market_cap
    price_to_book NUMERIC,          -- market_cap / total_equity
    roe NUMERIC,                    -- net_income / total_equity
    operating_margin NUMERIC,       -- operating_income / revenue
    debt_to_equity NUMERIC,         -- total_liabilities / total_equity
    earnings_consistency NUMERIC,   -- 1 - CV of net income over trailing years

    -- Component scores (0-100 each)
    earnings_yield_score NUMERIC,
    fcf_yield_score NUMERIC,
    price_to_book_score NUMERIC,
    roe_score NUMERIC,
    operating_margin_score NUMERIC,
    debt_to_equity_score NUMERIC,
    earnings_consistency_score NUMERIC,

    -- Composite
    valuation_score NUMERIC,        -- weighted average of component scores (0-100)
    score_grade TEXT,                -- A/B/C/D/F letter grade

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (cik, ticker, score_date)
);

CREATE INDEX IF NOT EXISTS idx_valuation_scores_ticker
    ON sec_raw.company_valuation_scores(ticker);

CREATE INDEX IF NOT EXISTS idx_valuation_scores_date
    ON sec_raw.company_valuation_scores(score_date);

CREATE INDEX IF NOT EXISTS idx_valuation_scores_composite
    ON sec_raw.company_valuation_scores(valuation_score DESC);

COMMENT ON TABLE sec_raw.company_valuation_scores IS
'Berkshire-style company valuation scores combining cheapness and quality factors. Higher score = better value.';
