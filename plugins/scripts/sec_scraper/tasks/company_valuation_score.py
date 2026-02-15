"""Berkshire-style company valuation score.

Computes a composite cheapness/quality score for each company that has both
SEC fundamental data and daily ticker prices.  The methodology loosely follows
the value investing framework popularized by Buffett and Munger:

**Valuation factors** (is the stock cheap?):
  - Earnings yield  (net income / market cap; inverse P/E)
  - Free-cash-flow yield  (FCF / market cap)
  - Price-to-book  (market cap / equity)

**Quality factors** (is the business good?):
  - Return on equity  (net income / equity)
  - Operating margin  (operating income / revenue)
  - Debt-to-equity  (total liabilities / equity)
  - Earnings consistency  (1 − coefficient of variation of net income over
    trailing fiscal years)

Each factor is mapped to a 0–100 sub-score, then combined via fixed weights
into a single composite score (0–100).  Higher = better value.
"""
from __future__ import annotations

import logging
import math
import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from scripts.sec_scraper.common import Settings
from scripts.sec_scraper.postgres.helpers import get_postgres_config, get_postgres_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weights for the composite score.  Valuation factors get 55%, quality 45%.
# ---------------------------------------------------------------------------
WEIGHTS: Dict[str, float] = {
    "earnings_yield": 0.20,
    "fcf_yield": 0.20,
    "price_to_book": 0.15,
    "roe": 0.15,
    "operating_margin": 0.10,
    "debt_to_equity": 0.10,
    "earnings_consistency": 0.10,
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CompanyFundamentals:
    """Most-recent annual fundamentals for a single company."""
    cik: str
    ticker: str
    fiscal_year: int
    net_income: Optional[float]
    total_equity: Optional[float]
    total_assets: Optional[float]
    total_liabilities: Optional[float]
    revenue: Optional[float]
    operating_income: Optional[float]
    free_cash_flow: Optional[float]
    shares_outstanding: Optional[int]
    trailing_net_incomes: List[float]   # up to 5 years, newest first


@dataclass
class ValuationScore:
    """Full scoring result for one company."""
    cik: str
    ticker: str
    score_date: str
    close_price: Optional[float]
    shares_outstanding: Optional[int]
    market_cap: Optional[float]

    # Raw fundamentals
    net_income: Optional[float]
    total_equity: Optional[float]
    total_assets: Optional[float]
    total_liabilities: Optional[float]
    revenue: Optional[float]
    operating_income: Optional[float]
    free_cash_flow: Optional[float]
    fiscal_year: Optional[int]

    # Ratios
    earnings_yield: Optional[float]
    fcf_yield: Optional[float]
    price_to_book: Optional[float]
    roe: Optional[float]
    operating_margin: Optional[float]
    debt_to_equity: Optional[float]
    earnings_consistency: Optional[float]

    # Component scores (0-100)
    earnings_yield_score: Optional[float]
    fcf_yield_score: Optional[float]
    price_to_book_score: Optional[float]
    roe_score: Optional[float]
    operating_margin_score: Optional[float]
    debt_to_equity_score: Optional[float]
    earnings_consistency_score: Optional[float]

    # Composite
    valuation_score: Optional[float]
    score_grade: str

# ---------------------------------------------------------------------------
# Scoring functions — pure, stateless, easy to test
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def score_earnings_yield(ey: Optional[float]) -> Optional[float]:
    """Score earnings yield.

    Buffett's "owner earnings yield" comparison.
    0% → 0, 5% → 50, 10%+ → 90-100.  Negative earnings → 0.
    """
    if ey is None:
        return None
    if ey <= 0:
        return 0.0
    # Linear scale: 10% yield = score 100
    return _clamp(ey * 1000.0)  # 0.10 * 1000 = 100


def score_fcf_yield(fy: Optional[float]) -> Optional[float]:
    """Score free-cash-flow yield.

    Similar curve to earnings yield. FCF is arguably more important to
    Buffett than accounting earnings because it represents real cash.
    """
    if fy is None:
        return None
    if fy <= 0:
        return 0.0
    return _clamp(fy * 1000.0)


def score_price_to_book(pb: Optional[float]) -> Optional[float]:
    """Score price-to-book.

    Graham/early-Buffett factor. P/B <= 1 is ideal (score 100).
    P/B of 3 → 50, P/B of 5+ → ~10, negative book value → 0.
    """
    if pb is None:
        return None
    if pb <= 0:
        return 0.0  # negative book value
    if pb <= 1.0:
        return 100.0
    # Decay: 100 * (1/pb)  → P/B=2 → 50, P/B=5 → 20, P/B=10 → 10
    return _clamp(100.0 / pb)


def score_roe(roe: Optional[float]) -> Optional[float]:
    """Score return on equity.

    Buffett famously seeks ROE > 15%.
    0% → 0, 15% → 75, 20%+ → 90-100.  Negative → 0.
    """
    if roe is None:
        return None
    if roe <= 0:
        return 0.0
    # Scale: 20% ROE = 100
    return _clamp(roe * 500.0)  # 0.20 * 500 = 100


def score_operating_margin(om: Optional[float]) -> Optional[float]:
    """Score operating margin.

    High margins indicate pricing power / economic moat.
    0% → 0, 20% → 66, 30%+ → 100.  Negative → 0.
    """
    if om is None:
        return None
    if om <= 0:
        return 0.0
    # Scale: 30% margin = 100
    return _clamp(om / 0.30 * 100.0)


def score_debt_to_equity(de: Optional[float]) -> Optional[float]:
    """Score debt-to-equity (inverse — lower is better).

    Buffett prefers conservative balance sheets.
    D/E = 0 → 100, D/E = 1 → 50, D/E >= 3 → ~0.
    Negative equity → 0.
    """
    if de is None:
        return None
    if de < 0:
        return 0.0  # negative equity
    # Linear decay from 100 at D/E=0 to 0 at D/E=3
    return _clamp(100.0 * (1.0 - de / 3.0))


def score_earnings_consistency(consistency: Optional[float]) -> Optional[float]:
    """Score earnings consistency (0-1 scale input, where 1 = perfectly consistent).

    Buffett wants predictable businesses. Score maps consistency directly.
    """
    if consistency is None:
        return None
    return _clamp(consistency * 100.0)


def compute_earnings_consistency(trailing_incomes: List[float]) -> Optional[float]:
    """Compute earnings consistency as 1 − coefficient of variation.

    Returns a value in [0, 1] where 1 means perfectly stable earnings.
    Requires at least 2 years of data; returns None otherwise.
    Ignores years with zero or negative net income for the mean, but
    any negative year reduces consistency (the CV will be high).
    """
    if len(trailing_incomes) < 2:
        return None

    mean = statistics.mean(trailing_incomes)
    if mean <= 0:
        return 0.0  # persistent losses → inconsistent

    stdev = statistics.stdev(trailing_incomes)
    cv = stdev / abs(mean)
    # Cap CV at 2 to avoid negative scores; CV > 2 → very inconsistent
    return max(0.0, min(1.0, 1.0 - cv / 2.0))


def compute_composite_score(
    component_scores: Dict[str, Optional[float]],
    weights: Dict[str, float] | None = None,
) -> Optional[float]:
    """Weighted average of non-None component scores, re-normalized."""
    w = weights or WEIGHTS
    total_weight = 0.0
    weighted_sum = 0.0
    for key, weight in w.items():
        score = component_scores.get(key)
        if score is not None:
            weighted_sum += score * weight
            total_weight += weight

    if total_weight == 0:
        return None
    return round(weighted_sum / total_weight, 2)


def assign_grade(score: Optional[float]) -> str:
    """Map composite score to a letter grade."""
    if score is None:
        return "N/A"
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    if score >= 35:
        return "D"
    return "F"


# ---------------------------------------------------------------------------
# Ratio computation
# ---------------------------------------------------------------------------

def compute_ratios(
    fundamentals: CompanyFundamentals,
    close_price: float,
) -> ValuationScore:
    """Compute all ratios and scores for a single company."""
    shares = fundamentals.shares_outstanding
    market_cap: Optional[float] = None
    if shares and shares > 0 and close_price > 0:
        market_cap = close_price * shares

    ni = fundamentals.net_income
    equity = fundamentals.total_equity
    assets = fundamentals.total_assets
    liab = fundamentals.total_liabilities
    rev = fundamentals.revenue
    oi = fundamentals.operating_income
    fcf = fundamentals.free_cash_flow

    # Ratios
    earnings_yield = (ni / market_cap) if (ni is not None and market_cap and market_cap > 0) else None
    fcf_yield = (fcf / market_cap) if (fcf is not None and market_cap and market_cap > 0) else None
    price_to_book = (market_cap / equity) if (market_cap and equity and equity > 0) else None
    roe = (ni / equity) if (ni is not None and equity and equity > 0) else None
    operating_margin = (oi / rev) if (oi is not None and rev and rev > 0) else None
    debt_to_equity = (liab / equity) if (liab is not None and equity and equity > 0) else None
    earnings_consistency = compute_earnings_consistency(fundamentals.trailing_net_incomes)

    # Component scores
    ey_score = score_earnings_yield(earnings_yield)
    fy_score = score_fcf_yield(fcf_yield)
    pb_score = score_price_to_book(price_to_book)
    roe_score_val = score_roe(roe)
    om_score = score_operating_margin(operating_margin)
    de_score = score_debt_to_equity(debt_to_equity)
    ec_score = score_earnings_consistency(earnings_consistency)

    component_scores = {
        "earnings_yield": ey_score,
        "fcf_yield": fy_score,
        "price_to_book": pb_score,
        "roe": roe_score_val,
        "operating_margin": om_score,
        "debt_to_equity": de_score,
        "earnings_consistency": ec_score,
    }

    composite = compute_composite_score(component_scores)
    grade = assign_grade(composite)

    return ValuationScore(
        cik=fundamentals.cik,
        ticker=fundamentals.ticker,
        score_date="",  # filled in by caller
        close_price=close_price,
        shares_outstanding=shares,
        market_cap=market_cap,
        net_income=ni,
        total_equity=equity,
        total_assets=assets,
        total_liabilities=liab,
        revenue=rev,
        operating_income=oi,
        free_cash_flow=fcf,
        fiscal_year=fundamentals.fiscal_year,
        earnings_yield=earnings_yield,
        fcf_yield=fcf_yield,
        price_to_book=price_to_book,
        roe=roe,
        operating_margin=operating_margin,
        debt_to_equity=debt_to_equity,
        earnings_consistency=earnings_consistency,
        earnings_yield_score=ey_score,
        fcf_yield_score=fy_score,
        price_to_book_score=pb_score,
        roe_score=roe_score_val,
        operating_margin_score=om_score,
        debt_to_equity_score=de_score,
        earnings_consistency_score=ec_score,
        valuation_score=composite,
        score_grade=grade,
    )


# ---------------------------------------------------------------------------
# Database queries — extract fundamentals and prices
# ---------------------------------------------------------------------------

# Metrics we need from companyfacts_facts (us-gaap taxonomy).
# For each we list fallback metric names to try (SEC data is inconsistent).
_METRIC_LOOKUPS: Dict[str, List[str]] = {
    "net_income": ["NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic", "ProfitLoss"],
    "total_equity": ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
    "revenue": ["Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerExcludingAssessedTax"],
    "operating_income": ["OperatingIncomeLoss"],
    "free_cash_flow": ["FreeCashFlow"],
    "shares_outstanding": [
        "EntityCommonStockSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "CommonStockSharesOutstanding",
    ],
}


def _fetch_latest_annual_metric(
    conn: Any,
    schema: str,
    cik: str,
    metric_names: List[str],
    ingest_date: str,
) -> Optional[Tuple[float, int]]:
    """Return (value, fiscal_year) for the most recent 10-K filing of the given metric."""
    cur = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(metric_names))
        cur.execute(
            f"""
            SELECT value, fiscal_year
            FROM {schema}.companyfacts_facts
            WHERE cik = %s
              AND ingest_date = %s
              AND taxonomy = 'us-gaap'
              AND metric_name IN ({placeholders})
              AND unit = 'USD'
              AND form_type = '10-K'
              AND value IS NOT NULL
              AND fiscal_year IS NOT NULL
            ORDER BY fiscal_year DESC, period_end DESC
            LIMIT 1
            """,
            [cik, ingest_date] + metric_names,
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return (float(row[0]), int(row[1]))
        return None
    finally:
        cur.close()


def _fetch_trailing_net_incomes(
    conn: Any,
    schema: str,
    cik: str,
    ingest_date: str,
    years: int = 5,
) -> List[float]:
    """Return up to `years` annual net income values, newest first."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT DISTINCT ON (fiscal_year) value
            FROM {schema}.companyfacts_facts
            WHERE cik = %s
              AND ingest_date = %s
              AND taxonomy = 'us-gaap'
              AND metric_name IN ('NetIncomeLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic', 'ProfitLoss')
              AND unit = 'USD'
              AND form_type = '10-K'
              AND value IS NOT NULL
              AND fiscal_year IS NOT NULL
            ORDER BY fiscal_year DESC, period_end DESC
            LIMIT %s
            """,
            [cik, ingest_date, years],
        )
        return [float(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    finally:
        cur.close()


def _fetch_shares_outstanding(
    conn: Any,
    schema: str,
    cik: str,
    ingest_date: str,
) -> Optional[int]:
    """Return shares outstanding from dei taxonomy first, then us-gaap fallbacks."""
    cur = conn.cursor()
    try:
        # dei taxonomy has EntityCommonStockSharesOutstanding (shares, not USD)
        cur.execute(
            f"""
            SELECT value
            FROM {schema}.companyfacts_facts
            WHERE cik = %s
              AND ingest_date = %s
              AND metric_name = 'EntityCommonStockSharesOutstanding'
              AND unit = 'shares'
              AND value IS NOT NULL
              AND value > 0
            ORDER BY period_end DESC
            LIMIT 1
            """,
            [cik, ingest_date],
        )
        row = cur.fetchone()
        if row and row[0]:
            return int(float(row[0]))

        # Fallback: us-gaap shares outstanding
        cur.execute(
            f"""
            SELECT value
            FROM {schema}.companyfacts_facts
            WHERE cik = %s
              AND ingest_date = %s
              AND taxonomy = 'us-gaap'
              AND metric_name IN (
                  'WeightedAverageNumberOfSharesOutstandingBasic',
                  'WeightedAverageNumberOfDilutedSharesOutstanding',
                  'CommonStockSharesOutstanding'
              )
              AND unit = 'shares'
              AND form_type = '10-K'
              AND value IS NOT NULL
              AND value > 0
            ORDER BY fiscal_year DESC, period_end DESC
            LIMIT 1
            """,
            [cik, ingest_date],
        )
        row = cur.fetchone()
        if row and row[0]:
            return int(float(row[0]))
        return None
    finally:
        cur.close()


def _fetch_latest_close_price(
    conn: Any, schema: str, ticker: str, score_date: str
) -> Optional[float]:
    """Return the most recent close price on or before score_date."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT close
            FROM {schema}.ticker_prices_daily
            WHERE ticker = %s
              AND price_date <= %s::date
              AND close IS NOT NULL
              AND close > 0
            ORDER BY price_date DESC
            LIMIT 1
            """,
            [ticker, score_date],
        )
        row = cur.fetchone()
        if row and row[0]:
            return float(row[0])
        return None
    finally:
        cur.close()


def _fetch_scoreable_companies(
    conn: Any, schema: str, ingest_date: str, *, limit: int
) -> List[Dict[str, str]]:
    """Return companies that have both fundamentals and a ticker."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT DISTINCT tm.cik, tm.ticker
            FROM {schema}.submissions_ticker_mapping tm
            WHERE tm.ingest_date = %s
              AND tm.is_likely_common_stock = TRUE
              AND tm.ticker IS NOT NULL
              AND tm.ticker <> ''
              AND EXISTS (
                  SELECT 1 FROM {schema}.companyfacts_facts cf
                  WHERE cf.cik = tm.cik AND cf.ingest_date = %s
              )
            ORDER BY tm.ticker
            LIMIT %s
            """,
            [ingest_date, ingest_date, limit],
        )
        return [{"cik": r[0], "ticker": r[1]} for r in cur.fetchall()]
    finally:
        cur.close()


def build_fundamentals(
    conn: Any,
    schema: str,
    cik: str,
    ticker: str,
    ingest_date: str,
) -> Optional[CompanyFundamentals]:
    """Assemble a CompanyFundamentals from the database for one company."""
    fiscal_year: Optional[int] = None
    metrics: Dict[str, Optional[float]] = {}

    for field, metric_names in _METRIC_LOOKUPS.items():
        if field == "shares_outstanding":
            continue  # handled separately
        result = _fetch_latest_annual_metric(conn, schema, cik, metric_names, ingest_date)
        if result:
            metrics[field] = result[0]
            if fiscal_year is None:
                fiscal_year = result[1]
        else:
            metrics[field] = None

    if fiscal_year is None:
        return None  # no annual data at all

    shares = _fetch_shares_outstanding(conn, schema, cik, ingest_date)
    trailing = _fetch_trailing_net_incomes(conn, schema, cik, ingest_date)

    return CompanyFundamentals(
        cik=cik,
        ticker=ticker,
        fiscal_year=fiscal_year,
        net_income=metrics.get("net_income"),
        total_equity=metrics.get("total_equity"),
        total_assets=metrics.get("total_assets"),
        total_liabilities=metrics.get("total_liabilities"),
        revenue=metrics.get("revenue"),
        operating_income=metrics.get("operating_income"),
        free_cash_flow=metrics.get("free_cash_flow"),
        shares_outstanding=shares,
        trailing_net_incomes=trailing,
    )


def _upsert_valuation_scores(
    conn: Any, schema: str, scores: List[ValuationScore]
) -> int:
    """Upsert valuation scores into PostgreSQL."""
    if not scores:
        return 0
    cur = conn.cursor()
    try:
        cur.executemany(
            f"""
            INSERT INTO {schema}.company_valuation_scores (
                cik, ticker, score_date,
                close_price, shares_outstanding, market_cap,
                net_income, total_equity, total_assets, total_liabilities,
                revenue, operating_income, free_cash_flow, fiscal_year,
                earnings_yield, fcf_yield, price_to_book,
                roe, operating_margin, debt_to_equity, earnings_consistency,
                earnings_yield_score, fcf_yield_score, price_to_book_score,
                roe_score, operating_margin_score, debt_to_equity_score,
                earnings_consistency_score,
                valuation_score, score_grade,
                created_at
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (cik, ticker, score_date) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                shares_outstanding = EXCLUDED.shares_outstanding,
                market_cap = EXCLUDED.market_cap,
                net_income = EXCLUDED.net_income,
                total_equity = EXCLUDED.total_equity,
                total_assets = EXCLUDED.total_assets,
                total_liabilities = EXCLUDED.total_liabilities,
                revenue = EXCLUDED.revenue,
                operating_income = EXCLUDED.operating_income,
                free_cash_flow = EXCLUDED.free_cash_flow,
                fiscal_year = EXCLUDED.fiscal_year,
                earnings_yield = EXCLUDED.earnings_yield,
                fcf_yield = EXCLUDED.fcf_yield,
                price_to_book = EXCLUDED.price_to_book,
                roe = EXCLUDED.roe,
                operating_margin = EXCLUDED.operating_margin,
                debt_to_equity = EXCLUDED.debt_to_equity,
                earnings_consistency = EXCLUDED.earnings_consistency,
                earnings_yield_score = EXCLUDED.earnings_yield_score,
                fcf_yield_score = EXCLUDED.fcf_yield_score,
                price_to_book_score = EXCLUDED.price_to_book_score,
                roe_score = EXCLUDED.roe_score,
                operating_margin_score = EXCLUDED.operating_margin_score,
                debt_to_equity_score = EXCLUDED.debt_to_equity_score,
                earnings_consistency_score = EXCLUDED.earnings_consistency_score,
                valuation_score = EXCLUDED.valuation_score,
                score_grade = EXCLUDED.score_grade,
                created_at = CURRENT_TIMESTAMP
            """,
            [
                (
                    s.cik, s.ticker, s.score_date,
                    s.close_price, s.shares_outstanding, s.market_cap,
                    s.net_income, s.total_equity, s.total_assets, s.total_liabilities,
                    s.revenue, s.operating_income, s.free_cash_flow, s.fiscal_year,
                    s.earnings_yield, s.fcf_yield, s.price_to_book,
                    s.roe, s.operating_margin, s.debt_to_equity, s.earnings_consistency,
                    s.earnings_yield_score, s.fcf_yield_score, s.price_to_book_score,
                    s.roe_score, s.operating_margin_score, s.debt_to_equity_score,
                    s.earnings_consistency_score,
                    s.valuation_score, s.score_grade,
                )
                for s in scores
            ],
        )
        conn.commit()
        return len(scores)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_valuation_scores(
    cfg: Settings,
    *,
    score_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute and store Berkshire-style valuation scores for all scoreable companies.

    Parameters
    ----------
    cfg : Settings
        Pipeline settings (used for postgres config path, max_ciks).
    score_date : str | None
        Date for price lookup (YYYY-MM-DD).  Defaults to today.
    """
    import os
    from datetime import datetime, timezone

    score_date = score_date or os.environ.get("SEC_PRICE_DATE", "").strip()
    if not score_date:
        score_date = datetime.now(timezone.utc).date().isoformat()

    max_companies = cfg.ingest_max_ciks if cfg.ingest_max_ciks > 0 else cfg.max_ciks

    postgres_config = get_postgres_config(cfg.postgres_config_path)
    schema = postgres_config.get("schema", "sec_raw")
    conn = get_postgres_connection(postgres_config, schema)

    try:
        # Find latest ingest_date
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(ingest_date) FROM {schema}.submissions")
        latest_ingest = cur.fetchone()[0]
        cur.close()

        if not latest_ingest:
            logger.info("No submissions data found — skipping valuation scoring.")
            return {"score_date": score_date, "companies": 0, "scored": 0, "skipped": 0}

        ingest_date = str(latest_ingest)
        companies = _fetch_scoreable_companies(conn, schema, ingest_date, limit=max_companies)
        logger.info("Found %d scoreable companies for ingest_date=%s", len(companies), ingest_date)

        scored: List[ValuationScore] = []
        skipped = 0
        errors = 0

        for comp in companies:
            cik, ticker = comp["cik"], comp["ticker"]
            try:
                price = _fetch_latest_close_price(conn, schema, ticker, score_date)
                if price is None:
                    skipped += 1
                    continue

                fundamentals = build_fundamentals(conn, schema, cik, ticker, ingest_date)
                if fundamentals is None:
                    skipped += 1
                    continue

                vs = compute_ratios(fundamentals, price)
                vs = ValuationScore(
                    **{**vs.__dict__, "score_date": score_date}
                )
                scored.append(vs)
            except Exception as e:
                logger.warning("Error scoring cik=%s ticker=%s: %s", cik, ticker, e)
                errors += 1

        stored = _upsert_valuation_scores(conn, schema, scored)
        logger.info(
            "Valuation scores: scored=%d skipped=%d errors=%d stored=%d score_date=%s",
            len(scored), skipped, errors, stored, score_date,
        )

        # Log top scores
        top = sorted(scored, key=lambda s: s.valuation_score or 0, reverse=True)[:10]
        if top:
            logger.info("Top 10 by valuation score:")
            for vs in top:
                logger.info(
                    "  %s (CIK %s): score=%.1f grade=%s EY=%.1f%% ROE=%.1f%% P/B=%.1f D/E=%.1f",
                    vs.ticker, vs.cik,
                    vs.valuation_score or 0,
                    vs.score_grade,
                    (vs.earnings_yield or 0) * 100,
                    (vs.roe or 0) * 100,
                    vs.price_to_book or 0,
                    vs.debt_to_equity or 0,
                )

        return {
            "score_date": score_date,
            "ingest_date": ingest_date,
            "companies": len(companies),
            "scored": len(scored),
            "skipped": skipped,
            "errors": errors,
            "stored": stored,
        }
    finally:
        conn.close()
