"""Tests for tasks/company_valuation_score.py.

Covers: scoring functions, ratio computation, earnings consistency,
composite score, grade assignment, and build_fundamentals via fake DB.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from scripts.sec_scraper.tasks.company_valuation_score import (
    CompanyFundamentals,
    ValuationScore,
    WEIGHTS,
    _clamp,
    assign_grade,
    compute_composite_score,
    compute_earnings_consistency,
    compute_ratios,
    score_debt_to_equity,
    score_earnings_consistency,
    score_earnings_yield,
    score_fcf_yield,
    score_operating_margin,
    score_price_to_book,
    score_roe,
)


# ---------------------------------------------------------------------------
# Helper: build a CompanyFundamentals with sensible defaults
# ---------------------------------------------------------------------------

def _make_fundamentals(
    *,
    cik: str = "0000320193",
    ticker: str = "AAPL",
    fiscal_year: int = 2024,
    net_income: Optional[float] = 100_000_000_000,
    total_equity: Optional[float] = 60_000_000_000,
    total_assets: Optional[float] = 350_000_000_000,
    total_liabilities: Optional[float] = 290_000_000_000,
    revenue: Optional[float] = 400_000_000_000,
    operating_income: Optional[float] = 120_000_000_000,
    free_cash_flow: Optional[float] = 110_000_000_000,
    shares_outstanding: Optional[int] = 15_000_000_000,
    trailing_net_incomes: Optional[List[float]] = None,
) -> CompanyFundamentals:
    if trailing_net_incomes is None:
        trailing_net_incomes = [100e9, 97e9, 95e9, 94e9, 90e9]
    return CompanyFundamentals(
        cik=cik,
        ticker=ticker,
        fiscal_year=fiscal_year,
        net_income=net_income,
        total_equity=total_equity,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        revenue=revenue,
        operating_income=operating_income,
        free_cash_flow=free_cash_flow,
        shares_outstanding=shares_outstanding,
        trailing_net_incomes=trailing_net_incomes,
    )


# ---------------------------------------------------------------------------
# _clamp
# ---------------------------------------------------------------------------

def test_clamp_within_range():
    assert _clamp(50.0) == 50.0


def test_clamp_below_min():
    assert _clamp(-10.0) == 0.0


def test_clamp_above_max():
    assert _clamp(150.0) == 100.0


def test_clamp_at_boundaries():
    assert _clamp(0.0) == 0.0
    assert _clamp(100.0) == 100.0


# ---------------------------------------------------------------------------
# score_earnings_yield
# ---------------------------------------------------------------------------

def test_earnings_yield_none():
    assert score_earnings_yield(None) is None


def test_earnings_yield_negative():
    assert score_earnings_yield(-0.05) == 0.0


def test_earnings_yield_zero():
    assert score_earnings_yield(0.0) == 0.0


def test_earnings_yield_5_percent():
    assert score_earnings_yield(0.05) == 50.0


def test_earnings_yield_10_percent():
    assert score_earnings_yield(0.10) == 100.0


def test_earnings_yield_15_percent_capped():
    # 15% → 150 clamped to 100
    assert score_earnings_yield(0.15) == 100.0


def test_earnings_yield_2_percent():
    assert score_earnings_yield(0.02) == 20.0


# ---------------------------------------------------------------------------
# score_fcf_yield
# ---------------------------------------------------------------------------

def test_fcf_yield_none():
    assert score_fcf_yield(None) is None


def test_fcf_yield_negative():
    assert score_fcf_yield(-0.03) == 0.0


def test_fcf_yield_8_percent():
    assert score_fcf_yield(0.08) == 80.0


def test_fcf_yield_10_percent():
    assert score_fcf_yield(0.10) == 100.0


# ---------------------------------------------------------------------------
# score_price_to_book
# ---------------------------------------------------------------------------

def test_price_to_book_none():
    assert score_price_to_book(None) is None


def test_price_to_book_negative():
    assert score_price_to_book(-1.0) == 0.0


def test_price_to_book_below_one():
    assert score_price_to_book(0.8) == 100.0


def test_price_to_book_exactly_one():
    assert score_price_to_book(1.0) == 100.0


def test_price_to_book_two():
    assert score_price_to_book(2.0) == 50.0


def test_price_to_book_five():
    assert score_price_to_book(5.0) == 20.0


def test_price_to_book_ten():
    assert score_price_to_book(10.0) == 10.0


# ---------------------------------------------------------------------------
# score_roe
# ---------------------------------------------------------------------------

def test_roe_none():
    assert score_roe(None) is None


def test_roe_negative():
    assert score_roe(-0.10) == 0.0


def test_roe_15_percent():
    assert score_roe(0.15) == 75.0


def test_roe_20_percent():
    assert score_roe(0.20) == 100.0


def test_roe_25_percent_capped():
    assert score_roe(0.25) == 100.0


def test_roe_10_percent():
    assert score_roe(0.10) == 50.0


# ---------------------------------------------------------------------------
# score_operating_margin
# ---------------------------------------------------------------------------

def test_operating_margin_none():
    assert score_operating_margin(None) is None


def test_operating_margin_negative():
    assert score_operating_margin(-0.05) == 0.0


def test_operating_margin_15_percent():
    assert score_operating_margin(0.15) == 50.0


def test_operating_margin_30_percent():
    assert score_operating_margin(0.30) == 100.0


def test_operating_margin_45_percent_capped():
    assert score_operating_margin(0.45) == 100.0


# ---------------------------------------------------------------------------
# score_debt_to_equity
# ---------------------------------------------------------------------------

def test_debt_to_equity_none():
    assert score_debt_to_equity(None) is None


def test_debt_to_equity_negative():
    assert score_debt_to_equity(-1.0) == 0.0


def test_debt_to_equity_zero():
    assert score_debt_to_equity(0.0) == 100.0


def test_debt_to_equity_one():
    # D/E=1 → 100 * (1 - 1/3) ≈ 66.67
    result = score_debt_to_equity(1.0)
    assert result is not None
    assert abs(result - 66.67) < 0.1


def test_debt_to_equity_three():
    assert score_debt_to_equity(3.0) == 0.0


def test_debt_to_equity_five():
    # D/E=5 → 100 * (1 - 5/3) = -66.67 clamped to 0
    assert score_debt_to_equity(5.0) == 0.0


# ---------------------------------------------------------------------------
# score_earnings_consistency
# ---------------------------------------------------------------------------

def test_earnings_consistency_none():
    assert score_earnings_consistency(None) is None


def test_earnings_consistency_perfect():
    assert score_earnings_consistency(1.0) == 100.0


def test_earnings_consistency_half():
    assert score_earnings_consistency(0.5) == 50.0


def test_earnings_consistency_zero():
    assert score_earnings_consistency(0.0) == 0.0


# ---------------------------------------------------------------------------
# compute_earnings_consistency
# ---------------------------------------------------------------------------

def test_earnings_consistency_insufficient_data():
    assert compute_earnings_consistency([100.0]) is None
    assert compute_earnings_consistency([]) is None


def test_earnings_consistency_stable_earnings():
    # Nearly identical earnings → high consistency
    incomes = [100.0, 100.0, 100.0, 100.0, 100.0]
    result = compute_earnings_consistency(incomes)
    assert result is not None
    assert result == 1.0  # zero stdev → CV=0 → consistency=1


def test_earnings_consistency_volatile_earnings():
    # Very volatile → low consistency
    incomes = [200.0, 50.0, 200.0, 50.0, 200.0]
    result = compute_earnings_consistency(incomes)
    assert result is not None
    assert result < 0.75


def test_earnings_consistency_all_negative():
    incomes = [-10.0, -20.0, -5.0]
    result = compute_earnings_consistency(incomes)
    assert result == 0.0


def test_earnings_consistency_mixed_sign():
    incomes = [100.0, -50.0, 80.0, -30.0]
    result = compute_earnings_consistency(incomes)
    assert result is not None
    # Mean is 25, but high stdev → low consistency
    assert result < 0.5


def test_earnings_consistency_growing():
    # Steadily growing earnings are slightly inconsistent but still decent
    incomes = [100.0, 90.0, 80.0, 70.0, 60.0]
    result = compute_earnings_consistency(incomes)
    assert result is not None
    assert result > 0.5


# ---------------------------------------------------------------------------
# compute_composite_score
# ---------------------------------------------------------------------------

def test_composite_score_all_present():
    scores = {
        "earnings_yield": 80.0,
        "fcf_yield": 70.0,
        "price_to_book": 60.0,
        "roe": 75.0,
        "operating_margin": 50.0,
        "debt_to_equity": 66.67,
        "earnings_consistency": 90.0,
    }
    result = compute_composite_score(scores)
    assert result is not None
    # Manual: 80*0.20 + 70*0.20 + 60*0.15 + 75*0.15 + 50*0.10 + 66.67*0.10 + 90*0.10
    #       = 16 + 14 + 9 + 11.25 + 5 + 6.667 + 9 = 70.917
    assert abs(result - 70.92) < 0.1


def test_composite_score_some_none():
    scores = {
        "earnings_yield": 80.0,
        "fcf_yield": None,
        "price_to_book": 60.0,
        "roe": None,
        "operating_margin": 50.0,
        "debt_to_equity": None,
        "earnings_consistency": None,
    }
    result = compute_composite_score(scores)
    assert result is not None
    # Re-normalized over: EY(0.20) + PB(0.15) + OM(0.10) = 0.45
    # = (80*0.20 + 60*0.15 + 50*0.10) / 0.45 = (16 + 9 + 5) / 0.45 = 66.67
    assert abs(result - 66.67) < 0.1


def test_composite_score_all_none():
    scores = {k: None for k in WEIGHTS}
    assert compute_composite_score(scores) is None


# ---------------------------------------------------------------------------
# assign_grade
# ---------------------------------------------------------------------------

def test_grade_a():
    assert assign_grade(85.0) == "A"
    assert assign_grade(80.0) == "A"
    assert assign_grade(100.0) == "A"


def test_grade_b():
    assert assign_grade(65.0) == "B"
    assert assign_grade(79.9) == "B"


def test_grade_c():
    assert assign_grade(50.0) == "C"
    assert assign_grade(64.9) == "C"


def test_grade_d():
    assert assign_grade(35.0) == "D"
    assert assign_grade(49.9) == "D"


def test_grade_f():
    assert assign_grade(0.0) == "F"
    assert assign_grade(34.9) == "F"


def test_grade_none():
    assert assign_grade(None) == "N/A"


# ---------------------------------------------------------------------------
# compute_ratios — full integration of scoring for one company
# ---------------------------------------------------------------------------

def test_compute_ratios_healthy_company():
    """AAPL-like: high quality, moderate valuation."""
    f = _make_fundamentals(
        net_income=100e9,
        total_equity=60e9,
        total_assets=350e9,
        total_liabilities=290e9,
        revenue=400e9,
        operating_income=120e9,
        free_cash_flow=110e9,
        shares_outstanding=15_000_000_000,
        trailing_net_incomes=[100e9, 97e9, 95e9, 94e9, 90e9],
    )
    # Price ~$200, market_cap = 3T
    vs = compute_ratios(f, close_price=200.0)

    assert vs.market_cap == 200.0 * 15e9  # 3T
    assert vs.cik == "0000320193"
    assert vs.ticker == "AAPL"

    # Earnings yield: 100B / 3T ≈ 3.33%
    assert vs.earnings_yield is not None
    assert abs(vs.earnings_yield - 0.0333) < 0.001

    # ROE: 100B / 60B ≈ 166.7% (extremely high)
    assert vs.roe is not None
    assert abs(vs.roe - 1.667) < 0.01

    # P/B: 3T / 60B = 50 (high)
    assert vs.price_to_book is not None
    assert abs(vs.price_to_book - 50.0) < 0.1

    # Composite should be non-None
    assert vs.valuation_score is not None
    assert vs.score_grade in ("A", "B", "C", "D", "F")


def test_compute_ratios_cheap_value_stock():
    """Classic Buffett value play: low P/E, reasonable quality."""
    f = _make_fundamentals(
        ticker="VALUE",
        net_income=5e9,
        total_equity=30e9,
        total_assets=50e9,
        total_liabilities=20e9,
        revenue=40e9,
        operating_income=8e9,
        free_cash_flow=6e9,
        shares_outstanding=1_000_000_000,
        trailing_net_incomes=[5e9, 4.8e9, 4.5e9, 4.7e9, 4.6e9],
    )
    # Price $30, market_cap = 30B → P/E = 6
    vs = compute_ratios(f, close_price=30.0)

    assert vs.market_cap == 30e9
    # EY: 5B / 30B ≈ 16.67% → score 100 (capped)
    assert vs.earnings_yield is not None
    assert vs.earnings_yield > 0.15
    assert vs.earnings_yield_score == 100.0

    # P/B: 30B / 30B = 1.0 → score 100
    assert vs.price_to_book is not None
    assert abs(vs.price_to_book - 1.0) < 0.01
    assert vs.price_to_book_score == 100.0

    # D/E: 20B / 30B ≈ 0.67 → good
    assert vs.debt_to_equity is not None
    assert abs(vs.debt_to_equity - 0.667) < 0.01

    # Should get a high composite
    assert vs.valuation_score is not None
    assert vs.valuation_score > 70


def test_compute_ratios_no_shares():
    """No shares outstanding → no market cap → limited scoring."""
    f = _make_fundamentals(shares_outstanding=None)
    vs = compute_ratios(f, close_price=100.0)

    assert vs.market_cap is None
    assert vs.earnings_yield is None
    assert vs.fcf_yield is None
    assert vs.price_to_book is None
    # Quality factors still computed
    assert vs.roe is not None
    assert vs.operating_margin is not None


def test_compute_ratios_zero_price():
    f = _make_fundamentals()
    vs = compute_ratios(f, close_price=0.0)
    assert vs.market_cap is None
    assert vs.earnings_yield is None


def test_compute_ratios_negative_equity():
    """Negative equity → P/B, ROE, D/E all degraded."""
    f = _make_fundamentals(
        total_equity=-5e9,
        total_liabilities=100e9,
    )
    vs = compute_ratios(f, close_price=50.0)

    assert vs.price_to_book is None  # equity <= 0, can't compute
    assert vs.roe is None
    assert vs.debt_to_equity is None


def test_compute_ratios_no_revenue():
    """Pre-revenue company → no operating margin."""
    f = _make_fundamentals(revenue=None, operating_income=None)
    vs = compute_ratios(f, close_price=10.0)
    assert vs.operating_margin is None
    assert vs.operating_margin_score is None


def test_compute_ratios_loss_making():
    """Company with net losses → low earnings scores."""
    f = _make_fundamentals(
        net_income=-2e9,
        free_cash_flow=-1e9,
        operating_income=-1.5e9,
        trailing_net_incomes=[-2e9, -1.5e9, -1e9],
    )
    vs = compute_ratios(f, close_price=20.0)

    assert vs.earnings_yield is not None
    assert vs.earnings_yield < 0
    assert vs.earnings_yield_score == 0.0
    assert vs.fcf_yield_score == 0.0
    assert vs.operating_margin_score == 0.0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_compute_ratios_all_none_fundamentals():
    """Company with no fundamental data → all scores None except consistency."""
    f = CompanyFundamentals(
        cik="0000000001",
        ticker="EMPTY",
        fiscal_year=2024,
        net_income=None,
        total_equity=None,
        total_assets=None,
        total_liabilities=None,
        revenue=None,
        operating_income=None,
        free_cash_flow=None,
        shares_outstanding=None,
        trailing_net_incomes=[],
    )
    vs = compute_ratios(f, close_price=10.0)
    assert vs.valuation_score is None
    assert vs.score_grade == "N/A"


def test_compute_ratios_score_date_set_by_caller():
    """score_date is left empty by compute_ratios; caller fills it."""
    f = _make_fundamentals()
    vs = compute_ratios(f, close_price=100.0)
    assert vs.score_date == ""
