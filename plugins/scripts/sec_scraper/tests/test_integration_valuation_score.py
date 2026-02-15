"""Integration test: spin up temp Postgres, deploy migrations, seed data, score top 10.

Launches an ephemeral PostgreSQL instance, runs all schema migrations, seeds it
with realistic fundamental + price data for 10 well-known mega-cap companies,
then runs the full valuation scoring pipeline and prints the results.

Handles the root-user case (common in Docker/CI) by creating a ``_pgtest``
system user and running PostgreSQL under that account.

Requires:
    - PostgreSQL binaries on the host (``initdb``, ``pg_ctl``)
    - ``psycopg2`` (already a project dependency)

Run with:
    ./scripts/run_with_venv.sh python -m pytest \\
        plugins/scripts/sec_scraper/tests/test_integration_valuation_score.py -v -s

The ``-s`` flag is important so the results table prints to stdout.
"""
from __future__ import annotations

import glob
import json
import logging
import os
import pwd
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional

import pytest

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seed data: 10 well-known mega-cap companies with realistic fundamentals.
# All dollar amounts in USD, shares in actual count.
# Sources: approximate FY2024/2025 10-K data (rounded for readability).
# ---------------------------------------------------------------------------
INGEST_DATE = "2026-02-15"
SCORE_DATE = "2026-02-15"

COMPANIES: List[Dict[str, Any]] = [
    {
        "cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc.",
        "price": 228.0, "shares": 15_115_000_000,
        "revenue": 391_000_000_000, "net_income": 97_000_000_000,
        "operating_income": 123_000_000_000, "fcf": 111_000_000_000,
        "assets": 353_000_000_000, "liabilities": 290_000_000_000,
        "equity": 63_000_000_000,
        "trailing_ni": [97e9, 97e9, 100e9, 95e9, 94.7e9],
    },
    {
        "cik": "0000789019", "ticker": "MSFT", "name": "Microsoft Corp.",
        "price": 412.0, "shares": 7_430_000_000,
        "revenue": 245_000_000_000, "net_income": 88_000_000_000,
        "operating_income": 109_000_000_000, "fcf": 74_000_000_000,
        "assets": 512_000_000_000, "liabilities": 243_000_000_000,
        "equity": 269_000_000_000,
        "trailing_ni": [88e9, 72e9, 72.4e9, 61.3e9, 61.3e9],
    },
    {
        "cik": "0001652044", "ticker": "GOOGL", "name": "Alphabet Inc.",
        "price": 182.0, "shares": 12_200_000_000,
        "revenue": 340_000_000_000, "net_income": 94_000_000_000,
        "operating_income": 112_000_000_000, "fcf": 72_000_000_000,
        "assets": 430_000_000_000, "liabilities": 119_000_000_000,
        "equity": 311_000_000_000,
        "trailing_ni": [94e9, 73.8e9, 60e9, 76e9, 40e9],
    },
    {
        "cik": "0001018724", "ticker": "AMZN", "name": "Amazon.com Inc.",
        "price": 218.0, "shares": 10_500_000_000,
        "revenue": 638_000_000_000, "net_income": 59_000_000_000,
        "operating_income": 69_000_000_000, "fcf": 39_000_000_000,
        "assets": 624_000_000_000, "liabilities": 418_000_000_000,
        "equity": 206_000_000_000,
        "trailing_ni": [59e9, 30.4e9, -2.7e9, 21.3e9, 33.4e9],
    },
    {
        "cik": "0001318605", "ticker": "TSLA", "name": "Tesla Inc.",
        "price": 340.0, "shares": 3_210_000_000,
        "revenue": 97_000_000_000, "net_income": 7_100_000_000,
        "operating_income": 7_600_000_000, "fcf": 3_600_000_000,
        "assets": 122_000_000_000, "liabilities": 48_000_000_000,
        "equity": 74_000_000_000,
        "trailing_ni": [7.1e9, 15e9, 12.6e9, 5.5e9, 0.86e9],
    },
    {
        "cik": "0000021344", "ticker": "KO", "name": "The Coca-Cola Company",
        "price": 63.0, "shares": 4_300_000_000,
        "revenue": 47_000_000_000, "net_income": 10_700_000_000,
        "operating_income": 13_700_000_000, "fcf": 9_500_000_000,
        "assets": 100_000_000_000, "liabilities": 68_000_000_000,
        "equity": 32_000_000_000,
        "trailing_ni": [10.7e9, 10.7e9, 9.5e9, 9.8e9, 7.7e9],
    },
    {
        "cik": "0000050863", "ticker": "JNJ", "name": "Johnson & Johnson",
        "price": 155.0, "shares": 2_400_000_000,
        "revenue": 86_000_000_000, "net_income": 14_600_000_000,
        "operating_income": 22_000_000_000, "fcf": 18_000_000_000,
        "assets": 187_000_000_000, "liabilities": 113_000_000_000,
        "equity": 74_000_000_000,
        "trailing_ni": [14.6e9, 35.2e9, 17.9e9, 17.9e9, 20.9e9],
    },
    {
        "cik": "0000078003", "ticker": "PFE", "name": "Pfizer Inc.",
        "price": 26.0, "shares": 5_650_000_000,
        "revenue": 58_000_000_000, "net_income": 8_000_000_000,
        "operating_income": 8_500_000_000, "fcf": 6_500_000_000,
        "assets": 214_000_000_000, "liabilities": 148_000_000_000,
        "equity": 66_000_000_000,
        "trailing_ni": [8e9, 2.1e9, 31.4e9, 100.3e9, 22e9],
    },
    {
        "cik": "0000051143", "ticker": "IBM", "name": "International Business Machines",
        "price": 260.0, "shares": 923_000_000,
        "revenue": 62_000_000_000, "net_income": 8_500_000_000,
        "operating_income": 10_200_000_000, "fcf": 11_000_000_000,
        "assets": 135_000_000_000, "liabilities": 111_000_000_000,
        "equity": 24_000_000_000,
        "trailing_ni": [8.5e9, 7.5e9, 1.6e9, 5.7e9, 5.7e9],
    },
    {
        "cik": "0000858877", "ticker": "CMG", "name": "Chipotle Mexican Grill",
        "price": 57.0, "shares": 1_360_000_000,
        "revenue": 11_300_000_000, "net_income": 1_530_000_000,
        "operating_income": 2_100_000_000, "fcf": 1_200_000_000,
        "assets": 9_000_000_000, "liabilities": 7_200_000_000,
        "equity": 1_800_000_000,
        "trailing_ni": [1.53e9, 1.23e9, 0.90e9, 0.81e9, 0.65e9],
    },
]


# ---------------------------------------------------------------------------
# Ephemeral PostgreSQL helper (works as root or non-root)
# ---------------------------------------------------------------------------

def _find_pg_bin(name: str) -> Optional[str]:
    """Find a PostgreSQL binary by name."""
    candidates = sorted(glob.glob(f"/usr/lib/postgresql/*/bin/{name}"), reverse=True)
    return candidates[0] if candidates else shutil.which(name)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class EphemeralPostgres:
    """Manage a temporary PostgreSQL server that works even when running as root.

    When running as root (common in Docker/CI), creates a ``_pgtest`` system
    user and runs all commands via ``su _pgtest -s /bin/sh -c '...'``.
    """

    def __init__(self):
        self._base_dir = tempfile.mkdtemp(prefix="pgtest_")
        self._data_dir = os.path.join(self._base_dir, "data")
        self._log_file = os.path.join(self._base_dir, "pg.log")
        self._port = _find_free_port()
        self._run_as: Optional[str] = None

        initdb = _find_pg_bin("initdb")
        self._pg_ctl = _find_pg_bin("pg_ctl")
        if not initdb or not self._pg_ctl:
            raise RuntimeError("PostgreSQL binaries (initdb, pg_ctl) not found")

        # If running as root, create a dedicated user.
        if os.getuid() == 0:
            self._run_as = "_pgtest"
            subprocess.run(
                ["useradd", "-r", "-s", "/usr/sbin/nologin", self._run_as],
                check=False, capture_output=True,
            )
            uid = pwd.getpwnam(self._run_as).pw_uid
            os.chown(self._base_dir, uid, -1)
            os.chmod(self._base_dir, 0o700)

        self._pg_user = self._run_as or os.environ.get("USER", "postgres")
        self._shell(f"{initdb} -D {self._data_dir} --lc-messages=C -A trust")
        self._start_server()

    def _shell(self, cmd: str, timeout: int = 30) -> subprocess.CompletedResult:
        """Run a shell command, optionally as the _pgtest user."""
        if self._run_as:
            full = ["su", self._run_as, "-s", "/bin/sh", "-c", cmd]
        else:
            full = ["/bin/sh", "-c", cmd]
        return subprocess.run(full, capture_output=True, timeout=timeout)

    def _start_server(self):
        opts = f"-p {self._port} -h 127.0.0.1 -k {self._base_dir}"
        self._shell(
            f"{self._pg_ctl} start -D {self._data_dir} "
            f'-l {self._log_file} -o "{opts}"'
        )
        # Wait for the server to accept connections.
        import psycopg2
        for _ in range(20):
            try:
                conn = psycopg2.connect(
                    host="127.0.0.1", port=self._port,
                    database="postgres", user=self._pg_user,
                    connect_timeout=2,
                )
                conn.close()
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError(f"PostgreSQL did not start on port {self._port}")

    def dsn(self) -> Dict[str, Any]:
        return {
            "host": "127.0.0.1",
            "port": self._port,
            "database": "postgres",
            "user": self._pg_user,
        }

    def stop(self):
        try:
            self._shell(f"{self._pg_ctl} stop -D {self._data_dir} -m fast")
        except Exception:
            pass
        try:
            shutil.rmtree(self._base_dir, ignore_errors=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pg_instance():
    """Spin up a temporary PostgreSQL instance for the test session."""
    try:
        pg = EphemeralPostgres()
    except Exception as exc:
        pytest.skip(f"Cannot start ephemeral PostgreSQL: {exc}")
    yield pg
    pg.stop()


@pytest.fixture(scope="module")
def db_conn(pg_instance):
    """Return a psycopg2 connection to the temp Postgres."""
    import psycopg2
    params = pg_instance.dsn()
    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        database=params["database"],
        user=params["user"],
    )
    conn.autocommit = False
    return conn


@pytest.fixture(scope="module")
def schema_ready(db_conn, pg_instance):
    """Run migrations against the temp Postgres and return the schema name."""
    from scripts.sec_scraper.postgres.deploy_migrations import MigrationRunner

    params = pg_instance.dsn()
    config = {
        "host": params["host"],
        "port": params["port"],
        "database": params["database"],
        "user": params["user"],
        "password": "",
        "schema": "sec_raw",
    }
    runner = MigrationRunner(config)
    applied, skipped, failed = runner.run_migrations()
    runner.close()

    assert failed == 0, f"{failed} migration(s) failed"
    assert applied > 0 or skipped > 0, "No migrations found"

    # Set search path on the test connection
    cur = db_conn.cursor()
    cur.execute("SET search_path TO sec_raw, public")
    cur.close()
    db_conn.commit()

    return "sec_raw"


@pytest.fixture(scope="module")
def seeded_db(db_conn, schema_ready):
    """Seed the temp Postgres with realistic company data and prices."""
    schema = schema_ready
    cur = db_conn.cursor()

    for co in COMPANIES:
        cik = co["cik"]
        ticker = co["ticker"]

        # --- submissions ---
        cur.execute(
            f"""
            INSERT INTO {schema}.submissions
                (cik, entity_name, tickers, exchanges, ingest_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (cik, co["name"], json.dumps([ticker]), json.dumps(["NYSE"]), INGEST_DATE),
        )

        # --- companyfacts_metadata ---
        cur.execute(
            f"""
            INSERT INTO {schema}.companyfacts_metadata
                (cik, entity_name, ingest_date)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (cik, co["name"], INGEST_DATE),
        )

        # --- companyfacts_facts: seed key metrics from 10-K ---
        metrics = [
            ("Revenues", "USD", co["revenue"]),
            ("NetIncomeLoss", "USD", co["net_income"]),
            ("OperatingIncomeLoss", "USD", co["operating_income"]),
            ("FreeCashFlow", "USD", co["fcf"]),
            ("Assets", "USD", co["assets"]),
            ("Liabilities", "USD", co["liabilities"]),
            ("StockholdersEquity", "USD", co["equity"]),
        ]
        for metric_name, unit, value in metrics:
            cur.execute(
                f"""
                INSERT INTO {schema}.companyfacts_facts
                    (cik, ingest_date, taxonomy, metric_name, unit,
                     period_end, value, accession_number,
                     fiscal_year, fiscal_period, form_type, filed_date)
                VALUES (%s, %s, 'us-gaap', %s, %s,
                        '2025-12-31', %s, %s,
                        2025, 'FY', '10-K', '2026-02-01')
                ON CONFLICT DO NOTHING
                """,
                (cik, INGEST_DATE, metric_name, unit, value,
                 f"0001-26-{cik[-6:]}"),
            )

        # Shares outstanding (dei taxonomy, unit=shares)
        cur.execute(
            f"""
            INSERT INTO {schema}.companyfacts_facts
                (cik, ingest_date, taxonomy, metric_name, unit,
                 period_end, value, accession_number,
                 fiscal_year, fiscal_period, form_type, filed_date)
            VALUES (%s, %s, 'dei', 'EntityCommonStockSharesOutstanding', 'shares',
                    '2025-12-31', %s, %s,
                    2025, 'FY', '10-K', '2026-02-01')
            ON CONFLICT DO NOTHING
            """,
            (cik, INGEST_DATE, co["shares"], f"0002-26-{cik[-6:]}"),
        )

        # Trailing net income (older fiscal years for consistency calc)
        for yr_offset, ni_val in enumerate(co["trailing_ni"]):
            fy = 2025 - yr_offset
            cur.execute(
                f"""
                INSERT INTO {schema}.companyfacts_facts
                    (cik, ingest_date, taxonomy, metric_name, unit,
                     period_end, value, accession_number,
                     fiscal_year, fiscal_period, form_type, filed_date)
                VALUES (%s, %s, 'us-gaap', 'NetIncomeLoss', 'USD',
                        %s, %s, %s,
                        %s, 'FY', '10-K', %s)
                ON CONFLICT DO NOTHING
                """,
                (cik, INGEST_DATE,
                 f"{fy}-12-31", ni_val, f"0003-{fy}-{cik[-6:]}",
                 fy, f"{fy + 1}-02-01"),
            )

        # --- ticker_prices_daily ---
        cur.execute(
            f"""
            INSERT INTO {schema}.ticker_prices_daily
                (ticker, price_date, close, source)
            VALUES (%s, %s, %s, 'seed')
            ON CONFLICT DO NOTHING
            """,
            (ticker, SCORE_DATE, co["price"]),
        )

    db_conn.commit()
    cur.close()

    return db_conn, schema


# ---------------------------------------------------------------------------
# The integration test
# ---------------------------------------------------------------------------

def test_score_top_10_by_market_cap(seeded_db):
    """Score 10 mega-cap companies and display results.

    Full pipeline exercised:
      1. Ephemeral Postgres spun up (via fixture)
      2. All migrations deployed
      3. Realistic fundamental + price data seeded for 10 companies
      4. build_fundamentals extracts metrics per company
      5. compute_ratios scores each company
      6. Scores upserted into company_valuation_scores table
      7. Results printed in formatted table for review
    """
    conn, schema = seeded_db

    from scripts.sec_scraper.tasks.company_valuation_score import (
        ValuationScore,
        build_fundamentals,
        compute_ratios,
        _fetch_latest_close_price,
        _fetch_scoreable_companies,
        _upsert_valuation_scores,
    )

    # Discover scoreable companies
    companies = _fetch_scoreable_companies(conn, schema, INGEST_DATE, limit=100)
    assert len(companies) == 10, f"Expected 10 seeded companies, got {len(companies)}"

    # Score each company
    scored: List[ValuationScore] = []
    skipped = 0
    for comp in companies:
        cik, ticker = comp["cik"], comp["ticker"]
        price = _fetch_latest_close_price(conn, schema, ticker, SCORE_DATE)
        if price is None:
            skipped += 1
            continue
        fundamentals = build_fundamentals(conn, schema, cik, ticker, INGEST_DATE)
        if fundamentals is None:
            skipped += 1
            continue
        vs = compute_ratios(fundamentals, price)
        vs = ValuationScore(**{**vs.__dict__, "score_date": SCORE_DATE})
        scored.append(vs)

    assert len(scored) == 10, (
        f"Expected all 10 companies to be scored, got {len(scored)} "
        f"(skipped={skipped})"
    )

    # Sort by market_cap descending
    scored.sort(key=lambda s: s.market_cap or 0, reverse=True)

    # Upsert to DB
    stored = _upsert_valuation_scores(conn, schema, scored)
    assert stored == 10

    # Verify data was persisted
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {schema}.company_valuation_scores")
    db_count = cur.fetchone()[0]
    cur.close()
    assert db_count == 10

    # Print results for review
    _print_results_table(scored, INGEST_DATE, SCORE_DATE)

    # --- Assertions ---
    for vs in scored:
        assert vs.market_cap is not None and vs.market_cap > 0
        assert vs.valuation_score is not None
        assert vs.score_grade in ("A", "B", "C", "D", "F")
        assert 0 <= vs.valuation_score <= 100

    # AAPL should have higher market cap than CMG
    aapl = next(s for s in scored if s.ticker == "AAPL")
    cmg = next(s for s in scored if s.ticker == "CMG")
    assert aapl.market_cap > cmg.market_cap

    # PFE should be cheaper (higher earnings yield) than TSLA
    pfe = next(s for s in scored if s.ticker == "PFE")
    tsla = next(s for s in scored if s.ticker == "TSLA")
    assert pfe.earnings_yield > tsla.earnings_yield

    # All companies should have ROE computed
    for vs in scored:
        assert vs.roe is not None, f"{vs.ticker} missing ROE"


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _print_results_table(
    results: List[Any],
    ingest_date: str,
    score_date: str,
):
    """Print a formatted summary table to stdout."""
    def _fmt_billions(v: Optional[float]) -> str:
        if v is None:
            return "       N/A"
        return f"{v / 1e9:>10.1f}B"

    def _fmt_pct(v: Optional[float]) -> str:
        if v is None:
            return "   N/A"
        return f"{v * 100:>6.1f}%"

    def _fmt_ratio(v: Optional[float]) -> str:
        if v is None:
            return "  N/A"
        return f"{v:>5.1f}"

    def _fmt_score(v: Optional[float]) -> str:
        if v is None:
            return " N/A"
        return f"{v:>4.0f}"

    print()
    print("=" * 130)
    print("  BERKSHIRE-STYLE VALUATION SCORES — Top 10 by Market Cap")
    print(f"  ingest_date={ingest_date}  score_date={score_date}")
    print("=" * 130)
    print()

    # Header
    hdr = (
        f"{'#':>2}  {'Ticker':<8} {'Grade':>5}  {'Score':>5}  "
        f"{'Mkt Cap':>11}  {'Price':>8}  "
        f"{'EY':>6}  {'FCF Y':>6}  {'P/B':>5}  "
        f"{'ROE':>6}  {'OpMgn':>6}  {'D/E':>5}  {'Consist':>7}  "
        f"{'FY':>4}"
    )
    print(hdr)
    print("-" * len(hdr))

    for i, vs in enumerate(results, 1):
        print(
            f"{i:>2}  {vs.ticker:<8} {vs.score_grade:>5}  {_fmt_score(vs.valuation_score):>5}  "
            f"{_fmt_billions(vs.market_cap):>11}  {vs.close_price:>8.2f}  "
            f"{_fmt_pct(vs.earnings_yield):>6}  {_fmt_pct(vs.fcf_yield):>6}  {_fmt_ratio(vs.price_to_book):>5}  "
            f"{_fmt_pct(vs.roe):>6}  {_fmt_pct(vs.operating_margin):>6}  {_fmt_ratio(vs.debt_to_equity):>5}  "
            f"{_fmt_pct(vs.earnings_consistency):>7}  "
            f"{vs.fiscal_year or 'N/A':>4}"
        )

    print()
    by_score = sorted(results, key=lambda s: s.valuation_score or 0, reverse=True)
    print("  Re-ranked by valuation score (best value first):")
    print()
    for i, vs in enumerate(by_score, 1):
        bar = "#" * int((vs.valuation_score or 0) / 2)
        print(
            f"    {i:>2}. {vs.ticker:<8} {vs.score_grade}  {_fmt_score(vs.valuation_score)}  "
            f"{bar}"
        )
    print()
    print("=" * 130)
