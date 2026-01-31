from __future__ import annotations

from typing import Any, Dict

from scripts.sec_scraper.common import (
    convert_companyfacts_to_ndjson,
    convert_submissions_to_ndjson,
    pad_cik,
    validate_date_string,
    validate_integer,
    validate_numeric,
)


def test_pad_cik_zero_pads_to_10_digits():
    assert pad_cik("1") == "0000000001"
    assert pad_cik("1234567890") == "1234567890"


def test_validate_numeric_handles_strings_and_invalid():
    cik = "0000000001"
    assert validate_numeric(10, "value", cik) == 10
    assert validate_numeric("10", "value", cik) == 10
    assert validate_numeric("10.5", "value", cik) == 10.5
    # Invalid string becomes None
    assert validate_numeric("not-a-number", "value", cik) is None


def test_validate_integer_handles_int_float_and_string():
    cik = "0000000001"
    assert validate_integer(10, "fy", cik) == 10
    assert validate_integer(10.0, "fy", cik) == 10
    assert validate_integer("10", "fy", cik) == 10
    # Non-integer string becomes None
    assert validate_integer("10.5", "fy", cik) is None


def test_validate_date_string_accepts_reasonable_dates():
    cik = "0000000001"
    assert validate_date_string("2025-01-01", "end", cik) == "2025-01-01"
    # Non-string returns a stringified version
    assert validate_date_string(123, "end", cik) == "123"


def test_convert_submissions_to_ndjson_basic_fields_and_truncation():
    data: Dict[str, Any] = {
        "name": "Example Corp",
        "entityType": "operating",
        "tickers": ["AAA", "BBB"],
        "exchanges": ["NYSE"],
    }
    row = convert_submissions_to_ndjson(data, "1234", "2025-01-01")

    assert row["cik"] == "0000001234"
    assert row["entity_name"] == "Example Corp"
    assert row["entity_type"] == "operating"
    # tickers/exchanges truncated to shortest length
    assert row["tickers"] == ["AAA"]
    assert row["exchanges"] == ["NYSE"]
    assert row["ingest_date"] == "2025-01-01"


def test_convert_companyfacts_to_ndjson_builds_metadata_and_facts():
    data: Dict[str, Any] = {
        "entityName": "Example Corp",
        "facts": {
            "us-gaap": {
                "Assets": {
                    "label": "Assets",
                    "description": "Total assets",
                    "units": {
                        "USD": [
                            {
                                "fy": 2024,
                                "val": 100,
                                "end": "2024-12-31",
                                "filed": "2025-02-01",
                                "accn": "0000000000-24-000001",
                                "fp": "FY",
                                "form": "10-K",
                                "frame": "CY2024",
                            }
                        ]
                    },
                }
            }
        },
    }

    metadata_rows, facts_rows, metric_metadata_rows = convert_companyfacts_to_ndjson(
        data, "1234", "2025-01-01"
    )

    assert len(metadata_rows) == 1
    assert metadata_rows[0]["cik"] == "0000001234"
    assert metadata_rows[0]["entity_name"] == "Example Corp"

    assert len(facts_rows) == 1
    fact = facts_rows[0]
    assert fact["cik"] == "0000001234"
    assert fact["taxonomy"] == "us-gaap"
    assert fact["metric_name"] == "Assets"
    assert fact["unit"] == "USD"
    assert fact["value"] == 100
    assert fact["fiscal_year"] == 2024

    assert len(metric_metadata_rows) == 1
    meta = metric_metadata_rows[0]
    assert meta["taxonomy"] == "us-gaap"
    assert meta["metric_name"] == "Assets"
    assert meta["label"] == "Assets"
    assert meta["description"] == "Total assets"

