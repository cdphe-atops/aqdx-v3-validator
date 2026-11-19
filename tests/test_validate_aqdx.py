from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from validate_aqdx import iter_dataframe_rows, process_file

TEST_DIR = Path(__file__).parent.parent / "test_files"


def test_repair_formatting_edge_cases():
    """Verifies specific formatting artifacts are repaired and logged correctly."""
    file_path = TEST_DIR / "repair_formatting_edge_cases.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    results = process_file(str(file_path))

    assert len(results["errors"]) == 0
    assert len(results["warnings"]) == 0
    assert len(results["repairs"]) > 0

    df = results["repaired_dataframe"]
    assert not df.empty

    assert df.loc[0, "data_steward_name"] == "cdphe_apcd_atops"
    assert df.loc[1, "elevation"] == "1609.34"
    assert df.loc[2, "device_id"] == '"CAT-01"'
    assert pd.isna(df.loc[3, "qualifier_codes"])
    assert pd.isna(df.loc[4, "detection_limit"])
    assert df.loc[5, "method_code"] == "042"


def test_invalid_repairs_discarded():
    """Verifies repairs proposed for fields that ultimately fail validation are discarded."""
    file_path = TEST_DIR / "error_field_constraints.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    results = process_file(str(file_path))

    errors = results["errors"]
    repairs = results["repairs"]

    assert sum(len(v) for v in errors.values()) > 0
    assert not any(
        k[0] == "parameter_value" and "Rounded decimal" in k[1] for k in repairs
    )


def test_exclude_uncurated_validity_logic():
    """Verifies uncurated rows are skipped when exclude_uncurated_validity is True."""
    file_path = TEST_DIR / "error_uncurated_dirty_rows.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    results = process_file(str(file_path), exclude_uncurated_validity=True)

    assert len(results["errors"]) == 0
    assert results["excluded_count"] == 4
    assert "4 rows skipped" in results["exclusion_note"]


def test_max_errors_early_abort(tmp_path):
    """Verifies validation aborts early if max_errors threshold is reached."""
    base_path = TEST_DIR / "valid_example.csv"
    assert base_path.exists(), f"Test file missing: {base_path}"

    df = pd.read_csv(base_path, dtype=str, keep_default_na=False).head(10)
    df["sample_datetime"] = df["sample_datetime"].str.replace(
        r"[+-]\d{2}:\d{2}$", "", regex=True
    )
    bad_csv = tmp_path / "ten_errors.csv"
    df.to_csv(bad_csv, index=False)

    results = process_file(str(bad_csv), max_errors=5)

    errors = results["errors"]
    assert any(k[0] == "Validation Aborted" for k in errors)


def test_multi_chunk_row_numbering(tmp_path):
    """Verifies chunk splitting and 1-based CSV row numbers across chunk boundaries."""
    file_path = TEST_DIR / "valid_example.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    chunks = list(iter_dataframe_rows(str(file_path), chunksize=15))
    assert len(chunks) == 7
    assert all(len(c) == 15 for c in chunks[:-1])
    assert len(chunks[-1]) == 10

    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    df.loc[16, "instrument_classification"] = "99"
    chunk_err_csv = tmp_path / "chunk_boundary_error.csv"
    df.to_csv(chunk_err_csv, index=False)

    orig_iter = iter_dataframe_rows
    with patch(
        "validate_aqdx.iter_dataframe_rows",
        side_effect=lambda inp, chunksize=15: orig_iter(inp, chunksize=15),
    ):
        results = process_file(str(chunk_err_csv))

    err_tracker = next(
        v for k, v in results["errors"].items() if k[0] == "instrument_classification"
    )
    assert err_tracker.first_row == 18


@pytest.mark.parametrize(
    "filename", ["valid_example.csv", "valid_example.csv.gz", "valid_example.parquet"]
)
def test_supported_formats_clean_pass(filename):
    results = process_file(TEST_DIR / filename)
    assert not results["errors"]
    assert not results["warnings"]
    assert not results["repairs"]
    assert not results["missing_headers"]
    assert results["total_rows"] == 100


def test_warnings_and_geo_rules():
    warn_geo = process_file(TEST_DIR / "warn_geo_us_bounds.csv")
    assert not warn_geo["errors"]
    assert (
        "lat lon swap",
        "Coordinates are outside expected US bounds. POSSIBLE SWAP DETECTED.",
    ) in warn_geo["warnings"]
    assert ("out of bounds", "Coordinates are outside expected US bounds.") in warn_geo[
        "warnings"
    ]

    warn_missing = process_file(TEST_DIR / "warn_missing_values_unjustified.csv")
    assert not warn_missing["errors"]
    assert sum(len(v) for v in warn_missing["warnings"].values()) == 2

    err_geo = process_file(TEST_DIR / "error_missing_geo_without_ig.csv")
    assert len(err_geo["errors"]) == 1


def test_missing_headers_and_schema_padding(tmp_path):
    df = pd.read_csv(
        TEST_DIR / "valid_example.csv", dtype=str, keep_default_na=False
    ).head(5)
    df = df.drop(columns=["elevation", "qualifier_codes"])
    bad_schema = tmp_path / "missing_cols.csv"
    df.to_csv(bad_schema, index=False)

    results = process_file(bad_schema)
    assert set(results["missing_headers"]) == {"elevation", "qualifier_codes"}
    assert {"elevation", "qualifier_codes"}.issubset(
        results["repaired_dataframe"].columns
    )


def test_xlsx_and_native_null_parquet(tmp_path):
    base_df = pd.read_csv(
        TEST_DIR / "valid_example.csv", dtype=str, keep_default_na=False
    ).head(10)

    # 1. Excel (.xlsx) support
    xlsx_path = tmp_path / "sample.xlsx"
    base_df.to_excel(xlsx_path, index=False)
    res_xlsx = process_file(xlsx_path)
    assert not res_xlsx["errors"]
    assert res_xlsx["total_rows"] == 10

    # 2. Parquet with native None/NaN values in optional columns
    pq_df = base_df.copy()
    pq_df.loc[0, ["elevation", "qualifier_codes"]] = [None, float("nan")]
    pq_path = tmp_path / "native_nulls.parquet"
    pq_df.to_parquet(pq_path, index=False)
    res_pq = process_file(pq_path)
    assert not res_pq["errors"]
    assert pd.isna(res_pq["repaired_dataframe"].loc[0, "elevation"])
    assert pd.isna(res_pq["repaired_dataframe"].loc[0, "qualifier_codes"])


def test_datetime_repairs_and_rounding_boundaries(tmp_path):
    df = pd.read_csv(
        TEST_DIR / "valid_example.csv", dtype=str, keep_default_na=False
    ).head(5)
    df.loc[0, "sample_datetime"] = "2024-06-05 08:40:30-06:00"  # space -> T
    df.loc[1, "sample_datetime"] = "2024-06-05T08:40:33Z"  # Z -> +00:00
    df.loc[2, "sample_datetime"] = "2024-06-05T08:40:34.123-06:00"  # 3ms valid
    df.loc[3, "parameter_value"] = "12.12345678"  # rounds to 12.12346
    df.loc[4, "parameter_value"] = "9999999.999999"  # rounds to 8 int digits -> fails

    path = tmp_path / "repairs_and_overflow.csv"
    df.to_csv(path, index=False)
    results = process_file(path)

    assert len(results["errors"]) == 1
    assert ("parameter_value", "Rounded decimal to max scale (5)") in results["repairs"]
    assert (
        results["repairs"][
            ("parameter_value", "Rounded decimal to max scale (5)")
        ].count
        == 1
    )
    assert results["repaired_dataframe"].loc[3, "parameter_value"] == "12.12346"
