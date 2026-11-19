from pathlib import Path
from unittest.mock import patch

import pandas as pd

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
