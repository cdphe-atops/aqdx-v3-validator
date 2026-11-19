from pathlib import Path

import polars as pl
import pytest

from validate_aqdx import process_file
from validate_aqdx_performance import main, scan_validated_file, validate_file

TEST_DIR = Path(__file__).parent.parent / "test_files"


def test_allow_null_dataset_id_flag(capsys):
    """Verifies --allow-null-dataset-id behavior in API and CLI."""
    file_path = TEST_DIR / "warn_missing_dataset_id.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    # Fails by default
    res_default = validate_file(file_path)
    assert not res_default
    assert res_default.failed_rule == "dataset_id"

    # Passes with flag in API
    res_flag = validate_file(file_path, allow_null_dataset_id=True)
    assert res_flag.passed is True

    # Passes with flag in CLI
    exit_code = main([str(file_path), "--allow-null-dataset-id"])
    assert exit_code == 0
    assert "PASS:" in capsys.readouterr().out


def test_exclude_uncurated_cli_flag(capsys):
    """Verifies --exclude-uncurated CLI flag handles dirty uncurated rows."""
    file_path = TEST_DIR / "error_uncurated_dirty_rows.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    # Fails without flag
    exit_code_fail = main([str(file_path)])
    assert exit_code_fail == 1
    assert "FAIL" in capsys.readouterr().err

    # Passes with flag
    exit_code_pass = main([str(file_path), "--exclude-uncurated"])
    assert exit_code_pass == 0
    assert "PASS:" in capsys.readouterr().out


def test_native_parquet_dtypes(tmp_path):
    """Verifies parquet native dtypes are handled properly in normalization."""
    csv_path = TEST_DIR / "valid_example.csv"
    parquet_path = tmp_path / "native.parquet"

    # Keep zero-padded string codes intact while casting datetime/numeric columns
    df = pl.read_csv(csv_path, infer_schema=False)

    # Valid: Datetime with timezone
    df_valid = df.with_columns(
        [
            pl.col("sample_datetime")
            .str.to_datetime("%Y-%m-%dT%H:%M:%S%:z")
            .dt.cast_time_unit("us"),
            pl.col("aggregation_code").cast(pl.Int64),
            pl.col("latitude").cast(pl.Float64),
        ]
    )
    df_valid.write_parquet(parquet_path)

    res, _ = scan_validated_file(parquet_path)
    assert res.passed is True

    # Invalid: Naive Datetime (lacks required timezone offset)
    parquet_naive_path = tmp_path / "naive.parquet"
    df_naive = df.with_columns(
        [
            pl.col("sample_datetime")
            .str.replace(r"[+-]\d{2}:\d{2}$", "")
            .str.to_datetime("%Y-%m-%dT%H:%M:%S")
            .dt.cast_time_unit("us")
        ]
    )
    df_naive.write_parquet(parquet_naive_path)

    res_naive, _ = scan_validated_file(parquet_naive_path)
    assert not res_naive
    assert res_naive.failed_rule == "sample_datetime"


@pytest.mark.parametrize("file_path", TEST_DIR.glob("*.csv"))
def test_cross_engine_parity(file_path):
    """
    Parity Guarantee: Any file that strictly passes the high-performance Polars engine
    MUST yield 0 hard errors and 0 missing headers in the Pydantic engine.
    """
    polars_result = validate_file(
        file_path,
        exclude_uncurated_validity=True,
        allow_null_dataset_id=True,
    )

    if polars_result.passed:
        pydantic_results = process_file(
            file_path,
            exclude_uncurated_validity=True,
        )

        total_errors = sum(len(v) for v in pydantic_results["errors"].values())
        missing_headers = pydantic_results.get("missing_headers", [])

        assert total_errors == 0, (
            f"Engine mismatch on {file_path.name}: Polars passed, "
            f"but Pydantic found {total_errors} errors."
        )
        assert len(missing_headers) == 0, (
            f"Engine mismatch on {file_path.name}: Polars passed, "
            f"but Pydantic found missing headers: {missing_headers}"
        )
