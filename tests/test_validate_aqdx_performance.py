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


@pytest.mark.parametrize(
    "filename",
    [
        "valid_example.csv",
        "valid_example.csv.gz",
        "valid_example.parquet",
        "valid_missing_geo_with_ig.csv",
        "valid_missing_values_qualified.csv",
    ],
)
def test_valid_fixtures_pass_strictly(filename):
    res, lf = scan_validated_file(TEST_DIR / filename)
    assert res.passed is True
    assert lf is not None
    assert lf.select(pl.len()).collect().item() == 100


def test_null_island_and_equator_meridian(tmp_path):
    df = pl.read_csv(TEST_DIR / "valid_example.csv", infer_schema=False).head(3)
    # Row 0: Equator (valid), Row 1: Prime Meridian (valid)
    df_ok = df.with_columns(
        pl.Series("latitude", ["0.0", "39.784", "39.784"]),
        pl.Series("longitude", ["-105.11", "0.0", "-105.11"]),
    )
    ok_path = tmp_path / "equator_meridian.csv"
    df_ok.write_csv(ok_path)
    assert validate_file(ok_path).passed is True

    # Row 0: Null Island (0.0, 0.0) -> must fail
    df_ni = df.with_columns(
        pl.Series("latitude", ["0.0", "39.784", "39.784"]),
        pl.Series("longitude", ["0.0", "-105.11", "-105.11"]),
    )
    ni_path = tmp_path / "null_island.csv"
    df_ni.write_csv(ni_path)
    res_ni = validate_file(ni_path)
    assert not res_ni
    assert res_ni.failed_rule == "geo_null_island"


def test_scan_lazyframe_filtering_and_cli_errors(tmp_path, capsys):
    # Verify scan_validated_file actually filters uncurated rows out of the returned LazyFrame
    res, lf = scan_validated_file(
        TEST_DIR / "error_uncurated_dirty_rows.csv",
        exclude_uncurated_validity=True,
    )
    assert res.passed is True
    assert lf is not None
    assert lf.select(pl.len()).collect().item() == 96

    # Missing headers in Polars
    df_missing = pl.read_csv(TEST_DIR / "valid_example.csv", infer_schema=False).drop(
        "dataset_id"
    )
    missing_path = tmp_path / "missing_col.csv"
    df_missing.write_csv(missing_path)
    res_missing = validate_file(missing_path)
    assert not res_missing
    assert res_missing.failed_rule == "missing_headers"
    assert res_missing.missing_headers == ("dataset_id",)

    # CLI exit code 2 for unsupported format and missing file
    assert main([str(tmp_path / "does_not_exist.csv")]) == 2
    bad_ext = tmp_path / "bad.txt"
    bad_ext.write_text("hello")
    assert main([str(bad_ext)]) == 2
    capsys.readouterr()
