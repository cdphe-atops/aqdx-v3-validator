import gzip
from pathlib import Path

import pytest

# Skip this test module cleanly if the optional [performance] extra is not installed
pl = pytest.importorskip("polars")

from validate_aqdx_performance import (  # noqa: E402
    REQUIRED_HEADERS,
    VALIDATION_RULES,
    ValidationResult,
    ValidationRule,
    main,
    validate_file,
)

TEST_DIR = Path(__file__).parent.parent / "test_files"

# Format: (filename, expected_passed, expected_failed_rule)
EXPECTED_STRICT_RESULTS = [
    ("valid_example.csv", True, None),
    ("valid_column_swap.csv", True, None),
    # Soft warnings are omitted in strict pass/fail mode
    ("warn_latlon_swap.csv", False, "latitude_range"),  # fails range checks
    ("error_missing_parameter_value_without_validity_code.csv", True, None),
    # Previously auto-repaired formatting issues now fail fast
    ("error_precision.csv", False, "parameter_value"),
    ("error_leading_zeros.csv", False, "unit_code"),
    ("error_datetime_repairable.csv", False, "sample_datetime"),
    ("error_datetime_z.csv", False, "sample_datetime"),
    # Hard errors
    ("error_datetime_offset.csv", False, "sample_datetime"),
    ("error_null_island.csv", False, "geo_null_island"),
]


@pytest.mark.parametrize(
    "filename, expected_passed, expected_failed_rule", EXPECTED_STRICT_RESULTS
)
def test_csv_strict_validation(filename, expected_passed, expected_failed_rule):
    """Verifies strict pass/fail outcomes and short-circuit rule names on test_files/."""
    file_path = TEST_DIR / filename
    assert file_path.exists(), f"Test file missing: {file_path}"

    result = validate_file(file_path)

    assert isinstance(result, ValidationResult)
    assert bool(result) is expected_passed
    assert result.passed is expected_passed
    assert result.failed_rule == expected_failed_rule


def test_missing_headers():
    """Verifies missing required columns fail immediately with missing_headers."""
    file_path = TEST_DIR / "error_missing_column.csv"
    assert file_path.exists(), f"Test file missing: {file_path}"

    result = validate_file(file_path)

    assert not result
    assert result.failed_rule == "missing_headers"
    assert "parameter_code" in result.missing_headers


def test_unsupported_extension_raises():
    """Verifies unsupported inputs (such as .xlsx) raise ValueError."""
    xlsx_path = TEST_DIR / "valid_opened_in_excel.xlsx"
    assert xlsx_path.exists(), f"Test file missing: {xlsx_path}"

    with pytest.raises(
        ValueError, match="Only .csv, .csv.gz, and .parquet files are supported"
    ):
        validate_file(xlsx_path)


def test_parquet_support(tmp_path):
    """Verifies .parquet files pass validation and scan_validated_file returns a LazyFrame."""
    import polars as pl

    from validate_aqdx_performance import scan_validated_file

    csv_path = TEST_DIR / "valid_example.csv"
    parquet_path = tmp_path / "valid_example.parquet"
    pl.read_csv(csv_path, infer_schema=False).write_parquet(parquet_path)

    result, lf = scan_validated_file(parquet_path)
    assert bool(result) is True
    assert lf is not None
    assert lf.collect().height > 0


def test_gzip_csv_support(tmp_path):
    """Verifies .csv.gz files are scanned and validated identically to plain .csv."""
    valid_csv = TEST_DIR / "valid_example.csv"
    valid_gz = tmp_path / "valid_example.csv.gz"
    with valid_csv.open("rb") as src, gzip.open(valid_gz, "wb") as dst:
        dst.write(src.read())

    assert validate_file(valid_gz).passed is True

    invalid_csv = TEST_DIR / "error_null_island.csv"
    invalid_gz = tmp_path / "error_null_island.csv.gz"
    with invalid_csv.open("rb") as src, gzip.open(invalid_gz, "wb") as dst:
        dst.write(src.read())

    res = validate_file(invalid_gz)
    assert not res
    assert res.failed_rule == "geo_null_island"


def test_exclude_uncurated_validity(tmp_path):
    """Verifies rows with validity_code in [0, 8, 9] or missing are skipped when enabled."""
    valid_csv = TEST_DIR / "valid_example.csv"
    lines = valid_csv.read_text(encoding="utf-8").splitlines()
    headers = lines[0].split(",")
    sample_row = lines[1].split(",")

    val_idx = headers.index("parameter_value")
    vc_idx = headers.index("validity_code")

    # Introduce an invalid parameter_value (too many decimals) on uncurated rows
    uncurated_rows = []
    for code in ("0", "8", "9", ""):
        row = sample_row.copy()
        row[val_idx] = "1.12345678"
        row[vc_idx] = code
        uncurated_rows.append(",".join(row))

    test_csv = tmp_path / "uncurated_mix.csv"
    test_csv.write_text(
        "\n".join([lines[0], lines[1], *uncurated_rows]) + "\n", encoding="utf-8"
    )

    # Fails when uncurated rows are included
    strict_res = validate_file(test_csv, exclude_uncurated_validity=False)
    assert not strict_res
    assert strict_res.failed_rule == "parameter_value"

    # Passes when uncurated rows are excluded
    filtered_res = validate_file(test_csv, exclude_uncurated_validity=True)
    assert filtered_res.passed is True


def test_cli_exit_codes(capsys):
    """Verifies CLI exit codes: 0 on pass, 1 on validation fail, 2 on bad input."""
    assert main([str(TEST_DIR / "valid_example.csv")]) == 0
    assert "PASS:" in capsys.readouterr().out

    assert main([str(TEST_DIR / "error_null_island.csv")]) == 1
    assert "FAIL [geo_null_island]:" in capsys.readouterr().err

    assert main([str(TEST_DIR / "valid_opened_in_excel.xlsx")]) == 2
    assert "ERROR:" in capsys.readouterr().err


def test_validation_rules_inspection():
    """Ensures all rules are valid ValidationRule dataclasses referencing known columns."""
    known_cols = set(REQUIRED_HEADERS)
    assert len(VALIDATION_RULES) > 0
    for rule in VALIDATION_RULES:
        assert isinstance(rule, ValidationRule)
        assert isinstance(rule.expr, pl.Expr)
        assert set(rule.columns).issubset(known_cols)
