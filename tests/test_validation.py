import glob
import os
from pathlib import Path

import pytest

from src.validate_aqdx import process_file

TEST_DIR = Path(__file__).parent.parent / "test_files"

# Format: (filename, expected_errors, expected_warnings, expected_repairs)
EXPECTED_RESULTS = [
    ("valid_example.csv", 0, 0, 0),
    ("valid_column_swap.csv", 0, 0, 0),
    ("warn_latlon_swap.csv", 0, 100, 0),
    ("error_precision.csv", 0, 0, 100),
    ("error_leading_zeros.csv", 0, 0, 55),
    ("error_null_island.csv", 1, 0, 0),
]


@pytest.mark.parametrize(
    "filename, expected_errors, expected_warnings, expected_repairs", EXPECTED_RESULTS
)
def test_csv_row_validation(
    filename, expected_errors, expected_warnings, expected_repairs
):
    """Tests that files with valid headers are processed correctly at the row level."""
    file_path = TEST_DIR / filename

    assert file_path.exists(), f"Test file missing: {file_path}"

    # Run core engine
    results = process_file(str(file_path))

    # Aggregate results from dictionaries
    total_errors = sum(len(v) for v in results["errors"].values())
    total_warnings = sum(len(v) for v in results["warnings"].values())
    total_repairs = sum(len(v) for v in results["repairs"].values())

    # Assertions
    assert total_errors == expected_errors, f"{filename}: Error mismatch"
    assert total_warnings == expected_warnings, f"{filename}: Warning mismatch"
    assert total_repairs == expected_repairs, f"{filename}: Repair mismatch"

    # Cleanup temp repair files
    temp_path = results.get("repaired_file_path")
    if temp_path and os.path.exists(temp_path):
        os.remove(temp_path)


# --- 2. Schema-Level Failure Tests ---
# Format: (filename, expected_missing_substring)
SCHEMA_FAILURE_CASES = [
    ("error_missing_column.csv", "parameter_code"),
]


@pytest.mark.parametrize("filename, expected_missing_substring", SCHEMA_FAILURE_CASES)
def test_schema_critical_failures(filename, expected_missing_substring):
    """Tests that missing columns trigger a ValueError before row processing begins."""
    file_path = TEST_DIR / filename

    assert file_path.exists(), f"Test file missing: {file_path}"

    with pytest.raises(ValueError) as excinfo:
        process_file(str(file_path))

    error_msg = str(excinfo.value)
    assert "CRITICAL SCHEMA ERROR" in error_msg
    assert expected_missing_substring in error_msg

    base_name = file_path.stem
    repair_file_pattern = str(TEST_DIR / f".{base_name}*_repair.csv")

    for repair_file in glob.glob(repair_file_pattern):
        try:
            os.remove(repair_file)
        except OSError:
            pass  # File might already be closed/deleted by the OS
