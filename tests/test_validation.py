import os
from pathlib import Path

import pytest

from src.validate_aqdx import process_file

TEST_DIR = Path(__file__).parent.parent / "test_files"

# Format: (filename, expected_errors, expected_warnings, expected_repairs)
EXPECTED_RESULTS = [
    ("valid_example.csv", 0, 0, 0),
    ("valid_opened_in_excel.xlsx", 0, 0, 55),
    ("valid_column_swap.csv", 0, 0, 0),
    ("warn_latlon_swap.csv", 0, 100, 0),
    ("error_precision.csv", 0, 0, 100),
    ("error_leading_zeros.csv", 0, 0, 55),
    ("error_null_island.csv", 1, 0, 0),
    ("error_missing_parameter_value_without_validity_code.csv", 1, 0, 0),
    ("error_datetime_repairable.csv", 0, 0, 100),
    ("error_datetime_z.csv", 0, 0, 100),
    ("error_datetime_offset.csv", 100, 0, 0),
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
    """Tests that missing columns are flagged in the missing_headers output."""
    file_path = TEST_DIR / filename

    assert file_path.exists(), f"Test file missing: {file_path}"

    # Engine no longer raises ValueError; it returns the missing columns
    results = process_file(str(file_path))

    # Check if the expected column is in the missing headers list
    assert expected_missing_substring in results.get("missing_headers", []), (
        f"Expected missing column '{expected_missing_substring}' not found in missing_headers."
    )

    # Cleanup temp repair file
    temp_path = results.get("repaired_file_path")
    if temp_path and os.path.exists(temp_path):
        os.remove(temp_path)
