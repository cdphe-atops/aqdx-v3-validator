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
    ("error_null_island.csv", 1, 0, 0),
]


@pytest.mark.parametrize(
    "filename, expected_errors, expected_warnings, expected_repairs", EXPECTED_RESULTS
)
def test_csv_validation(filename, expected_errors, expected_warnings, expected_repairs):
    file_path = TEST_DIR / filename

    # Ensure the test file actually exists before we try to process it
    assert file_path.exists(), f"Test file missing: {file_path}"

    # Run the core validation engine
    results = process_file(str(file_path))

    # Calculate totals from the returned dictionaries
    total_errors = sum(len(v) for v in results["errors"].values())
    total_warnings = sum(len(v) for v in results["warnings"].values())
    total_repairs = sum(len(v) for v in results["repairs"].values())

    assert total_errors == expected_errors, (
        f"Expected {expected_errors} errors, got {total_errors}"
    )
    assert total_warnings == expected_warnings, (
        f"Expected {expected_warnings} warnings, got {total_warnings}"
    )
    assert total_repairs == expected_repairs, (
        f"Expected {expected_repairs} repairs, got {total_repairs}"
    )

    # Clean up the hidden temp repair files so they don't clutter the directory
    temp_path = results.get("repaired_file_path")
    if temp_path and os.path.exists(temp_path):
        os.remove(temp_path)
