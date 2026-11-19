# AQDx v3 Data Validator

This application is publicly hosted on Streamlit, accessible at this link:
**[https://aqdx-v3-validator.streamlit.app/](https://aqdx-v3-validator.streamlit.app/)**

This repository contains a Python-based validation engine designed to verify that air quality datasets adhere to the **[AQDx v3.0 Standard Format](https://cdphe-atops.github.io/aqdx-documentation)**. It provides government agencies, community groups, and other data stewards with tools to enforce schema integrity, coordinate logic, and data compliance before data ingestion.

## Overview

The package provides two complementary validation engines:

1. **Standard Pydantic Engine (`validate_aqdx`):** Performs row-level validation with detailed error/warning reporting and automatic formatting repairs. Supports `.csv`, `.csv.gz`, `.xlsx`, `.xls`, `.ods`, and `.parquet` files.
2. **High-Performance Polars Engine (`validate_aqdx_performance`):** Performs vectorized, lazy pass/fail validation designed for large datasets (up to 10M+ rows) in `.csv`, `.csv.gz`, and `.parquet` formats. Evaluates rules lazily, short-circuits on the first failing rule, and requires the optional `performance` dependency (`polars`).

### Core Validation Logic

- **Schema & Type Enforcement:** Verifies all required headers and fields (e.g., `parameter_code`, `unit_code`, `validity_code`) against defined lengths, integer bounds, `DECIMAL(P, S)` precision/scale limits, and regex patterns.
- **Datetime Standardization:** Strictly enforces ISO 8601 timestamps with mandatory numeric timezone offsets (`[+-]HH:MM`).
- **Geospatial Checks:** Enforces global coordinate bounds (`-90` to `90` latitude, `-180` to `180` longitude), rejects "Null Island" `(0.0, 0.0)`, requires an `IG` qualifier when coordinates are missing, and emits warnings in the Pydantic engine for coordinates outside expected U.S. bounds (potential Lat/Lon swaps).
- **Uncurated Row Filtering:** Supports `exclude_uncurated_validity=True` (`--exclude-uncurated` in the performance CLI) to ignore uncurated rows where `validity_code` is missing or in `[0, 8, 9]`.
- **Auto-Repair Engine (`validate_aqdx`):** Standardizes common formatting artifacts including:
  - Stripping `.0` float artifacts from integer codes (e.g., `45202.0` -> `45202`).
  - Applying zero-padding to codes (e.g., `unit_code` `7` -> `007`).
  - Standardizing smart/single quotes and whitespace.
  - Replacing forbidden null placeholders (`-999`, `N/A`, `null`, etc.) with empty values.
  - Converting space-separated or `Z`-suffixed ISO timestamps to `+00:00` offset format.
  - Rounding decimals to the maximum allowed scale for the AQDx schema.

---

## 📦 Usage: Integrating as a Library

This package is distributed via GitHub release tags rather than PyPI.

### Installing with pip

**Standard installation (Pydantic validator & auto-repair engine only):**

```bash
pip install git+https://github.com/cdphe-atops/aqdx-v3-validator.git@v1.1.1
```

**With the optional `performance` extra (includes `polars` for `validate_aqdx_performance`):**

```bash
pip install "aqdx-v3-validator[performance] @ git+https://github.com/cdphe-atops/aqdx-v3-validator.git@v1.1.1"

```

_(Note: Requires the `git` binary installed on your system.)_

### Installing with Pixi

Add the package to your `pixi.toml` under `[pypi-dependencies]`, locked to a specific release tag:

```toml
# Standard installation
[pypi-dependencies]
aqdx-v3-validator = { git = "https://github.com/cdphe-atops/aqdx-v3-validator.git", tag = "v1.1.1" }

# Or with the optional performance extra (includes polars)
[pypi-dependencies]
aqdx-v3-validator = { git = "https://github.com/cdphe-atops/aqdx-v3-validator.git", tag = "v1.1.1", extras = ["performance"] }

```

_(Note: Always lock to a specific `tag` to ensure upstream changes to `main` do not break your pipeline unexpectedly.)_

### Python API: Standard Validator & Auto-Repair (`validate_aqdx`)

```python
from validate_aqdx import process_file

# Run the Pydantic validation and auto-repair engine
results = process_file("path/to/data.csv", exclude_uncurated_validity=True)

if results["errors"] or results["missing_headers"]:
    print(f"Found {sum(len(v) for v in results['errors'].values())} errors!")
else:
    # Extract the fully validated and auto-repaired Pandas DataFrame
    clean_df = results["repaired_dataframe"]
    print("Ready for ingestion!")

```

### Python API: High-Performance Validator (`validate_aqdx_performance`)

Requires the `performance` optional dependency (`polars`).

```python
from validate_aqdx_performance import scan_validated_file, validate_file

# 1. Fast pass/fail check (short-circuits on first rule violation)
result = validate_file("path/to/large_dataset.csv.gz", exclude_uncurated_validity=True)

if not result:
    print(f"Validation failed [{result.failed_rule}]: {result.reason}")
else:
    print("Validation passed!")

# 2. Validate and obtain the filtered Polars LazyFrame for single-pass streaming
result, lf = scan_validated_file("path/to/large_dataset.parquet", exclude_uncurated_validity=True)

if result and lf is not None:
    # Continue building lazy transformations or sink directly to Parquet
    print(f"Validated rows: {lf.select(pl.len()).collect().item()}")

```

---

## 🖥️ Usage: Command Line Interfaces (CLI)

Installing the package exposes two CLI entrypoints: `validate-aqdx` and `validate-aqdx-performance`.

### Local Setup

```bash
git clone https://github.com/cdphe-atops/aqdx-v3-validator.git
cd aqdx-v3-validator

# Standard install
pip install -e .

# Or include the optional Polars performance validator
pip install -e ".[performance]"

```

### 1. Standard CLI (`validate-aqdx`)

Provides a tabular breakdown of errors, warnings, and available repairs:

```bash
validate-aqdx path/to/your/data.csv

```

**Interactive Auto-Repair:**
If fixable formatting issues or missing headers are detected, the CLI pauses at the end of the report and prompts:
`Press 'R' to accept proposed repairs/schema-padding and save as data_repair.csv, or press Enter to exit...`
Pressing `R` writes the cleaned dataset to a new `*_repair.csv` file in the same directory.

### 2. High-Performance CLI (`validate-aqdx-performance`)

Requires the `performance` extra (`polars`). Built for automated scripts and large `.csv`, `.csv.gz`, or `.parquet` files:

```bash
validate-aqdx-performance path/to/large_dataset.csv --exclude-uncurated

```

**Exit Codes:**

- `0`: Validation passed (`PASS: <file_path>` printed to stdout).
- `1`: Schema or rule validation failed (`FAIL [<failed_rule>]: <reason>` printed to stderr).
- `2`: File not found or unsupported file extension (`ERROR: <message>` printed to stderr).

---

## 💻 Usage: Web Interface (Streamlit)

End-users can also validate smaller files using the included Streamlit frontend, which provides a drag-and-drop interface, summary tables, and one-click CSV repair downloads:

```bash
pip install streamlit
streamlit run streamlit_app.py

```

---

## 🛠️ Local Development

1. Clone the repository and activate a Python `>=3.11` virtual environment.
2. Install the package with development dependencies (includes `pytest`, `ipykernel`, and `polars`):

```bash
pip install -e ".[dev]"

```

1. Run the test suite:

```bash
pytest -v

```
