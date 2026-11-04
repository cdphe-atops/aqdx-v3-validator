# AQDx v3 Data Validator

This application is publicly hosted on Streamlit, accessible at this link:
**[https://aqdx-v3-validator.streamlit.app/](https://aqdx-v3-validator.streamlit.app/)**

This repository contains a Python-based validation engine designed to verify that air quality datasets adhere to the **[AQDx v3.0 Standard Format](https://cdphe-atops.github.io/aqdx-documentation)**. It provides government agencies, community groups, and other data stewards with a tool to enforce schema integrity, coordinate logic, and data compliance before data ingestion.

## Overview

The engine uses **Pydantic** to define and enforce validation rules. This approach provides a portable, machine-readable schema definition that can be reused in other Python-based data pipelines or run as a standalone CLI/Web application.

The tool identifies structural errors, provides logical warnings, and can automatically generate a "repaired" version of data for common formatting inconsistencies.

### Core Validation Logic

- **Schema Enforcement:** Verifies all required fields (e.g., `parameter_code`, `unit_code`, `validity_code`) against defined lengths and regex patterns.
- **Datetime Standardization:** Strictly enforces ISO 8601 formats including mandatory timezone offsets.
- **Geospatial Checks:** Detects "Null Island" coordinates (0, 0) and provides warnings for values outside expected U.S. bounding boxes (potential Lat/Lon swaps).
- **Quality Logic:** Ensures cross-field consistency, such as requiring specific `validity_codes` when `parameter_values` are null.
- **Auto-Repair Engine:** Standardizes common formatting artifacts including:
  - Stripping float artifacts from integer codes (e.g., `45202.0` -> `45202`).
  - Applying zero-padding to codes (e.g., `unit_code` `7` -> `007`).
  - Standardizing "smart quotes" and whitespace.
  - Rounding decimals to the maximum allowed scale for the AQDx schema.

---

## 📦 Usage: Integrating as a Library

Because this repository is configured as a standard Python package, you can consume its validation engine directly in other applications without copying the source code.

Currently, this package is distributed via GitHub tags rather than PyPI.

### Installing with pip

```bash
pip install git+https://github.com/cdphe-atops/aqdx-v3-validator.git@v1.0.1
```

_(Note: requres git binary installed on your system.)_

### Installing with Pixi

If you are using Pixi, add the package directly to your `pixi.toml` under PyPI dependencies, pointing to a specific release tag:

```toml
[pypi-dependencies]
aqdx-v3-validator = { git = "https://github.com/cdphe-atops/aqdx-v3-validator.git", tag = "v1.0.1" }
```

_(Note: Always lock to a specific `tag` to ensure upstream changes to `main` do not break your pipeline unexpectedly.)_

### Python API Example

```python
from validate_aqdx import process_file

# Run the validation engine on a local file or dataframe
results = process_file("path/to/data.csv")

if results["errors"]:
    print(f"Found {sum(len(v) for v in results['errors'].values())} errors!")
else:
    # Extract the fully validated and auto-repaired Pandas DataFrame
    clean_df = results["repaired_dataframe"]
    print("Ready for ingestion!")
```

---

## 🖥️ Usage: Command Line Interface (CLI)

You can run the core validation engine directly from your terminal. The CLI provides a detailed tabular breakdown of errors, warnings, and available repairs.

### Setup

Clone the repository and install it locally in editable mode:

```bash
git clone https://github.com/cdphe-atops/aqdx-v3-validator.git
cd aqdx-v3-validator
pip install -e .
```

### Running the CLI

Installing the package makes the `validate-aqdx` command globally available in your environment:

```bash
validate-aqdx path/to/your/data.csv
```

**Interactive Auto-Repair:**
If the engine detects fixable formatting issues, it will pause at the end of the validation report and ask:
`Press 'R' to accept proposed repairs and save as data_repair.csv, or press Enter to exit...`
Hitting `R` will instantly write the cleaned data to a new file in the same directory.

---

## 💻 Usage: Web Interface (Streamlit)

The easiest way for end-users to validate files is using the included Streamlit frontend. It provides a drag-and-drop interface, renders beautiful summary tables, and handles temporary file cleanup automatically.

To launch the web app locally:

```bash
# Ensure dependencies are installed
pip install -r requirements.txt

# Run the app
streamlit run streamlit_app.py
```

- The app will automatically open in your default browser.
- If auto-repairs are available, a **Download Repaired CSV** button will appear in the UI.

---

## 🛠️ Local Development

To contribute to the validator rules or engine:

1. Clone the repository and set up a virtual environment.
2. Install the package with development dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

3. Run the test suite:

   ```bash
   pytest
   ```
