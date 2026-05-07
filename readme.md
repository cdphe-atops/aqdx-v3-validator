# AQDx v3 Data Validator

This application is publically hosted on streamlit accessible at this link:
**[https://aqdx-v3-validator.streamlit.app/](https://aqdx-v3-validator.streamlit.app/)**

This repository contains a Python-based validation engine designed to verify that air quality datasets adhere to the **[AQDx v3.0 Standard Format](https://cdphe-atops.github.io/aqdx-documentation)**. It provides government agencies, community groups, and other data stewards with a tool to enforce schema integrity, coordinate logic, and data compliance before data ingestion.

## Overview

The engine uses **Pydantic** to define and enforce validation rules. This approach provides a portable, machine-readable schema definition that can be reused in other Python-based data pipelines or applications requiring AQDx v3.0 compliance.

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

## Installation

### 1. Clone the Repository

```bash
git clone [https://github.com/cdphe-atops/aqdx-v3-validator.git](https://github.com/cdphe-atops/aqdx-v3-validator.git)
cd aqdx-v3-validator
```

### 2. Set Up Your Environment

You can install the dependencies in "editable" mode for local development (which reads `pyproject.toml`), or use the Streamlit-specific requirements file.

**For Local/CLI Development:**

```bash
# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install the package and dependencies
pip install -e .
```

**For Streamlit Web UI:**

```bash
# Streamlit deployments rely on requirements.txt to bypass Poetry assumptions
pip install -r requirements.txt
```

---

## 💻 Usage: Web Interface (Streamlit)

The easiest way to validate files is using the included Streamlit frontend. It provides a drag-and-drop interface, renders beautiful summary tables, and handles temporary file cleanup automatically.

To launch the web app locally:

```bash
streamlit run streamlit_app.py
```

- The app will automatically open in your default browser.

- If auto-repairs are available, a **Download Repaired CSV** button will appear in the UI.

---

## 🖥️ Usage: Command Line Interface (CLI)

You can run the core validation engine directly from your terminal. The CLI provides a detailed tabular breakdown of errors, warnings, and available repairs.

```bash
python src/validate_aqdx.py path/to/your/data.csv
```

**Interactive Auto-Repair:**
If the engine detects fixable formatting issues, it will pause at the end of the validation report and ask:
`Press 'R' to accept proposed repairs and save as data_repair.csv, or press Enter to exit...`
Hitting `R` will instantly write the cleaned data to a new file in the same directory.

---
