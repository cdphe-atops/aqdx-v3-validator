# AQDx v3 Validator

A high-performance, Python-based validation engine designed to ensure tabular air quality data strictly adheres to the AQDx v3.0 schema.

This tool is built on **Pydantic** for rigorous type and pattern checking, and uses **Pandas** chunking to process massive datasets (`.csv`, `.xlsx`, `.parquet`) with a minimal memory footprint. It can be run as an interactive Command Line Interface (CLI) or as a user-friendly Streamlit web application.

This application is hosted on streamlit accessible at this link:
👉 **[https://aqdx-v3-validator.streamlit.app/](https://aqdx-v3-validator.streamlit.app/)**

## 🚀 Current Status & Features

The core validation engine is fully operational and includes the following features:

- **Memory-Efficient Processing:** Streams data in chunks, allowing it to validate datasets with hundreds of thousands of rows without crashing.
- **Strict Schema Enforcement:** Validates ISO 8601 datetimes, precision limits, forbidden null placeholders, and code patterns based on the AQDx v3.0 field dictionary.
- **Cross-Field Logic:** Distinguishes between hard Errors (e.g., Null Island coordinates) and Warnings (e.g., swapped Lat/Lon coordinates or missing qualification codes).
- **Auto-Repair Engine:** Safely pre-processes and mutates recoverable formatting issues (e.g., stripping commas, rounding precision floats, standardizing quotes, zero-padding codes) and offers a cleaned `_repair.csv` file for download.

---

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone [https://github.com/yourusername/aqdx-v3-validator.git](https://github.com/yourusername/aqdx-v3-validator.git)
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
streamlit run app.py
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
