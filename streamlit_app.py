import os
import tempfile

import pandas as pd
import streamlit as st

# Import your core validation engine!
from src.validate_aqdx import process_file

# --- Page Setup ---
st.set_page_config(page_title="AQDx Validator v3.0", page_icon="✅", layout="wide")

st.title("AQDx Standard Format Validator (v3.0)")
st.markdown(
    "Upload your tabular data file to validate it against the [AQDx v3](https://cdphe-atops.github.io/aqdx-documentation/standard-format/field-dictionary/) standard format."
)
st.markdown("---")

# --- File Uploader ---
uploaded_file = st.file_uploader(
    "Choose a file (CSV, XLSX, Parquet)", type=["csv", "gz", "xlsx", "parquet"]
)

if uploaded_file is not None:
    if uploaded_file.name.lower().endswith(
        ".gz"
    ) and not uploaded_file.name.lower().endswith(".csv.gz"):
        st.error(
            "✘ **Invalid File Type:** If uploading a compressed file, it must be a `.csv.gz`. Other gzip formats (like .tar.gz or .json.gz) are not supported."
        )
        st.stop()

    file_ext = os.path.splitext(uploaded_file.name)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    with st.spinner("Processing file... this may take a moment for large datasets."):
        try:
            # 1. Run the Core Validation Engine
            results = process_file(tmp_path)

            total_rows = results["total_rows"]
            grouped_errors = results["errors"]
            grouped_warnings = results["warnings"]
            grouped_repairs = results["repairs"]
            missing_headers = results.get("missing_headers", [])
            repaired_path = results["repaired_file_path"]

            st.markdown("---")

            # 2. Render Main Status & Errors
            if missing_headers:
                st.error(
                    "✘ **SCHEMA FAILURE:** The file is missing required columns. Auto-repairs have been applied to existing columns where possible, but the file remains formally incomplete."
                )
                st.error(
                    "**Missing Columns:**\n"
                    + "\n".join([f"- `{h}`" for h in missing_headers])
                )
            elif not grouped_errors and not grouped_repairs:
                st.success(
                    f"✔ **SUCCESS:** All {total_rows} rows match the AQDx v3 standard!"
                )
            elif not grouped_errors and grouped_repairs:
                st.warning(
                    "⚠️ **CONDITIONAL PASS:** The file contained formatting issues, but was fully auto-repaired."
                )
            else:
                total_errors = sum(len(rows) for rows in grouped_errors.values())
                st.error(
                    f"✘ **FAILURE:** Found {total_errors} hard error(s) across {total_rows} rows."
                )

            # Render Error Table
            if grouped_errors:
                error_data = [
                    {
                        "Error Name": k[0],
                        "Count": len(v),
                        "Message": k[1],
                        "First Affected Row": v[0],
                    }
                    for k, v in grouped_errors.items()
                ]
                st.dataframe(
                    pd.DataFrame(error_data).sort_values("Count", ascending=False),
                    width="stretch",
                    hide_index=True,
                )

            # 3. Render Warnings
            if grouped_warnings:
                total_warnings = sum(len(rows) for rows in grouped_warnings.values())
                st.subheader(f"⚠️ Warnings ({total_warnings})")
                st.info(
                    "These are potential logical issues that do not strictly invalidate the file."
                )

                warn_data = [
                    {
                        "Warning Name": k[0],
                        "Count": len(v),
                        "Message": k[1],
                        "First Affected Row": v[0],
                    }
                    for k, v in grouped_warnings.items()
                ]
                st.dataframe(
                    pd.DataFrame(warn_data).sort_values("Count", ascending=False),
                    width="stretch",
                    hide_index=True,
                )

            # 4. Render Repairs & Download Button
            if grouped_repairs:
                total_repairs = sum(len(rows) for rows in grouped_repairs.values())
                st.subheader(f"🛠️ Auto-Repairs Applied ({total_repairs})")

                repair_data = [
                    {
                        "Field": k[0],
                        "Count": len(v),
                        "Repair Action": k[1],
                        "First Affected Row": v[0],
                    }
                    for k, v in grouped_repairs.items()
                ]
                st.dataframe(
                    pd.DataFrame(repair_data).sort_values("Count", ascending=False),
                    width="stretch",
                    hide_index=True,
                )

            # Offer the repaired file as a download (always available if repair file generated)
            if os.path.exists(repaired_path):
                with open(repaired_path, "rb") as f:
                    st.download_button(
                        label="⬇️ Download Repaired CSV",
                        data=f,
                        file_name=f"{os.path.splitext(uploaded_file.name)[0]}_repair.csv",
                        mime="text/csv",
                        type="primary",
                    )

        except Exception as e:
            # Catch unexpected hard crashes (the ValueError catch for schema was removed)
            st.error(f"An unexpected critical error occurred: {e}")

        finally:
            # 5. Disk Cleanup
            # Always clean up the original uploaded temp file
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            # Always clean up the hidden repair temp file generated by the engine
            if "repaired_path" in locals() and os.path.exists(repaired_path):
                os.remove(repaired_path)
