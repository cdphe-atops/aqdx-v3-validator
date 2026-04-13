import os
import tempfile

import pandas as pd
import streamlit as st

# Import your core validation engine!
from src.validate_aqdx import process_file

# --- Page Setup ---
st.set_page_config(page_title="AQDx Validator v3.0", page_icon="✅", layout="wide")

st.title("AQDx Standard Format Validator (v3.0)")
st.markdown("Upload your tabular data file to validate it against the AQDx v3 schema.")
st.markdown("---")

# --- File Uploader ---
uploaded_file = st.file_uploader(
    "Choose a file (CSV, XLSX, Parquet)", type=["csv", "xlsx", "parquet"]
)

if uploaded_file is not None:
    # Streamlit holds files in RAM. We write it to a temp file on the disk
    # so your Pandas streaming engine can chunk it properly.
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
            repaired_path = results["repaired_file_path"]

            st.markdown("---")

            # 2. Render Main Status & Errors
            if not grouped_errors and not grouped_repairs:
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

                # Convert error dictionary to DataFrame for Streamlit UI
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

                # Offer the repaired file as a download
                if os.path.exists(repaired_path):
                    with open(repaired_path, "rb") as f:
                        st.download_button(
                            label="⬇️ Download Repaired CSV",
                            data=f,
                            file_name=f"{os.path.splitext(uploaded_file.name)[0]}_repair.csv",
                            mime="text/csv",
                            type="primary",
                        )

        except ValueError as ve:
            # Cleanly catch the critical missing-header errors
            st.error("✘ **CRITICAL SCHEMA ERROR**")
            st.code(str(ve))

        except Exception as e:
            # Catch unexpected hard crashes
            st.error(f"An unexpected critical error occurred: {e}")

        finally:
            # 5. Disk Cleanup
            # Always clean up the original uploaded temp file
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            # Always clean up the hidden repair temp file generated by the engine
            if "repaired_path" in locals() and os.path.exists(repaired_path):
                os.remove(repaired_path)
