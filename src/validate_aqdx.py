import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, ValidationError, ValidationInfo, model_validator

# --- Pydantic Model (AQDx v3 Schema) ---


class AQDxRecord(BaseModel):
    # 1. Time & Measurement
    datetime: str = Field(
        ...,
        max_length=29,
        pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?[+-]\d{2}:\d{2}$",
        description="ISO 8601 string with time zone offset. 'Z' is not allowed.",
    )
    parameter_code: str = Field(..., min_length=1, max_length=5, pattern=r"^\d+$")
    parameter_value: Optional[Decimal] = Field(
        default=None, max_digits=12, decimal_places=5
    )
    unit_code: str = Field(..., min_length=1, max_length=3, pattern=r"^\d+$")
    method_code: Optional[str] = Field(
        default=None, min_length=1, max_length=3, pattern=r"^\d+$"
    )
    duration: Decimal = Field(..., max_digits=12, decimal_places=3)
    aggregation_code: int = Field(...)

    # 2. Location
    latitude: Optional[Decimal] = Field(default=None, max_digits=9, decimal_places=5)
    longitude: Optional[Decimal] = Field(default=None, max_digits=9, decimal_places=5)
    elevation: Optional[Decimal] = Field(default=None, max_digits=8, decimal_places=2)

    # 3. Device & Organization
    data_steward_name: str = Field(..., max_length=64, pattern=r"^[^,.\s]+$")
    device_id: str = Field(..., max_length=64, pattern=r"^[^,.]+$")
    measurement_technology_code: str = Field(
        ...,
        max_length=14,
        pattern=r"^[A-Z]{2}[a-z]{0,2}-[A-Z]{2}[a-z]{0,2}-[A-Z]{2}[a-z]{0,2}$",
    )
    instrument_classification: int = Field(..., ge=1, le=3)
    dataset_id: str = Field(..., max_length=128, pattern=r"^[\w.-]+$")

    # 4. Quality Control
    validity_code: int = Field(...)
    calibration_code: int = Field(...)
    review_level_code: int = Field(...)
    detection_limit: Optional[Decimal] = Field(
        default=None, max_digits=12, decimal_places=5
    )
    qualifier_codes: Optional[str] = Field(default=None, max_length=254)

    # --- Cross-Field Validations (AQDx v3 Rules) ---

    @model_validator(mode="after")
    def check_geo_logic(self, info: ValidationInfo) -> "AQDxRecord":
        lat = self.latitude
        lon = self.longitude

        # Missing coordinates require IG code
        if lat is None or lon is None:
            if not self.qualifier_codes or "IG" not in self.qualifier_codes.split():
                raise ValueError(
                    "latitude/longitude are missing, but 'IG' is not in qualifier_codes."
                )
            return self

        lat_f = float(lat)
        lon_f = float(lon)

        # Null Island (This remains a strict Error)
        if lat_f == 0.0 and lon_f == 0.0:
            raise ValueError(
                "Null Island Error: Coordinates cannot be exactly (0.0, 0.0)."
            )

        # US Bounds check / Swap detection (Now recorded as WARNINGS)
        if info.context is not None and "warnings" in info.context:
            MIN_LAT, MAX_LAT = 24.0, 50.0
            MIN_LON, MAX_LON = -125.0, -66.0
            in_bounds = (MIN_LAT <= lat_f <= MAX_LAT) and (MIN_LON <= lon_f <= MAX_LON)

            if not in_bounds:
                swapped_ok = (MIN_LAT <= lon_f <= MAX_LAT) and (
                    MIN_LON <= lat_f <= MAX_LON
                )
                if swapped_ok:
                    info.context["warnings"].append(
                        (
                            "lat lon swap",
                            "Coordinates are outside expected US bounds. POSSIBLE SWAP DETECTED.",
                        )
                    )
                else:
                    info.context["warnings"].append(
                        ("out of bounds", "Coordinates are outside expected US bounds.")
                    )

        return self

    @model_validator(mode="after")
    def check_missing_values(self) -> "AQDxRecord":
        if self.parameter_value is None:
            if self.validity_code not in (0, 9):
                raise ValueError(
                    "If parameter_value is missing, validity_code must be 0 (Not Validated) or 9 (Invalid/Missing)."
                )
        return self


# --- File Processing & Execution ---


def iter_dataframe_rows(filepath: str, chunksize: int = 50000):
    """
    Reads tabular files in chunks, casting all data to string to preserve leading
    zeros on codes, and handling NaN to None conversions for Pydantic.
    """
    ext = Path(filepath).suffix.lower()

    if ext in [".csv", ".gz"]:
        # Pandas can natively chunk CSVs
        for chunk in pd.read_csv(filepath, chunksize=chunksize, dtype=str):
            # Replace Pandas string 'nan' and actual NaNs with None
            chunk.replace({"nan": None, np.nan: None}, inplace=True)
            yield chunk.to_dict(orient="records")

    elif ext in [".xlsx", ".xls", ".ods"]:
        # Excel: read first sheet entirely, then chunk the dict generation
        df = pd.read_excel(filepath, sheet_name=0, dtype=str)
        df.replace({"nan": None, np.nan: None}, inplace=True)
        records = df.to_dict(orient="records")
        for i in range(0, len(records), chunksize):
            yield records[i : i + chunksize]

    elif ext == ".parquet":
        # Parquet: read entirely (highly compressed), then chunk
        df = pd.read_parquet(filepath)
        # Cast to string to match CSV behavior
        df = df.astype(str)
        df.replace({"nan": None, "None": None, "<NA>": None}, inplace=True)
        records = df.to_dict(orient="records")
        for i in range(0, len(records), chunksize):
            yield records[i : i + chunksize]
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def main():
    print("-" * 115)
    print("   AQDx Standard Format Validator (v3.0) - Pydantic Engine")
    print("-" * 115)

    if len(sys.argv) < 2:
        print("\nUsage: Drag and drop your data file onto this executable.")
        input("\nPress Enter to exit...")
        sys.exit(1)

    raw_path = sys.argv[1]
    path_obj = Path(raw_path).resolve()

    if not path_obj.exists():
        print(f"\nError: File not found: {str(path_obj)}")
        input("\nPress Enter to exit...")
        sys.exit(1)

    print(f"\nValidating: {path_obj.name}")
    print("Processing (this may take a moment for large files)...\n")

    # Initialize variables for the validation run
    grouped_errors = defaultdict(list)
    grouped_warnings = defaultdict(list)
    total_rows = 0
    REQUIRED_HEADERS = set(AQDxRecord.model_fields.keys())

    try:
        for chunk_idx, records_chunk in enumerate(iter_dataframe_rows(str(path_obj))):
            # 1. Header Validation (Run once)
            if chunk_idx == 0 and records_chunk:
                file_headers = set(records_chunk[0].keys())
                missing_headers = REQUIRED_HEADERS - file_headers
                if missing_headers:
                    print("✘ CRITICAL SCHEMA ERROR: Missing required column headers:")
                    for h in missing_headers:
                        print(f"   - {h}")
                    print("\nValidation aborted. Fix headers and try again.")
                    sys.exit(1)

            # 2. Row Validation
            for idx, row in enumerate(records_chunk):
                row_number = total_rows + 2
                total_rows += 1

                # Create a fresh list to catch warnings for this specific row
                row_warnings = []

                try:
                    # Execute Validation, passing the warnings list into the model context
                    AQDxRecord.model_validate(row, context={"warnings": row_warnings})
                except ValidationError as e:
                    for err in e.errors():
                        loc = err.get("loc", ())
                        error_name = str(loc[0]) if len(loc) > 0 else "Row-Level Error"
                        msg = err.get("msg", "Validation error").replace(
                            "Value error, ", ""
                        )
                        grouped_errors[(error_name, msg)].append(row_number)

                # If the row generated any warnings, add them to our global warning tracker
                for warning_name, msg in row_warnings:
                    grouped_warnings[(warning_name, msg)].append(row_number)

        # --- Output Report ---
        print("-" * 115)
        if not grouped_errors:
            print(f"✔ SUCCESS: All {total_rows} rows passed AQDx v3 validation!")
        else:
            total_error_instances = sum(len(rows) for rows in grouped_errors.values())
            print(
                f"✘ FAILURE: Found {total_error_instances} total error(s) across {total_rows} rows."
            )
            print("-" * 115)
            print(
                f"{'Error Name':<20} | {'Count':<7} | {'Message':<60} | {'First Affected Row'}"
            )
            print("-" * 115)

            sorted_errors = sorted(
                grouped_errors.items(), key=lambda x: len(x[1]), reverse=True
            )
            for (error_name, msg), rows in sorted_errors:
                count = len(rows)
                first_row = rows[0]
                short_msg = (msg[:57] + "...") if len(msg) > 60 else msg
                print(f"{error_name:<20} | {count:<7} | {short_msg:<60} | {first_row}")

        # --- Warnings Report ---
        if grouped_warnings:
            total_warning_instances = sum(
                len(rows) for rows in grouped_warnings.values()
            )
            print("\n" + "-" * 115)
            print(
                f"⚠️ WARNINGS: Found {total_warning_instances} potential issue(s). (These do not invalidate the file)"
            )
            print("-" * 115)
            print(
                f"{'Warning Name':<20} | {'Count':<7} | {'Message':<60} | {'First Affected Row'}"
            )
            print("-" * 115)

            sorted_warnings = sorted(
                grouped_warnings.items(), key=lambda x: len(x[1]), reverse=True
            )
            for (warning_name, msg), rows in sorted_warnings:
                count = len(rows)
                first_row = rows[0]
                short_msg = (msg[:57] + "...") if len(msg) > 60 else msg
                print(
                    f"{warning_name:<20} | {count:<7} | {short_msg:<60} | {first_row}"
                )

    except Exception as e:
        print(f"\nCRITICAL UNHANDLED ERROR: {e}")

    print("-" * 115)
    input("\nPress Enter to close...")


if __name__ == "__main__":
    main()
