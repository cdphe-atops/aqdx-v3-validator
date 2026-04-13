import csv
import decimal
import os
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

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

    # --- Pre-Process Auto-Repairs ---

    @model_validator(mode="before")
    @classmethod
    def pre_process_repairs(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, dict):
            return data

        repairs = info.context.get("repairs") if info.context else None

        codes = {"parameter_code": 5, "unit_code": 3, "method_code": 3}
        decimals = {
            "parameter_value": 5,
            "duration": 3,
            "latitude": 5,
            "longitude": 5,
            "elevation": 2,
            "detection_limit": 5,
        }
        forbidden_nulls = {"-999", "-9999", "nan", "na", "n/a", "null", "missing"}

        for k, v in data.items():
            if v is None:
                continue

            orig_v = str(v)
            val_str = orig_v
            actions = []

            # 0. Whitespace
            if val_str != val_str.strip():
                val_str = val_str.strip()
                actions.append("Stripped leading/trailing whitespace")

            # 1. Null placeholders
            if val_str.lower() in forbidden_nulls or val_str == "":
                data[k] = None
                if repairs is not None and orig_v != "":
                    repairs.append(
                        (k, "Replaced forbidden null placeholder with empty cell")
                    )
                continue

            # 2. String cleaning (Quotes)
            if any(q in val_str for q in ["'", "“", "”"]):
                val_str = val_str.replace("'", '"').replace("“", '"').replace("”", '"')
                actions.append(
                    "Replaced smart/single quotes with standard double quotes"
                )

            # 3. Float artifacts on codes
            if k in codes and val_str.endswith(".0"):
                val_str = val_str[:-2]
                actions.append("Stripped '.0' float artifact from string code")

            # 4. Zero padding codes
            if k in codes and val_str.isdigit() and len(val_str) < codes[k]:
                val_str = val_str.zfill(codes[k])
                actions.append(f"Padded code with leading zeros to length {codes[k]}")

            # 5. Comma stripping
            if k in decimals and "," in val_str:
                val_str = val_str.replace(",", "")
                actions.append("Removed thousands separators (commas)")

            # 6. Datetime standardizing
            if k == "datetime":
                if " " in val_str:
                    val_str = val_str.replace(" ", "T")
                    actions.append("Replaced space with 'T' in datetime string")
                if val_str.endswith("Z"):
                    val_str = val_str[:-1] + "+00:00"
                    actions.append("Replaced 'Z' with '+00:00' in datetime string")

            # 7. Rounding Decimals
            if k in decimals and val_str:
                try:
                    d = Decimal(val_str)
                    scale = decimals[k]
                    dt = d.as_tuple()
                    current_scale = abs(dt.exponent) if dt.exponent < 0 else 0
                    if current_scale > scale:
                        quantizer = Decimal(f"1e-{scale}")
                        val_str = str(
                            d.quantize(quantizer, rounding=decimal.ROUND_HALF_UP)
                        )
                        actions.append(f"Rounded decimal to max scale ({scale})")
                except Exception:
                    pass

            # Apply changes and log actions
            if actions:
                data[k] = val_str
                if repairs is not None:
                    for action in actions:
                        repairs.append((k, action))

        return data

    # --- Cross-Field Validations ---

    @model_validator(mode="after")
    def check_geo_logic(self, info: ValidationInfo) -> "AQDxRecord":
        lat = self.latitude
        lon = self.longitude

        if lat is None or lon is None:
            if not self.qualifier_codes or "IG" not in self.qualifier_codes.split():
                raise ValueError(
                    "latitude/longitude are missing, but 'IG' is not in qualifier_codes."
                )
            return self

        lat_f = float(lat)
        lon_f = float(lon)

        if lat_f == 0.0 and lon_f == 0.0:
            raise ValueError(
                "Null Island Error: Coordinates cannot be exactly (0.0, 0.0)."
            )

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
    ext = Path(filepath).suffix.lower()

    if ext in [".csv", ".gz"]:
        for chunk in pd.read_csv(filepath, chunksize=chunksize, dtype=str):
            chunk.replace({"nan": None, np.nan: None}, inplace=True)
            yield chunk.to_dict(orient="records")

    elif ext in [".xlsx", ".xls", ".ods"]:
        df = pd.read_excel(filepath, sheet_name=0, dtype=str)
        df.replace({"nan": None, np.nan: None}, inplace=True)
        records = df.to_dict(orient="records")
        for i in range(0, len(records), chunksize):
            yield records[i : i + chunksize]

    elif ext == ".parquet":
        df = pd.read_parquet(filepath)
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

    grouped_errors = defaultdict(list)
    grouped_warnings = defaultdict(list)
    grouped_repairs = defaultdict(list)
    total_rows = 0
    REQUIRED_HEADERS = set(AQDxRecord.model_fields.keys())

    temp_csv_path = path_obj.with_name(f".{path_obj.stem}_temp_repair.csv")
    repaired_csv_path = path_obj.with_name(f"{path_obj.stem}_repair.csv")

    csv_writer = None
    file_headers = []

    try:
        with open(temp_csv_path, "w", newline="", encoding="utf-8") as temp_file:
            for chunk_idx, records_chunk in enumerate(
                iter_dataframe_rows(str(path_obj))
            ):
                # 1. Header Validation (Run once)
                if chunk_idx == 0 and records_chunk:
                    file_headers = list(records_chunk[0].keys())
                    missing_headers = REQUIRED_HEADERS - set(file_headers)
                    if missing_headers:
                        print(
                            "✘ CRITICAL SCHEMA ERROR: Missing required column headers:"
                        )
                        for h in missing_headers:
                            print(f"   - {h}")
                        print("\nValidation aborted. Fix headers and try again.")
                        temp_file.close()
                        if temp_csv_path.exists():
                            os.remove(temp_csv_path)
                        sys.exit(1)

                    # Initialize CSV writer with the exact headers from the file
                    csv_writer = csv.DictWriter(temp_file, fieldnames=file_headers)
                    csv_writer.writeheader()

                # 2. Row Validation
                for idx, row in enumerate(records_chunk):
                    row_number = total_rows + 2
                    total_rows += 1

                    row_warnings = []
                    row_repairs = []

                    try:
                        AQDxRecord.model_validate(
                            row,
                            context={"warnings": row_warnings, "repairs": row_repairs},
                        )
                    except ValidationError as e:
                        for err in e.errors():
                            loc = err.get("loc", ())
                            error_name = (
                                str(loc[0]) if len(loc) > 0 else "Row-Level Error"
                            )
                            msg = err.get("msg", "Validation error").replace(
                                "Value error, ", ""
                            )
                            grouped_errors[(error_name, msg)].append(row_number)

                    # Track warnings and repairs
                    for warning_name, msg in row_warnings:
                        grouped_warnings[(warning_name, msg)].append(row_number)
                    for field_name, repair_msg in row_repairs:
                        grouped_repairs[(field_name, repair_msg)].append(row_number)

                    # Write the fully mutated row to the temp file
                    clean_row = {k: ("" if v is None else v) for k, v in row.items()}
                    csv_writer.writerow(clean_row)

        # --- Output Reports ---
        print("-" * 115)
        if not grouped_errors and not grouped_repairs:
            print(
                f"✔ SUCCESS: All {total_rows} rows perfectly match the AQDx v3 standard!"
            )
        elif not grouped_errors and grouped_repairs:
            print(
                "⚠️ CONDITIONAL PASS: The file contains formatting issues, but can be fully auto-repaired."
            )
        else:
            total_error_instances = sum(len(rows) for rows in grouped_errors.values())
            print(
                f"✘ FAILURE: Found {total_error_instances} hard error(s) across {total_rows} rows."
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

        if grouped_repairs:
            total_repair_instances = sum(len(rows) for rows in grouped_repairs.values())
            print("\n" + "=" * 115)
            print(
                f"🛠️ AUTO-REPAIR AVAILABLE: {total_repair_instances} formatting issues can be automatically fixed."
            )
            print("=" * 115)
            print(
                f"{'Field':<20} | {'Count':<7} | {'Repair Action':<60} | {'First Affected Row'}"
            )
            print("-" * 115)
            sorted_repairs = sorted(
                grouped_repairs.items(), key=lambda x: len(x[1]), reverse=True
            )
            for (field, msg), rows in sorted_repairs:
                count = len(rows)
                first_row = rows[0]
                short_msg = (msg[:57] + "...") if len(msg) > 60 else msg
                print(f"{field:<20} | {count:<7} | {short_msg:<60} | {first_row}")

    except Exception as e:
        print(f"\nCRITICAL UNHANDLED ERROR: {e}")
        if temp_csv_path.exists():
            os.remove(temp_csv_path)

    # --- User Prompt for Repairs ---
    print("\n" + "-" * 115)
    if grouped_repairs:
        user_input = input(
            f"Press 'R' to accept proposed repairs and save as {repaired_csv_path.name}, or press Enter to exit... "
        )
        if user_input.strip().lower() == "r":
            if repaired_csv_path.exists():
                os.remove(repaired_csv_path)
            os.rename(temp_csv_path, repaired_csv_path)
            print(f"\n✔ Successfully saved repaired file to: {repaired_csv_path}")
        else:
            if temp_csv_path.exists():
                os.remove(temp_csv_path)
    else:
        if temp_csv_path.exists():
            os.remove(temp_csv_path)
        input("Press Enter to close...")


if __name__ == "__main__":
    main()
