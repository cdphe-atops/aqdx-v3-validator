"""Deterministic generator for AQDx v3 validation test fixtures."""

import gzip
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TEST_DIR = ROOT / "test_files"
BASE_CSV = TEST_DIR / "valid_example.csv"


def load_base() -> pd.DataFrame:
    return pd.read_csv(BASE_CSV, dtype=str, keep_default_na=False)


def write_csv(df: pd.DataFrame, name: str) -> None:
    df.to_csv(TEST_DIR / name, index=False)


def main() -> None:
    base = load_base()

    # 1. Multi-format equivalents (Parquet & Gzipped CSV)
    base.to_parquet(TEST_DIR / "valid_example.parquet", index=False)
    with (
        BASE_CSV.open("rb") as src,
        gzip.open(TEST_DIR / "valid_example.csv.gz", "wb") as dst,
    ):
        dst.write(src.read())

    # 2. Qualified missing parameter_value cases (should pass with 0 warnings)
    df = base.copy()
    df.loc[0, ["parameter_value", "validity_code", "qualifier_codes"]] = ["", "1", "ND"]
    df.loc[1, ["parameter_value", "validity_code", "qualifier_codes"]] = ["", "8", "AX"]
    df.loc[2, ["parameter_value", "validity_code", "qualifier_codes"]] = [
        "",
        "8",
        "AY CO",
    ]
    df.loc[3, ["parameter_value", "validity_code", "qualifier_codes"]] = ["", "0", ""]
    df.loc[4, ["parameter_value", "validity_code", "qualifier_codes"]] = ["", "9", ""]
    write_csv(df, "valid_missing_values_qualified.csv")

    # 3. Missing coordinates properly flagged with 'IG'
    df = base.copy()
    df.loc[0, ["latitude", "longitude", "qualifier_codes"]] = ["", "", "IG"]
    df.loc[1, ["latitude", "longitude", "qualifier_codes"]] = ["39.784", "", "AX IG"]
    write_csv(df, "valid_missing_geo_with_ig.csv")

    # 4. Missing dataset_id (warning in Pydantic, conditional pass in Polars)
    df = base.copy()
    df["dataset_id"] = ""
    write_csv(df, "warn_missing_dataset_id.csv")

    # 5. Soft US geospatial warnings (valid global coordinates, outside CONUS)
    df = base.copy()
    df.loc[0, ["latitude", "longitude"]] = [
        "-75.0",
        "40.0",
    ]  # CONUS swap within [-90, 90]
    df.loc[1, ["latitude", "longitude"]] = [
        "61.2181",
        "-149.9003",
    ]  # Anchorage, AK (out of CONUS)
    write_csv(df, "warn_geo_us_bounds.csv")

    # 6. Unjustified missing parameter_value warnings
    df = base.copy()
    df.loc[0, ["parameter_value", "validity_code", "qualifier_codes"]] = ["", "8", "ND"]
    df.loc[1, ["parameter_value", "validity_code", "qualifier_codes"]] = ["", "2", ""]
    write_csv(df, "warn_missing_values_unjustified.csv")

    # 7. Auto-repair coverage (whitespace, null placeholders, commas, smart quotes, .0 codes)
    df = base.copy()
    df.loc[0, "data_steward_name"] = "  cdphe_apcd_atops  "
    df.loc[1, "elevation"] = "1,609.34"
    df.loc[2, "device_id"] = "“CAT-01”"
    df.loc[3, "qualifier_codes"] = "-999"
    df.loc[4, "detection_limit"] = "N/A"
    df.loc[5, "method_code"] = "42.0"
    write_csv(df, "repair_formatting_edge_cases.csv")

    # 8. Hard error: missing coordinates without 'IG'
    df = base.copy()
    df.loc[0, ["latitude", "longitude", "qualifier_codes"]] = ["", "", "AX"]
    write_csv(df, "error_missing_geo_without_ig.csv")

    # 9. Uncurated rows containing bad data (for exclude_uncurated_validity=True)
    df = base.copy()
    for idx, vc in enumerate(["0", "8", "9", ""]):
        df.loc[idx, "validity_code"] = vc
        df.loc[idx, "parameter_value"] = "12.12345678"
        df.loc[idx, "unit_code"] = "7"
    write_csv(df, "error_uncurated_dirty_rows.csv")

    # 10. Hard field-level constraint errors (and unrepairable overflow)
    df = base.copy()
    df.loc[0, "measurement_technology_code"] = "bad-tech-code"
    df.loc[1, "qualifier_codes"] = "ND,AX"
    df.loc[2, "instrument_classification"] = "4"
    df.loc[3, "parameter_value"] = (
        "12345678.123456"  # exceeds max_digits=12 even after rounding
    )
    write_csv(df, "error_field_constraints.csv")

    print(f"Generated test fixtures in {TEST_DIR}")


if __name__ == "__main__":
    main()
