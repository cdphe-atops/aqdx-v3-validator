"""High-performance AQDx v3 validator built on Polars.

Designed for programmatic database ingestion pipelines and CLI batch checks on
large `.csv`, `.csv.gz`, and `.parquet` datasets (up to 10M+ rows). Evaluates
vectorized Polars expressions lazily and short-circuits on the first failing rule.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Union

try:
    import polars as pl
except ImportError as exc:
    raise ImportError("polars is required for validate_aqdx_performance.") from exc

__all__ = [
    "REQUIRED_HEADERS",
    "VALIDATION_RULES",
    "ValidationResult",
    "ValidationRule",
    "uncurated_validity_expr",
    "scan_validated_file",
    "validate_file",
    "main",
]

REQUIRED_HEADERS: tuple[str, ...] = (
    "sample_datetime",
    "parameter_code",
    "parameter_value",
    "unit_code",
    "method_code",
    "sample_duration",
    "aggregation_code",
    "latitude",
    "longitude",
    "elevation",
    "data_steward_name",
    "device_id",
    "measurement_technology_code",
    "instrument_classification",
    "dataset_id",
    "validity_code",
    "calibration_code",
    "review_level_code",
    "detection_limit",
    "qualifier_codes",
)

EXCLUDED_VALIDITY_CODES: frozenset[str] = frozenset(
    {"0", "8", "9", "0.0", "8.0", "9.0"}
)
NULL_VALIDITY_STRINGS: frozenset[str] = frozenset(
    {"", "nan", "none", "<na>", "null", "n/a", "na", "-999", "-9999", "missing"}
)


@dataclass(frozen=True)
class ValidationRule:
    """Declarative AQDx v3 validation rule backed by a vectorized Polars expression.

    `expr` must evaluate to True for valid rows and False for invalid rows.
    """

    name: str
    columns: tuple[str, ...]
    description: str
    expr: pl.Expr

    def violation_expr(self) -> pl.Expr:
        """Boolean expression that evaluates to True on rows violating this rule."""
        return self.expr.fill_null(False).not_()


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of a strict AQDx pass/fail validation run."""

    passed: bool
    failed_rule: str | None = None
    reason: str | None = None
    missing_headers: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return self.passed


# --- Vectorized Expression Builders ---


def _required_regex_expr(
    col_name: str, pattern: str, max_length: int | None = None
) -> pl.Expr:
    c = pl.col(col_name)
    valid = c.is_not_null() & c.str.contains(pattern).fill_null(False)
    if max_length is not None:
        valid = valid & (c.str.len_chars() <= max_length).fill_null(False)
    return valid


def _optional_regex_expr(
    col_name: str, pattern: str, max_length: int | None = None
) -> pl.Expr:
    c = pl.col(col_name)
    valid = c.str.contains(pattern).fill_null(False)
    if max_length is not None:
        valid = valid & (c.str.len_chars() <= max_length).fill_null(False)
    return c.is_null() | valid


def _int_expr(
    col_name: str, min_val: int | None = None, max_val: int | None = None
) -> pl.Expr:
    c = pl.col(col_name)
    casted = c.cast(pl.Int64, strict=False)
    valid = (
        c.is_not_null()
        & c.str.contains(r"^[+-]?[0-9]+$").fill_null(False)
        & casted.is_not_null()
    )
    if min_val is not None and max_val is not None:
        valid = valid & casted.is_between(min_val, max_val).fill_null(False)
    return valid


def _decimal_expr(
    col_name: str, max_digits: int, decimal_places: int, *, nullable: bool
) -> pl.Expr:
    max_int_digits = max_digits - decimal_places
    if max_int_digits < 0:
        raise ValueError(
            f"max_digits ({max_digits}) must be >= decimal_places ({decimal_places})."
        )

    c = pl.col(col_name)
    pattern = (
        rf"^[+-]?(?:[0-9]+(?:\.[0-9]{{1,{decimal_places}}})?"
        rf"|\.[0-9]{{1,{decimal_places}}})$"
    )
    integer_digits = (
        c.str.replace(r"^[+-]", "")
        .str.replace(r"\..*$", "")
        .str.replace(r"^0+", "")
        .str.len_chars()
    )
    valid_decimal = c.str.contains(pattern).fill_null(False) & (
        integer_digits <= max_int_digits
    ).fill_null(False)
    if nullable:
        return c.is_null() | valid_decimal
    return c.is_not_null() & valid_decimal


def uncurated_validity_expr() -> pl.Expr:
    """Boolean expression matching uncurated rows (null or validity_code in [0, 8, 9])."""
    vc = pl.col("validity_code")
    vc_norm = vc.str.strip_chars().str.to_lowercase()
    vc_float = vc_norm.cast(pl.Float64, strict=False)
    return (
        vc.is_null()
        | vc_norm.is_in(list(NULL_VALIDITY_STRINGS | EXCLUDED_VALIDITY_CODES))
        | vc_float.is_in([0.0, 8.0, 9.0]).fill_null(False)
    )


# --- Declarative AQDx v3 Validation Rules ---

VALIDATION_RULES: tuple[ValidationRule, ...] = (
    # 1. Time & Measurement
    ValidationRule(
        name="sample_datetime",
        columns=("sample_datetime",),
        description="Required ISO 8601 timestamp with mandatory numeric timezone offset (max 29 chars).",
        expr=_required_regex_expr(
            "sample_datetime",
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,3})?[+-][0-9]{2}:[0-9]{2}$",
            max_length=29,
        ),
    ),
    ValidationRule(
        name="parameter_code",
        columns=("parameter_code",),
        description="Required 5-digit zero-padded numeric string (^[0-9]{5}$).",
        expr=_required_regex_expr("parameter_code", r"^[0-9]{5}$"),
    ),
    ValidationRule(
        name="parameter_value",
        columns=("parameter_value",),
        description="Optional decimal with precision=12 and scale=5 (max 7 integer digits, 5 decimal places).",
        expr=_decimal_expr(
            "parameter_value", max_digits=12, decimal_places=5, nullable=True
        ),
    ),
    ValidationRule(
        name="unit_code",
        columns=("unit_code",),
        description="Required 3-digit zero-padded numeric string (^[0-9]{3}$).",
        expr=_required_regex_expr("unit_code", r"^[0-9]{3}$"),
    ),
    ValidationRule(
        name="method_code",
        columns=("method_code",),
        description="Optional 3-digit zero-padded numeric string (^[0-9]{3}$).",
        expr=_optional_regex_expr("method_code", r"^[0-9]{3}$"),
    ),
    ValidationRule(
        name="sample_duration",
        columns=("sample_duration",),
        description="Required decimal with precision=12 and scale=3 (max 9 integer digits, 3 decimal places).",
        expr=_decimal_expr(
            "sample_duration", max_digits=12, decimal_places=3, nullable=False
        ),
    ),
    ValidationRule(
        name="aggregation_code",
        columns=("aggregation_code",),
        description="Required single-digit integer (-9 to 9).",
        expr=_int_expr("aggregation_code", min_val=-9, max_val=9),
    ),
    # 2. Location
    ValidationRule(
        name="latitude",
        columns=("latitude",),
        description="Optional decimal with precision=9 and scale=5 (max 4 integer digits, 5 decimal places).",
        expr=_decimal_expr("latitude", max_digits=9, decimal_places=5, nullable=True),
    ),
    ValidationRule(
        name="longitude",
        columns=("longitude",),
        description="Optional decimal with precision=9 and scale=5 (max 4 integer digits, 5 decimal places).",
        expr=_decimal_expr("longitude", max_digits=9, decimal_places=5, nullable=True),
    ),
    ValidationRule(
        name="latitude_range",
        columns=("latitude",),
        description="Latitude must be between -90 and 90 degrees inclusive.",
        expr=(
            pl.col("latitude").is_null()
            | pl.col("latitude")
            .cast(pl.Float64, strict=False)
            .is_between(-90.0, 90.0)
            .fill_null(False)
        ),
    ),
    ValidationRule(
        name="longitude_range",
        columns=("longitude",),
        description="Longitude must be between -180 and 180 degrees inclusive.",
        expr=(
            pl.col("longitude").is_null()
            | pl.col("longitude")
            .cast(pl.Float64, strict=False)
            .is_between(-180.0, 180.0)
            .fill_null(False)
        ),
    ),
    ValidationRule(
        name="elevation",
        columns=("elevation",),
        description="Optional decimal with precision=8 and scale=2 (max 6 integer digits, 2 decimal places).",
        expr=_decimal_expr("elevation", max_digits=8, decimal_places=2, nullable=True),
    ),
    # 3. Device & Organization
    ValidationRule(
        name="data_steward_name",
        columns=("data_steward_name",),
        description="Required string without commas, periods, or whitespace (max 64 chars).",
        expr=_required_regex_expr("data_steward_name", r"^[^,.\s]+$", max_length=64),
    ),
    ValidationRule(
        name="device_id",
        columns=("device_id",),
        description="Required string without commas or periods (max 64 chars).",
        expr=_required_regex_expr("device_id", r"^[^,.]+$", max_length=64),
    ),
    ValidationRule(
        name="measurement_technology_code",
        columns=("measurement_technology_code",),
        description="Required hyphen-delimited technology code matching AQDx pattern (max 14 chars).",
        expr=_required_regex_expr(
            "measurement_technology_code",
            r"^[A-Z]{2}[a-z]{0,2}-(?:[A-Z]{2}[a-z]{0,2}|00)-[A-Z]{2}[a-z]{0,2}$",
            max_length=14,
        ),
    ),
    ValidationRule(
        name="instrument_classification",
        columns=("instrument_classification",),
        description="Required integer between 1 and 3 inclusive.",
        expr=_int_expr("instrument_classification", min_val=1, max_val=3),
    ),
    ValidationRule(
        name="dataset_id",
        columns=("dataset_id",),
        description="Required string matching ^[\\w.-]+$ (max 128 chars).",
        expr=_required_regex_expr("dataset_id", r"^[\w.-]+$", max_length=128),
    ),
    # 4. Quality Control
    ValidationRule(
        name="validity_code",
        columns=("validity_code",),
        description="Required single-digit integer (-9 to 9).",
        expr=_int_expr("validity_code", min_val=-9, max_val=9),
    ),
    ValidationRule(
        name="calibration_code",
        columns=("calibration_code",),
        description="Required single-digit integer (-9 to 9).",
        expr=_int_expr("calibration_code", min_val=-9, max_val=9),
    ),
    ValidationRule(
        name="review_level_code",
        columns=("review_level_code",),
        description="Required single-digit integer (-9 to 9).",
        expr=_int_expr("review_level_code", min_val=-9, max_val=9),
    ),
    ValidationRule(
        name="detection_limit",
        columns=("detection_limit",),
        description="Optional decimal with precision=12 and scale=5 (max 7 integer digits, 5 decimal places).",
        expr=_decimal_expr(
            "detection_limit", max_digits=12, decimal_places=5, nullable=True
        ),
    ),
    ValidationRule(
        name="qualifier_codes",
        columns=("qualifier_codes",),
        description="Optional space-delimited 1-2 char uppercase alphanumeric codes (max 254 chars).",
        expr=_optional_regex_expr(
            "qualifier_codes",
            r"^[A-Z0-9]{1,2}(?: [A-Z0-9]{1,2})*$",
            max_length=254,
        ),
    ),
    # 5. Cross-Field Rules
    ValidationRule(
        name="geo_missing_requires_ig",
        columns=("latitude", "longitude", "qualifier_codes"),
        description="If latitude or longitude is missing, qualifier_codes must include 'IG'.",
        expr=(
            (pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null())
            | pl.col("qualifier_codes")
            .str.contains(r"(?:^| )IG(?: |$)")
            .fill_null(False)
        ),
    ),
    ValidationRule(
        name="geo_null_island",
        columns=("latitude", "longitude"),
        description="Coordinates cannot be exactly Null Island (0.0, 0.0).",
        expr=(
            pl.col("latitude").is_null()
            | pl.col("longitude").is_null()
            | (
                (pl.col("latitude").cast(pl.Float64, strict=False) == 0.0)
                & (pl.col("longitude").cast(pl.Float64, strict=False) == 0.0)
            )
            .not_()
            .fill_null(True)
        ),
    ),
)


# --- Internal Scanning & Normalization ---


def _validate_supported_path(file_path: Union[str, Path]) -> Path:
    path = Path(file_path)
    name_lower = path.name.lower()
    if not (
        name_lower.endswith(".csv")
        or name_lower.endswith(".csv.gz")
        or name_lower.endswith(".parquet")
    ):
        raise ValueError(
            f"Unsupported file format '{path.name}'. Only .csv, .csv.gz, and .parquet files are supported."
        )
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    return path


def _normalize_lazyframe(lf: pl.LazyFrame, schema: pl.Schema) -> pl.LazyFrame:
    """Normalizes columns to trimmed `pl.String` (empty -> null) for rule parity."""
    normalized_exprs: list[pl.Expr] = []

    for col_name, dtype in schema.items():
        if col_name == "sample_datetime" and isinstance(dtype, pl.Datetime):
            if dtype.time_zone is not None:
                s = pl.col(col_name).dt.strftime("%Y-%m-%dT%H:%M:%S%.3f%:z")
            else:
                # Naive timestamps lack the mandatory timezone offset and will fail validation
                s = pl.col(col_name).dt.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            s = pl.col(col_name).cast(pl.String).str.strip_chars()

        normalized_exprs.append(
            pl.when(s == "").then(None).otherwise(s).alias(col_name)
        )

    return lf.with_columns(normalized_exprs)


# --- Public Validation API ---


def scan_validated_file(
    file_path: Union[str, Path],
    *,
    exclude_uncurated_validity: bool = False,
    allow_null_dataset_id: bool = False,
) -> tuple[ValidationResult, pl.LazyFrame | None]:
    """Strictly validate a `.csv`, `.csv.gz`, or `.parquet` file against AQDx v3.

    Evaluates rules lazily via Polars and short-circuits on the first failure.

    :param file_path: Path to a `.csv`, `.csv.gz`, or `.parquet` file.
    :param exclude_uncurated_validity: When True, ignores rows where `validity_code`
        is null/missing or in [0, 8, 9].
    :param allow_null_dataset_id: When True, allows blank or null values in the
        `dataset_id` column (the column header itself remains required).
    :return: `(ValidationResult, filtered_lazyframe)` when validation succeeds, or
        `(ValidationResult, None)` when validation fails.
    """
    path = _validate_supported_path(file_path)

    if path.name.lower().endswith(".parquet"):
        lf = pl.scan_parquet(path)
    else:
        lf = pl.scan_csv(
            path,
            infer_schema=False,
            null_values=[""],
            try_parse_dates=False,
        )

    try:
        schema = lf.collect_schema()
    except pl.exceptions.PolarsError as exc:
        return (
            ValidationResult(
                passed=False,
                failed_rule="parse_error",
                reason=f"Failed to read file schema: {exc}",
            ),
            None,
        )

    file_headers = set(schema.names())
    missing = tuple(h for h in REQUIRED_HEADERS if h not in file_headers)
    if missing:
        return (
            ValidationResult(
                passed=False,
                failed_rule="missing_headers",
                reason=f"Missing required column(s): {', '.join(missing)}",
                missing_headers=missing,
            ),
            None,
        )

    lf = _normalize_lazyframe(lf, schema)

    if exclude_uncurated_validity:
        lf = lf.filter(uncurated_validity_expr().not_())

    for rule in VALIDATION_RULES:
        if allow_null_dataset_id and rule.name == "dataset_id":
            rule = ValidationRule(
                name="dataset_id",
                columns=("dataset_id",),
                description="Optional string matching ^[\\w.-]+$ (max 128 chars).",
                expr=_optional_regex_expr("dataset_id", r"^[\w.-]+$", max_length=128),
            )

        try:
            violated = lf.filter(rule.violation_expr()).limit(1).collect().height > 0
        except pl.exceptions.PolarsError as exc:
            return (
                ValidationResult(
                    passed=False,
                    failed_rule="parse_error",
                    reason=f"Parsing failed during rule '{rule.name}': {exc}",
                ),
                None,
            )

        if violated:
            return (
                ValidationResult(
                    passed=False,
                    failed_rule=rule.name,
                    reason=rule.description,
                ),
                None,
            )

    return ValidationResult(passed=True), lf


def validate_file(
    file_path: Union[str, Path],
    *,
    exclude_uncurated_validity: bool = False,
    allow_null_dataset_id: bool = False,
) -> ValidationResult:
    """Strictly validate a `.csv`, `.csv.gz`, or `.parquet` file against AQDx v3.

    Evaluates rules lazily via Polars and short-circuits on the first failure.

    :param file_path: Path to a `.csv`, `.csv.gz`, or `.parquet` file.
    :param exclude_uncurated_validity: When True, ignores rows where `validity_code`
        is null/missing or in [0, 8, 9].
    :param allow_null_dataset_id: When True, allows blank or null values in the
        `dataset_id` column (the column header itself remains required).
    :return: `ValidationResult` (truthy on pass, falsy on fail).
    """
    result, _ = scan_validated_file(
        file_path,
        exclude_uncurated_validity=exclude_uncurated_validity,
        allow_null_dataset_id=allow_null_dataset_id,
    )
    return result


# --- CLI Entrypoint ---


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="High-performance AQDx v3 pass/fail validator (.csv, .csv.gz, and .parquet)."
    )
    parser.add_argument(
        "file_path",
        help="Path to the .csv, .csv.gz, or .parquet file to validate.",
    )
    parser.add_argument(
        "--exclude-uncurated",
        action="store_true",
        help="Ignore rows with validity_code in [0, 8, 9] or missing.",
    )
    parser.add_argument(
        "--allow-null-dataset-id",
        action="store_true",
        help="Allow blank or null values in the dataset_id column.",
    )
    args = parser.parse_args(argv)

    try:
        result = validate_file(
            args.file_path,
            exclude_uncurated_validity=args.exclude_uncurated,
            allow_null_dataset_id=args.allow_null_dataset_id,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if result:
        print(f"PASS: {args.file_path}")
        return 0

    print(f"FAIL [{result.failed_rule}]: {result.reason}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
