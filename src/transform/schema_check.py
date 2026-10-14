"""Schema drift detection: additive drift warns, subtractive drift raises."""
from __future__ import annotations
import logging
from pydantic import BaseModel
from src.utils.exceptions import SchemaError
from src.utils.logging_setup import log_event


def check_schema(
    df_columns: list[str],
    expected_columns: list[str] | type[BaseModel],
    source_name: str,
    logger: logging.Logger,
) -> None:
    """
    Compare actual DataFrame columns against an expected schema contract.

    Supports either an explicit list of column names or a Pydantic model class:
    - When a Pydantic model class is provided, required columns are derived from
      non-optional model fields (field.is_required()), while all model fields are
      treated as known to avoid false extra-column warnings for optional fields.
    - When a list of column names is provided, all listed columns are treated as
      both required and known (legacy behavior).

    Drift Handling:
    - Extra columns (additive drift)      → Logs structured WARNING event, execution continues.
    - Missing columns (subtractive drift)  → Raises SchemaError.

    Args:
        df_columns: Column names present in the incoming DataFrame.
        expected_columns: An explicit list of expected column names or a Pydantic
            BaseModel class defining the schema contract.
        source_name: Name of the data source (e.g. "orders", "customers", "products")
            used for error reporting and log context.
        logger: Logger instance used to record structured drift warnings.

    Raises:
        SchemaError: If any required columns are missing from df_columns.
    """
    actual = set(df_columns)

    if isinstance(expected_columns, type) and issubclass(expected_columns, BaseModel):
        # Known columns include all fields (required + optional)
        known = set(expected_columns.model_fields.keys())
        # Required columns only include non-optional fields
        required = {
            name
            for name, field in expected_columns.model_fields.items()
            if field.is_required()
        }
    else: # Keep the current `list[str]` path so existing callers are unaffected
        required = set(expected_columns)
        known = set(expected_columns)

    added = actual - known
    missing = required - actual

    if added:
        log_event(logger, "WARNING", "schema_drift_extra_columns", source=source_name, columns=sorted(added))

    if missing:
        raise SchemaError(
            f"[schema_drift] {source_name}: required columns missing {sorted(missing)}"
        )
