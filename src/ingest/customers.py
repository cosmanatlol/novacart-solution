"""Ingest customers nested JSON → Bronze parquet."""
from __future__ import annotations
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "customer_id", "first_name", "last_name", "email",
    "address", "signup_date", "tier",
]


def _row_hash(row: pd.Series) -> str:
    """SHA-256 of the source business columns (stable across key order and nested values)."""
    payload = json.dumps(
        [row[c] for c in EXPECTED_COLUMNS], sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ingest_customers(
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Flatten nested JSON export and write to Bronze. Returns output path."""
    src = landing_dir / "customers.json"
    if not src.exists():
        raise IngestionError(f"customers file not found: {src}")

    df = pd.read_json(src, dtype = str)
    log_event(logger, "INFO", "customers_ingested", rows=len(df))

    check_schema(list(df.columns), EXPECTED_COLUMNS, "customers", logger)

    df["_row_hash"] = df.apply(_row_hash, axis=1)
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = bronze_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "customers_bronze_written", path=str(out_path), rows=len(df))
    return out_path
