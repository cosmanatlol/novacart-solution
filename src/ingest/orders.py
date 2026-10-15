"""Ingest daily orders CSV → Bronze parquet."""
from __future__ import annotations
import logging
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.transform.schema_check import check_schema
from src.utils.schemas import SOURCE_SCHEMAS


def _row_hash(row: pd.Series) -> str:
    """SHA-256 of the source business columns (stable across key order and nested values)."""
    payload = json.dumps(
        [row[c] for c in SOURCE_SCHEMAS["orders"]["model"].model_fields if c in row.index], sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ingest_orders(
    date_str: str,
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
    email_cfg: Optional["EmailAlertConfig"] = None,
) -> Path:
    """Read orders_YYYY-MM-DD.csv and write to Bronze layer. Returns output path."""
    src = landing_dir / f"orders_{date_str}.csv"
    if not src.exists():
        raise IngestionError(f"orders file not found: {src}")
    try:
        df = pd.read_csv(src, encoding = 'utf-8', dtype=str)
    except UnicodeDecodeError :
        df = pd.read_csv(src, encoding = 'latin-1', dtype=str)

    log_event(logger, "INFO", "orders_ingested", date=date_str, rows=len(df))

    check_schema(list(df.columns), SOURCE_SCHEMAS["orders"]["model"], "orders", logger, email_cfg)
    
    # Add Bronze metadata
    df["_row_hash"] = df.apply(_row_hash, axis=1)
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_partition_date"] = date_str

    out_dir = bronze_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "orders_bronze_written", path=str(out_path), rows=len(df))
    return out_path
