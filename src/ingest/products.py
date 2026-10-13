"""Ingest products from SQLite with watermark-based incremental loading → Bronze parquet."""
from __future__ import annotations
import logging
import sqlite3
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.utils.state import StateManager
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "product_id", "name", "category", "unit_cost", "supplier_id", "updated_at",
]
WATERMARK_KEY = "products_updated_at"

def _row_hash(row: pd.Series) -> str:
    """SHA-256 of the source business columns (stable across key order and nested values)."""
    payload = json.dumps(
        [row[c] for c in EXPECTED_COLUMNS], sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def ingest_products(
    db_path: Path,
    bronze_dir: Path,
    state: StateManager,
    logger: logging.Logger,
) -> Path:
    """
    Incrementally load only rows newer than the stored watermark.
    Advances the watermark after a successful write.
    """
    if not db_path.exists():
        raise IngestionError(f"products DB not found: {db_path}")

    watermark = state.get_watermark(WATERMARK_KEY) or "1970-01-01T00:00:00"
    log_event(logger, "INFO", "products_watermark_read", watermark=watermark)

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM products WHERE updated_at > ? ORDER BY updated_at",
            conn,
            params=(watermark,),
        )
    finally:
        conn.close()

    log_event(logger, "INFO", "products_ingested", rows=len(df), since=watermark)

    if df.empty:
        log_event(logger, "INFO", "products_no_new_rows")
        out_dir = bronze_dir / "products"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / "data.parquet"

    check_schema(list(df.columns), EXPECTED_COLUMNS, "products", logger)

    df["_row_hash"] = df.apply(_row_hash, axis=1)
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = db_path.name


    out_dir = bronze_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    new_watermark = str(df["updated_at"].max())
    state.set_watermark(WATERMARK_KEY, new_watermark)
    log_event(logger, "INFO", "products_watermark_advanced", new_watermark=new_watermark)

    return out_path
