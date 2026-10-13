"""Bronze → Silver: validate with Pydantic, dedupe, quarantine bad rows."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Type
import ast 

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.utils.logging_setup import log_event
from src.utils.schemas import OrderRow, CustomerRow, ProductRow


def _validate_df(
    df: pd.DataFrame,
    model: Type[BaseModel],
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
) -> pd.DataFrame:
    """Validate each row with Pydantic. Good rows → Silver, bad rows → quarantine."""
    good, bad = [], []
    for _, row in df.iterrows():
        try:
            validated = model.model_validate(row.to_dict())
            cleaned = row.to_dict()
            cleaned.update(validated.model_dump(mode="json"))
            good.append(cleaned)
        except ValidationError as exc:
            row_dict = row.to_dict()
            row_dict["_error_reason"] = str(exc)
            row_dict["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            bad.append(row_dict)

    if bad:
        q_dir = quarantine_path / source_name
        q_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%f"
        )
        pd.DataFrame(bad).to_parquet(q_dir / f"{ts}.parquet", index=False)
        log_event(logger, "WARNING", f"{source_name}_quarantined", count=len(bad))

    result = pd.DataFrame(good) if good else pd.DataFrame(columns=df.columns)

    # Deduplicate on primary key — keep last occurrence
    if primary_key in result.columns and not result.empty:
        before = len(result)
        result = (
            result.sort_values("_ingested_at", kind="stable")
            .drop_duplicates(subset=[primary_key], keep="last")
        )
        dupes = before - len(result)
        if dupes:
            log_event(logger, "INFO", f"{source_name}_deduped", dropped=dupes)

    return result.reset_index(drop=True)


def build_silver_orders(
    date_str: str,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "orders" / f"date={date_str}" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_orders_no_bronze", date=date_str)
        return silver_dir / "orders" / f"date={date_str}" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, OrderRow, "order_id", quarantine_dir, logger, "orders")

    out_dir = silver_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_orders_written", rows=len(df), date=date_str)
    return out_path


def build_silver_customers(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "customers" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_customers_no_bronze")
        return silver_dir / "customers" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, CustomerRow, "customer_id", quarantine_dir, logger, "customers")
    df['address'] = df['address'].apply(ast.literal_eval)
    df = df.drop(columns=['address']).join(df['address'].apply(pd.Series))
    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_customers_written", rows=len(df))
    return out_path


def build_silver_products(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "products" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_products_no_bronze")
        return silver_dir / "products" / "data.parquet"

    df = pd.read_parquet(src)
    if df.empty:
        return silver_dir / "products" / "data.parquet"

    df = _validate_df(df, ProductRow, "product_id", quarantine_dir, logger, "products")

    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_products_written", rows=len(df))
    return out_path
