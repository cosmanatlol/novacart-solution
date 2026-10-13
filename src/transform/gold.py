"""Silver → Gold: SCD Type 1 & 2 dimensions + idempotent fact_orders."""
from __future__ import annotations
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import log_event

_HIGH_DATE = "9999-12-31"


def _row_hash(row: pd.Series, fields: list[str]) -> str:
    val = "|".join(str(row.get(f, "")) for f in sorted(fields))
    return hashlib.md5(val.encode()).hexdigest()


# ── dim_product: SCD Type 1 (overwrite) ─────────────────────────────────────

def build_dim_product(
    silver_dir: Path,
    gold_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = silver_dir / "products" / "data.parquet"
    out_dir = gold_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dim_product.parquet"

    if not src.exists() or pd.read_parquet(src).empty:
        log_event(logger, "INFO", "dim_product_skipped_no_data")
        return out_path

    df = pd.read_parquet(src)
    # SCD1 — just keep latest snapshot; drop internal columns
    df = df[[c for c in df.columns if not c.startswith("_")]].copy()
    df["_updated_at"] = datetime.now(timezone.utc).isoformat()

    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_product_written", rows=len(df))
    return out_path


# ── dim_customer: SCD Type 2 (track history) ─────────────────────────────────

def build_dim_customer(
    silver_dir: Path,
    gold_dir: Path,
    scd2_fields: list[str],
    logger: logging.Logger,
) -> Path:
    src = silver_dir / "customers" / "data.parquet"
    out_dir = gold_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dim_customer.parquet"

    if not src.exists():
        log_event(logger, "INFO", "dim_customer_skipped_no_data")
        return out_path

    incoming = pd.read_parquet(src)
    incoming = incoming[[c for c in incoming.columns if not c.startswith("_")]].copy()
    today = datetime.now(timezone.utc).date().isoformat()

    if not out_path.exists():
        # First load — open all rows
        incoming["_eff_start"] = today
        incoming["_eff_end"] = _HIGH_DATE
        incoming["_current"] = True
        incoming["_row_hash"] = incoming.apply(
            lambda r: _row_hash(r, scd2_fields), axis=1
        )
        incoming.to_parquet(out_path, index=False)
        log_event(logger, "INFO", "dim_customer_initial_load", rows=len(incoming))
        return out_path

    existing = pd.read_parquet(out_path)
    current = existing[existing["_current"] == True].copy()

    updated_rows, new_rows = [], []

    for _, inc_row in incoming.iterrows():
        cid = inc_row["customer_id"]
        new_hash = _row_hash(inc_row, scd2_fields)
        match = current[current["customer_id"] == cid]

        if match.empty:
            # Brand-new customer
            r = inc_row.to_dict()
            r.update({"_eff_start": today, "_eff_end": _HIGH_DATE,
                       "_current": True, "_row_hash": new_hash})
            new_rows.append(r)
        elif match.iloc[0]["_row_hash"] != new_hash:
            # SCD2 — expire old row, open new row
            old_idx = match.index[0]
            existing.at[old_idx, "_eff_end"] = today
            existing.at[old_idx, "_current"] = False

            r = inc_row.to_dict()
            r.update({"_eff_start": today, "_eff_end": _HIGH_DATE,
                       "_current": True, "_row_hash": new_hash})
            new_rows.append(r)
        # else: unchanged — keep existing row as-is

    frames = [existing]
    if updated_rows:
        frames.append(pd.DataFrame(updated_rows))
    if new_rows:
        frames.append(pd.DataFrame(new_rows))

    result = pd.concat(frames, ignore_index=True)
    result.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_customer_written",
              rows=len(result), new=len(new_rows))
    return out_path


# ── fact_orders: idempotent partition-replace ─────────────────────────────────

def build_fact_orders(
    date_str: str,
    silver_dir: Path,
    gold_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = silver_dir / "orders" / f"date={date_str}" / "data.parquet"
    out_dir = gold_dir / "fact_orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    if not src.exists():
        log_event(logger, "INFO", "fact_orders_skipped_no_data", date=date_str)
        return out_path

    df = pd.read_parquet(src)
    df = df[[c for c in df.columns if not c.startswith("_")]].copy()

    # Derived metrics
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["total_amount"] = df["quantity"].astype(int) * df["unit_price"].astype(float)

    # Idempotent: full replace of this date partition
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "fact_orders_written", date=date_str, rows=len(df))
    return out_path
