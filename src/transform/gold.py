"""Silver -> Gold: star schema (dim_date, dim_product SCD1, dim_customer SCD2, fact_orders).

Design rules
------------
* Idempotent: running the same date twice yields identical Gold output.
  - fact_orders: delete the _load_date partition, then write it again.
  - dim_product: upsert by product_id (same input -> same table).
  - dim_customer: a row is only closed/opened when a tracked field really changes.
  - dim_date: fully deterministic calendar, rewritten identically every time.
* No wall-clock timestamps are written into Gold data (they would break idempotency).
* Function signatures of the original gold.py are preserved; new arguments are optional.
"""
from __future__ import annotations

import hashlib
import logging
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.config import Config
from src.utils.logging_setup import log_event

_HIGH_DATE = "9999-12-31"
DIM_DATE_START = "2024-01-01"
DIM_DATE_END = "2026-12-31"

# Used ONLY when a caller does not pass `config` (e.g. a quick standalone call).
# The pipeline always passes config, so the real keys come from pipeline.yaml.
_FALLBACK_KEYS = {"products": "product_id", "customers": "customer_id", "orders": "order_id"}

DEFAULT_SCD2_FIELDS = ["first_name", "last_name", "email", "city", "country", "tier"]


def _strip_internal(df: pd.DataFrame) -> pd.DataFrame:
    """Drop pipeline-internal columns (those starting with an underscore)."""
    return df[[c for c in df.columns if not c.startswith("_")]].copy()


def _row_hash(row: pd.Series, fields: list[str]) -> str:
    val = "|".join(str(row.get(f, "")) for f in sorted(fields))
    return hashlib.md5(val.encode()).hexdigest()


def _primary_key(config: Config | None, source: str) -> str:
    """Primary key of a source, read from pipeline.yaml (`sources:` section)."""
    if config is None:
        return _FALLBACK_KEYS[source]
    return config.sources_cfg[source]["primary_key"]


def _as_of(as_of: str | None) -> str:
    return as_of or datetime.now(timezone.utc).date().isoformat()


# ── FR-4.1 dim_date: calendar 2024-2026 ──────────────────────────────────────

def build_dim_date(
    gold_dir: Path,
    logger: logging.Logger,
    start: str = DIM_DATE_START,
    end: str = DIM_DATE_END,
) -> Path:
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / "dim_date.parquet"

    d = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})
    d["date_key"] = d["date"].dt.strftime("%Y%m%d").astype(int)
    d["year"] = d["date"].dt.year
    d["quarter"] = d["date"].dt.quarter
    d["month"] = d["date"].dt.month
    d["month_name"] = d["date"].dt.month_name()
    d["day"] = d["date"].dt.day
    d["day_of_week"] = d["date"].dt.dayofweek + 1  # Monday=1 ... Sunday=7
    d["day_name"] = d["date"].dt.day_name()
    d["is_weekend"] = d["day_of_week"] >= 6
    d = d[["date_key", "date", "year", "quarter", "month", "month_name",
           "day", "day_of_week", "day_name", "is_weekend"]]

    d.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_date_written", rows=len(d), start=start, end=end)
    return out_path


# ── FR-4.2 dim_product: SCD Type 1 (overwrite on key collision) ──────────────

def build_dim_product(
    silver_dir: Path,
    gold_dir: Path,
    logger: logging.Logger,
    config: Config | None = None,
) -> Path:
    primary_key = _primary_key(config, "products")
    src = silver_dir / "products" / "data.parquet"
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / "dim_product.parquet"

    if not src.exists():
        log_event(logger, "INFO", "dim_product_skipped_no_data")
        return out_path
    incoming = pd.read_parquet(src)
    if incoming.empty:
        log_event(logger, "INFO", "dim_product_skipped_no_data")
        return out_path

    incoming = _strip_internal(incoming)
    incoming = incoming.drop_duplicates(subset=[primary_key], keep="last")

    # Products load incrementally, so Silver only holds changed rows.
    # SCD1 = upsert: new version replaces the old one on the same primary key,
    # every other product already in Gold is kept.
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        merged = pd.concat([existing, incoming], ignore_index=True)
        merged = merged.drop_duplicates(subset=[primary_key], keep="last")
    else:
        merged = incoming

    merged = merged.sort_values(primary_key, kind="stable").reset_index(drop=True)
    merged.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_product_written",
              rows=len(merged), incoming=len(incoming))
    return out_path


# ── FR-4.3 dim_customer: SCD Type 2 (close out old, insert new) ──────────────

def build_dim_customer(
    silver_dir: Path,
    gold_dir: Path,
    scd2_fields: list[str] | None,
    logger: logging.Logger,
    as_of: str | None = None,
    config: Config | None = None,
) -> Path:
    primary_key = _primary_key(config, "customers")
    src = silver_dir / "customers" / "data.parquet"
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / "dim_customer.parquet"

    if not src.exists():
        log_event(logger, "INFO", "dim_customer_skipped_no_data")
        return out_path

    incoming = _strip_internal(pd.read_parquet(src))
    if incoming.empty:
        log_event(logger, "INFO", "dim_customer_skipped_no_data")
        return out_path
    incoming = incoming.drop_duplicates(subset=[primary_key], keep="last")

    fields = [f for f in (scd2_fields or DEFAULT_SCD2_FIELDS) if f in incoming.columns]
    eff = _as_of(as_of)
    incoming["_row_hash"] = incoming.apply(lambda r: _row_hash(r, fields), axis=1)

    def _open(frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy()
        frame["_eff_start"] = eff
        frame["_eff_end"] = _HIGH_DATE
        frame["_current"] = True
        return frame

    if not out_path.exists():
        result = _open(incoming)
        result.to_parquet(out_path, index=False)
        log_event(logger, "INFO", "dim_customer_initial_load", rows=len(result))
        return out_path

    existing = pd.read_parquet(out_path)
    current = existing[existing["_current"]].set_index(primary_key)["_row_hash"]

    cur_hash = incoming[primary_key].map(current)          # NaN when customer is new
    is_new = cur_hash.isna()
    is_changed = ~is_new & (cur_hash != incoming["_row_hash"])
    is_same = ~is_new & ~is_changed

    changed_ids = incoming.loc[is_changed, primary_key]
    to_insert = _open(incoming[is_new | is_changed])

    is_cur = existing["_current"]
    # A tracked field changed again on the SAME effective date (e.g. a corrected
    # re-run): replace that row instead of leaving a zero-length history row.
    same_day = is_cur & existing[primary_key].isin(changed_ids) & (existing["_eff_start"] == eff)
    existing = existing[~same_day].copy()
    is_cur = existing["_current"]

    # Normal SCD2: close the old current row for every changed customer.
    close_mask = is_cur & existing[primary_key].isin(changed_ids)
    existing.loc[close_mask, "_eff_end"] = eff
    existing.loc[close_mask, "_current"] = False

    # Untracked attributes (e.g. signup_date) of unchanged customers: refresh the
    # current row in place so it never goes stale (history is only for tracked fields).
    untracked = [c for c in incoming.columns
                 if c not in fields and c not in (primary_key, "_row_hash") and c in existing.columns]
    if untracked:
        keep = existing["_current"] & existing[primary_key].isin(incoming.loc[is_same, primary_key])
        latest = incoming.set_index(primary_key)
        for c in untracked:
            existing.loc[keep, c] = existing.loc[keep, primary_key].map(latest[c])

    result = pd.concat([existing, to_insert], ignore_index=True)
    result = result.sort_values([primary_key, "_eff_start"], kind="stable").reset_index(drop=True)
    result.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_customer_written", rows=len(result),
              new=int(is_new.sum()), changed=int(is_changed.sum()))
    return out_path


# ── FR-4.4 fact_orders: partition-replace by _load_date ──────────────────────

def build_fact_orders(
    date_str: str,
    silver_dir: Path,
    gold_dir: Path,
    logger: logging.Logger,
    config: Config | None = None,
) -> Path:
    primary_key = _primary_key(config, "orders")
    src = silver_dir / "orders" / f"date={date_str}" / "data.parquet"
    part_dir = gold_dir / "fact_orders" / f"date={date_str}"
    out_path = part_dir / "data.parquet"

    if not src.exists():
        log_event(logger, "INFO", "fact_orders_skipped_no_data", date=date_str)
        return out_path

    # Make sure the calendar dimension exists (cheap, deterministic).
    if not (gold_dir / "dim_date.parquet").exists():
        build_dim_date(gold_dir, logger)

    df = _strip_internal(pd.read_parquet(src))
    df = df.drop_duplicates(subset=[primary_key], keep="last")

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)
    df["_load_date"] = date_str
    df = df.sort_values(primary_key, kind="stable").reset_index(drop=True)

    # Delete this date's partition, then write fresh -> no duplicates on re-run.
    if part_dir.exists():
        shutil.rmtree(part_dir)
    part_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "fact_orders_written", date=date_str, rows=len(df))
    return out_path