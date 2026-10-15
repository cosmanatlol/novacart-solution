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

    def _latest_per_key(frame: pd.DataFrame) -> pd.DataFrame:
        """One row per product: the newest by updated_at wins; on a tie, the later row wins."""
        if "updated_at" in frame.columns:
            frame = frame.sort_values("updated_at", kind="stable", na_position="first")
        return frame.drop_duplicates(subset=[primary_key], keep="last")

    incoming = _latest_per_key(incoming)

    # Products load incrementally, so Silver only holds changed rows.
    # SCD1 = upsert: the new version replaces the old one on the same primary key
    # (price/cost and every other column are overwritten, no history is kept) and
    # every other product already in Gold stays. A stale record with an OLDER
    # updated_at than what Gold already has never overwrites the newer one.
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        merged = _latest_per_key(pd.concat([existing, incoming], ignore_index=True))
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

    # Silver order = oldest -> newest. Silver may hold SEVERAL versions of one
    # customer_id (it only drops identical records), so we keep them all here.
    incoming = _strip_internal(pd.read_parquet(src)).reset_index(drop=True)
    if incoming.empty:
        log_event(logger, "INFO", "dim_customer_skipped_no_data")
        return out_path

    fields = [f for f in (scd2_fields or DEFAULT_SCD2_FIELDS) if f in incoming.columns]
    eff = _as_of(as_of)
    incoming["_row_hash"] = incoming.apply(lambda r: _row_hash(r, fields), axis=1)
    latest = incoming.drop_duplicates(subset=[primary_key], keep="last").set_index(primary_key)

    if out_path.exists():
        existing = pd.read_parquet(out_path)
    else:
        existing = incoming.iloc[0:0].assign(
            _eff_start=pd.Series(dtype="object"), _eff_end=pd.Series(dtype="object"),
            _current=pd.Series(dtype="bool"))

    # SCD2 only moves forward. The customer source is one snapshot (not one file per
    # date), so replaying an OLDER date would apply today's data to the past and corrupt
    # the history. Gold is already past that date, so there is nothing to do.
    if not existing.empty and eff < existing["_eff_start"].max():
        log_event(logger, "INFO", "dim_customer_skipped_historic_date", date=eff,
                  latest=existing["_eff_start"].max())
        return out_path

    # Re-running the LATEST date must give the same result as the first run, so first
    # undo that date: drop rows it opened, re-open rows it closed.
    existing = existing[existing["_eff_start"] != eff].copy()
    reopen = existing["_eff_end"] == eff
    existing.loc[reopen, "_eff_end"] = _HIGH_DATE
    existing.loc[reopen, "_current"] = True

    # Walk each customer's versions in order: a version is a real change only if its
    # tracked fields differ from the version before it (or from the current Gold row).
    current = existing[existing["_current"]].set_index(primary_key)["_row_hash"]
    prev = incoming.groupby(primary_key)["_row_hash"].shift()
    prev = prev.where(prev.notna(), incoming[primary_key].map(current))
    changes = incoming[incoming["_row_hash"] != prev].copy()   # NaN prev = new customer
    changed_ids = changes[primary_key].unique()
    is_new = ~pd.Series(changed_ids).isin(current.index)

    # The newest version of each customer becomes the current row. Older versions that
    # arrived in the same batch are kept as superseded rows (_current=False); they were
    # replaced on the same effective date, so their validity window is empty.
    is_last = ~changes.duplicated(subset=[primary_key], keep="last")
    changes["_eff_start"] = eff
    changes["_eff_end"] = [_HIGH_DATE if last else eff for last in is_last]
    changes["_current"] = is_last.to_numpy()

    # Normal SCD2: close the old current row of every customer that changed.
    close_mask = existing["_current"] & existing[primary_key].isin(changed_ids)
    existing.loc[close_mask, "_eff_end"] = eff
    existing.loc[close_mask, "_current"] = False

    result = pd.concat([existing, changes], ignore_index=True)

    # Untracked attributes (e.g. signup_date, tier): keep the current row up to date
    # in place (history is only kept for the tracked fields).
    untracked = [c for c in incoming.columns
                 if c not in fields and c not in (primary_key, "_row_hash")]
    keep = result["_current"] & result[primary_key].isin(latest.index)
    for c in untracked:
        if c in result.columns:
            result.loc[keep, c] = result.loc[keep, primary_key].map(latest[c])

    result = result.sort_values([primary_key, "_eff_start", "_current"],
                                kind="stable").reset_index(drop=True)
    result.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_customer_written", rows=len(result),
              new=int(is_new.sum()), changed=int(len(changed_ids) - is_new.sum()))
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