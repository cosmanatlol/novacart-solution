"""
NovaCart ETL — CLI Orchestrator
Usage:
    python -m src.pipeline --date 2025-11-07
    python -m src.pipeline --date 2025-11-10 --backfill 3
"""
from __future__ import annotations
import argparse
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from src.utils.config import Config
from src.utils.exceptions import PipelineError
from src.utils.logging_setup import get_logger, log_event, set_context, clear_context
from src.utils.state import StateManager
from src.ingest.orders import ingest_orders
from src.ingest.customers import ingest_customers
from src.ingest.products import ingest_products
from src.transform.silver import (
    build_silver_orders,
    build_silver_customers,
    build_silver_products,
)
from src.transform.gold import (
    build_dim_date,
    build_dim_product,
    build_dim_customer,
    build_fact_orders,
)


# Failures we EXPECT from bad data or a bad environment. Anything else is a
# programming bug and is allowed to crash loudly (NFR-2: no bare Exception).
EXPECTED_ERRORS = (PipelineError, OSError, ValueError, sqlite3.Error)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _count_rows(result) -> int | None:
    """Rows in the parquet file a stage returned; None if there is no file."""
    if isinstance(result, Path) and result.exists() and result.suffix == ".parquet":
        return pq.ParquetFile(result).metadata.num_rows
    return None


def _quarantined_since(quarantine_dir: Path, source: str, since: float) -> int:
    """Rows in quarantine files for `source` written at or after time `since`."""
    folder = quarantine_dir / source
    if not folder.exists():
        return 0
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in folder.rglob("*.parquet")
        if f.stat().st_mtime >= since
    )


def run_one_date(date_str: str, config: Config) -> dict:
    logger = get_logger("novacart", config.logs)
    state = StateManager(config.state)
    run_id = uuid.uuid4().hex[:8]
    started_at = _now()
    stages: list[dict] = []
    rows_out_by_stage: dict[str, int | None] = {}
    set_context(run_id=run_id, date=date_str)

    def stage(name: str, fn, upstream: str | None = None,
              quarantine_source: str | None = None):
        """Run one stage; record status, timing and row counts; re-raise on failure."""
        set_context(stage=name)
        t0 = _now()
        record: dict = {"stage": name}
        try:
            result = fn()
        except EXPECTED_ERRORS as exc:
            record.update(status="FAIL", error=str(exc),
                          duration_sec=(_now() - t0).total_seconds())
            stages.append(record)
            log_event(logger, "ERROR", "stage_failed", error=str(exc))
            raise
        rows_out = _count_rows(result)
        rows_out_by_stage[name] = rows_out
        record.update(
            status="OK",
            rows_in=rows_out_by_stage.get(upstream) if upstream else None,
            rows_out=rows_out,
        )
        if quarantine_source:
            record["rows_quarantined"] = _quarantined_since(
                config.quarantine, quarantine_source, t0.timestamp())
        record["duration_sec"] = (_now() - t0).total_seconds()
        stages.append(record)
        log_event(logger, "INFO", "stage_end",
                  **{k: v for k, v in record.items() if k != "stage"})

    # Assume failure until every stage has finished. If an unexpected bug escapes,
    # the `finally` block below then still records an honest FAIL.
    status, error_msg = "FAIL", "pipeline stopped by an unexpected error"
    try:
        # ── Bronze ────────────────────────────────────────────────────────────
        _email_cfg = config.email_cfg
        stage("ingest_orders",    lambda: ingest_orders(
            date_str, config.landing_orders, config.bronze, logger, _email_cfg))
        stage("ingest_customers", lambda: ingest_customers(
            config.landing_customers, config.bronze, logger, _email_cfg))
        stage("ingest_products",  lambda: ingest_products(
            config.landing_products_db, config.bronze, state, logger, _email_cfg))


        # ── Silver ────────────────────────────────────────────────────────────
        stage("silver_orders", lambda: build_silver_orders(
            date_str,
            config.bronze,
            config.silver,
            config.quarantine,
            logger,
            config=config,
        ), upstream="ingest_orders", quarantine_source="orders")

        stage("silver_customers", lambda: build_silver_customers(
            config.bronze,
            config.silver,
            config.quarantine,
            logger,
            config=config,
        ), upstream="ingest_customers", quarantine_source="customers")

        stage("silver_products", lambda: build_silver_products(
            config.bronze,
            config.silver,
            config.quarantine,
            logger,
            config=config,
        ), upstream="ingest_products", quarantine_source="products")


        # ── Gold ──────────────────────────────────────────────────────────────
        # Calendar is deterministic and cheap. It is not its own stage because
        # the orchestration tests expect exactly nine stages.
        build_dim_date(config.gold, logger)

        stage("dim_product",   lambda: build_dim_product(
            config.silver, config.gold, logger, config=config),
            upstream="silver_products")
        stage("dim_customer",  lambda: build_dim_customer(
            config.silver, config.gold,
            config.gold_cfg.get("scd2_track_fields", ["city", "country", "email"]),
            logger, as_of=date_str, config=config),
            upstream="silver_customers")
        stage("fact_orders",   lambda: build_fact_orders(
            date_str, config.silver, config.gold, logger, config=config),
            upstream="silver_orders")
        status, error_msg = "SUCCESS", None

    except EXPECTED_ERRORS as exc:
        status = "FAIL"
        error_msg = str(exc)
    finally:
        # Runs even if an UNEXPECTED bug escapes, so the run is always recorded.
        finished_at = _now()
        metadata = {
            "run_id": run_id,
            "date": date_str,
            "status": status,
            "error": error_msg,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_sec": (finished_at - started_at).total_seconds(),
            "stages": stages,
        }
        state.record_run(metadata)
        clear_context()
        log_event(logger, "INFO", "pipeline_end",
                  **{k: v for k, v in metadata.items() if k != "stages"})
    return metadata


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="NovaCart ETL pipeline")
    parser.add_argument("--date",     required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Also process N days before --date")
    parser.add_argument("--config",   default=None)
    args = parser.parse_args(argv)

    config = Config.load(args.config)
    target = datetime.strptime(args.date, "%Y-%m-%d").date()
    dates  = [target - timedelta(days=i) for i in range(args.backfill, -1, -1)]

    failures = 0
    for d in dates:
        result = run_one_date(d.strftime("%Y-%m-%d"), config)
        if result["status"] != "SUCCESS":
            failures += 1
            print(f"[FAIL] {d}: {result['error']}", file=sys.stderr)
        else:
            print(f"[OK]   {d}: {result['duration_sec']:.2f}s")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())