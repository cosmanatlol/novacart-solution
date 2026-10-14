"""Orchestration tests: failure isolation, run metadata, exit codes, log fields."""
from __future__ import annotations
import json

import pytest

from tests.conftest import write_orders_csv, write_customers_json, make_products_db
from src import pipeline
from src.pipeline import run_one_date, main
from src.utils.config import Config

DATE = "2025-11-07"
GOOD_CUSTOMER = {
    "customer_id": "CUST-001", "first_name": "Alice", "last_name": "Smith",
    "email": "alice@example.com", "address": {"city": "NYC", "country": "US"},
    "signup_date": "2024-01-01", "tier": "gold",
}
GOOD_PRODUCT = ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00")
ORDER_ROW = ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"]


def _seed(config: Config, dates=(DATE,)) -> None:
    for d in dates:
        write_orders_csv(config.landing_orders, d, [
            [f"ORD-{d[-2:]}", "CUST-001", "PROD-001", d, "2", "49.99", "shipped"]])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])


def _runs(config: Config) -> list[dict]:
    lines = (config.state / "runs.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


def test_run_appends_one_record_with_run_id(config: Config):
    _seed(config)
    result = run_one_date(DATE, config)
    runs = _runs(config)
    assert len(runs) == 1
    assert runs[0]["run_id"] == result["run_id"]
    assert runs[0]["status"] == "SUCCESS"


def test_every_run_gets_a_different_run_id(config: Config):
    _seed(config)
    a = run_one_date(DATE, config)["run_id"]
    b = run_one_date(DATE, config)["run_id"]
    assert a != b
    assert [r["run_id"] for r in _runs(config)] == [a, b]


def test_all_nine_stages_recorded_in_order(config: Config):
    _seed(config)
    result = run_one_date(DATE, config)
    assert [s["stage"] for s in result["stages"]] == [
        "ingest_orders", "ingest_customers", "ingest_products",
        "silver_orders", "silver_customers", "silver_products",
        "dim_product", "dim_customer", "fact_orders"]


def test_failed_stage_stops_later_stages_but_run_is_recorded(config: Config):
    # No orders file -> the very first stage fails.
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])
    result = run_one_date(DATE, config)
    assert result["status"] == "FAIL"
    assert [s["stage"] for s in result["stages"]] == ["ingest_orders"]
    assert result["stages"][0]["status"] == "FAIL"
    assert _runs(config)[0]["status"] == "FAIL"


def test_row_counts_are_recorded(config: Config):
    write_orders_csv(config.landing_orders, DATE, [
        ORDER_ROW,
        ["ORD-002", "CUST-001", "PROD-001", DATE, "0", "49.99", "shipped"],  # bad qty
    ])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])
    stages = {s["stage"]: s for s in run_one_date(DATE, config)["stages"]}
    assert stages["ingest_orders"]["rows_out"] == 2
    assert stages["silver_orders"]["rows_in"] == 2
    assert stages["silver_orders"]["rows_out"] == 1
    assert stages["silver_orders"]["rows_quarantined"] == 1
    assert stages["fact_orders"]["rows_in"] == 1


def test_log_lines_carry_run_id_and_stage(config: Config):
    _seed(config)
    result = run_one_date(DATE, config)
    lines = [json.loads(l) for l in
             (config.logs / "pipeline.jsonl").read_text().splitlines()]
    stage_ends = [l for l in lines if l["event"] == "stage_end"]
    assert len(stage_ends) == 9
    assert all(l["run_id"] == result["run_id"] for l in stage_ends)
    assert {l["stage"] for l in stage_ends} >= {"silver_orders", "fact_orders"}
    silver = next(l for l in stage_ends if l["stage"] == "silver_orders")
    assert {"rows_in", "rows_out", "rows_quarantined"} <= silver.keys()


def test_backfill_continues_after_a_failed_date(config: Config, capsys):
    _seed(config, dates=("2025-11-07", "2025-11-09"))  # 2025-11-08 is missing
    code = main(["--date", "2025-11-09", "--backfill", "2",
                 "--config", "config/pipeline.yaml"])
    assert code == 1
    statuses = {r["date"]: r["status"] for r in _runs(config)}
    assert statuses == {"2025-11-07": "SUCCESS", "2025-11-08": "FAIL",
                        "2025-11-09": "SUCCESS"}


def test_exit_code_zero_when_all_dates_succeed(config: Config):
    _seed(config, dates=("2025-11-07", "2025-11-08"))
    assert main(["--date", "2025-11-08", "--backfill", "1",
                 "--config", "config/pipeline.yaml"]) == 0


def test_unexpected_bug_is_not_swallowed_but_run_is_still_recorded(config, monkeypatch):
    _seed(config)

    def boom(*args, **kwargs):
        raise KeyError("programming bug")

    monkeypatch.setattr(pipeline, "ingest_customers", boom)
    with pytest.raises(KeyError):
        run_one_date(DATE, config)
    runs = _runs(config)
    assert len(runs) == 1 and runs[0]["status"] == "FAIL"


def test_quarantine_count_finds_files_in_nested_dt_folders(tmp_path):
    """Works for today's layout AND the spec layout quarantine/<src>/dt=<date>/*.parquet."""
    import time
    import pandas as pd
    from src.pipeline import _quarantined_since
    flat = tmp_path / "orders"
    nested = tmp_path / "orders" / "dt=2025-11-09"
    nested.mkdir(parents=True)
    before = time.time() - 1
    pd.DataFrame({"a": [1, 2]}).to_parquet(flat / "20251109T000000.parquet")
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(nested / "orders_bad.parquet")
    assert _quarantined_since(tmp_path, "orders", before) == 5
    assert _quarantined_since(tmp_path, "orders", time.time() + 100) == 0
    assert _quarantined_since(tmp_path, "customers", before) == 0