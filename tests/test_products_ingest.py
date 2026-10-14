"""Tests for products ingestion and watermark management in src/ingest/products.py."""
from __future__ import annotations
import logging
import sqlite3
from pathlib import Path
import pandas as pd
import pytest

from tests.conftest import make_products_db
from src.ingest.products import ingest_products, WATERMARK_KEY
from src.utils.config import Config
from src.utils.exceptions import IngestionError, SchemaError
from src.utils.logging_setup import get_logger
from src.utils.state import StateManager


@pytest.fixture()
def state(config: Config) -> StateManager:
    return StateManager(config.state)


@pytest.fixture()
def logger(config: Config) -> logging.Logger:
    return get_logger("test_products_ingest", config.logs)


# 1. First run loads all rows and sets the watermark (key products_updated_at) to max updated_at.
def test_first_run_loads_all_rows_and_sets_watermark(config: Config, state: StateManager, logger: logging.Logger):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00"),
        ("PROD-002", "Gadget", "Electronics", 20.0, "SUP-B", "2025-01-02T12:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)

    out_path = ingest_products(config.landing_products_db, config.bronze, state, logger)
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert len(df) == 2
    assert state.get_watermark(WATERMARK_KEY) == "2025-01-02T12:00:00"


# 2. Rerun with no changes loads nothing, leaves watermark and Bronze file unchanged.
def test_rerun_with_no_changes_leaves_watermark_and_bronze_unchanged(config: Config, state: StateManager, logger: logging.Logger):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)

    out_path = ingest_products(config.landing_products_db, config.bronze, state, logger)
    initial_bytes = out_path.read_bytes()
    initial_mtime = out_path.stat().st_mtime_ns
    initial_watermark = state.get_watermark(WATERMARK_KEY)

    # Second run without changes
    ingest_products(config.landing_products_db, config.bronze, state, logger)

    assert state.get_watermark(WATERMARK_KEY) == initial_watermark
    assert out_path.read_bytes() == initial_bytes
    assert out_path.stat().st_mtime_ns == initial_mtime


# 3. A row with a newer updated_at is loaded alone and the watermark advances.
def test_newer_row_loaded_and_watermark_advances(config: Config, state: StateManager, logger: logging.Logger):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)
    ingest_products(config.landing_products_db, config.bronze, state, logger)
    assert state.get_watermark(WATERMARK_KEY) == "2025-01-01T00:00:00"

    # Add newer row
    conn = sqlite3.connect(config.landing_products_db)
    conn.execute(
        "INSERT INTO products VALUES (?,?,?,?,?,?)",
        ("PROD-002", "Gadget", "Electronics", 25.0, "SUP-B", "2025-01-05T10:00:00"),
    )
    conn.commit()
    conn.close()

    out_path = ingest_products(config.landing_products_db, config.bronze, state, logger)
    df = pd.read_parquet(out_path)
    assert len(df) == 1
    assert df.iloc[0]["product_id"] == "PROD-002"
    assert state.get_watermark(WATERMARK_KEY) == "2025-01-05T10:00:00"


# 4. A row with an older updated_at is not loaded.
def test_older_row_not_loaded(config: Config, state: StateManager, logger: logging.Logger):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-05T00:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)
    out_path = ingest_products(config.landing_products_db, config.bronze, state, logger)
    initial_bytes = out_path.read_bytes()

    # Add older row
    conn = sqlite3.connect(config.landing_products_db)
    conn.execute(
        "INSERT INTO products VALUES (?,?,?,?,?,?)",
        ("PROD-002", "OldWidget", "Electronics", 5.0, "SUP-A", "2025-01-01T00:00:00"),
    )
    conn.commit()
    conn.close()

    ingest_products(config.landing_products_db, config.bronze, state, logger)
    assert state.get_watermark(WATERMARK_KEY) == "2025-01-05T00:00:00"
    assert out_path.read_bytes() == initial_bytes


# 5. Missing DB file raises IngestionError and the watermark is unchanged.
def test_missing_db_file_raises_ingestion_error(config: Config, state: StateManager, logger: logging.Logger):
    state.set_watermark(WATERMARK_KEY, "2025-01-01T00:00:00")
    if config.landing_products_db.exists():
        config.landing_products_db.unlink()

    with pytest.raises(IngestionError, match="products DB not found"):
        ingest_products(config.landing_products_db, config.bronze, state, logger)

    assert state.get_watermark(WATERMARK_KEY) == "2025-01-01T00:00:00"


# 6. A simulated failure during the Parquet write leaves the watermark unchanged.
def test_parquet_write_failure_leaves_watermark_unchanged(
    config: Config, state: StateManager, logger: logging.Logger, monkeypatch: pytest.MonkeyPatch
):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)
    state.set_watermark(WATERMARK_KEY, "2024-01-01T00:00:00")

    def mock_to_parquet(*args, **kwargs):
        raise OSError("Disk full simulation")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", mock_to_parquet)

    with pytest.raises(OSError, match="Disk full simulation"):
        ingest_products(config.landing_products_db, config.bronze, state, logger)

    assert state.get_watermark(WATERMARK_KEY) == "2024-01-01T00:00:00"


# 7. Missing required column raises SchemaError, including on an empty delta.
def test_missing_required_column_raises_schema_error(config: Config, state: StateManager, logger: logging.Logger):
    # Missing supplier_id column
    conn = sqlite3.connect(config.landing_products_db)
    conn.execute("""
        CREATE TABLE products (
            product_id TEXT, name TEXT, category TEXT,
            unit_cost REAL, updated_at TEXT
        )
    """)
    conn.execute("INSERT INTO products VALUES ('PROD-001', 'Widget', 'Electronics', 10.0, '2025-01-01T00:00:00')")
    conn.commit()
    conn.close()

    with pytest.raises(SchemaError, match=r"\[schema_drift\] products: required columns missing"):
        ingest_products(config.landing_products_db, config.bronze, state, logger)


def test_missing_required_column_on_empty_delta_raises_schema_error(
    config: Config, state: StateManager, logger: logging.Logger
):
    # Set watermark ahead so delta is empty
    state.set_watermark(WATERMARK_KEY, "2025-02-01T00:00:00")
    conn = sqlite3.connect(config.landing_products_db)
    conn.execute("""
        CREATE TABLE products (
            product_id TEXT, name TEXT, category TEXT,
            unit_cost REAL, updated_at TEXT
        )
    """)
    conn.commit()
    conn.close()

    with pytest.raises(SchemaError, match=r"\[schema_drift\] products: required columns missing"):
        ingest_products(config.landing_products_db, config.bronze, state, logger)


# 8. Extra column logs a warning and the run succeeds.
def test_extra_column_logs_warning_and_run_succeeds(
    config: Config, state: StateManager, logger: logging.Logger, caplog: pytest.LogCaptureFixture
):
    conn = sqlite3.connect(config.landing_products_db)
    conn.execute("""
        CREATE TABLE products (
            product_id TEXT, name TEXT, category TEXT,
            unit_cost REAL, supplier_id TEXT, updated_at TEXT,
            is_active INTEGER
        )
    """)
    conn.execute(
        "INSERT INTO products VALUES ('PROD-001', 'Widget', 'Electronics', 10.0, 'SUP-A', '2025-01-01T00:00:00', 1)"
    )
    conn.commit()
    conn.close()

    with caplog.at_level(logging.WARNING):
        out_path = ingest_products(config.landing_products_db, config.bronze, state, logger)

    assert out_path.exists()
    assert any(
        "schema_drift_extra_columns" in record.message and "is_active" in record.message
        for record in caplog.records
    )


# 9. All Bronze columns are string dtype; _ingested_at, _source_file, _row_hash present.
def test_bronze_columns_all_string_and_metadata_present(config: Config, state: StateManager, logger: logging.Logger):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.5, "SUP-A", "2025-01-01T00:00:00"),
        ("PROD-002", "Gadget", None, None, "SUP-B", "2025-01-02T00:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)

    out_path = ingest_products(config.landing_products_db, config.bronze, state, logger)
    df = pd.read_parquet(out_path)

    for col in df.columns:
        assert pd.api.types.is_string_dtype(df[col]), f"Column {col} is not string dtype"

    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns
    assert "_row_hash" in df.columns
    assert (df["_source_file"] == "products.db").all()


# 10. _row_hash is stable for identical input across two separate Bronze writes.
def test_row_hash_stable_for_identical_input(config: Config, state: StateManager, logger: logging.Logger):
    rows = [
        ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00"),
    ]
    make_products_db(config.landing_products_db, rows)

    out_path1 = ingest_products(config.landing_products_db, config.bronze, state, logger)
    df1 = pd.read_parquet(out_path1)
    hash1 = df1.iloc[0]["_row_hash"]

    # Reset watermark and re-ingest same rows
    state.set_watermark(WATERMARK_KEY, "1970-01-01T00:00:00")
    out_path2 = ingest_products(config.landing_products_db, config.bronze, state, logger)
    df2 = pd.read_parquet(out_path2)
    hash2 = df2.iloc[0]["_row_hash"]

    assert hash1 == hash2


# 11. Missing products table raises IngestionError ("no such table").
def test_missing_products_table_raises_ingestion_error(config: Config, state: StateManager, logger: logging.Logger):
    conn = sqlite3.connect(config.landing_products_db)
    conn.execute("CREATE TABLE non_products (id TEXT)")
    conn.commit()
    conn.close()

    with pytest.raises(IngestionError, match="no such table"):
        ingest_products(config.landing_products_db, config.bronze, state, logger)
