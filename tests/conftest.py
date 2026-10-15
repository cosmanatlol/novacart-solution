"""Shared pytest fixtures for NovaCart pipeline tests."""
from __future__ import annotations
import csv
import json
import sqlite3
from pathlib import Path
import pytest
from src.utils.config import Config


@pytest.fixture()
def tmp_project(tmp_path: Path) -> Path:
    """Return a temp directory wired up as a minimal project root."""
    for d in [
        "config", "data/landing/orders", "data/landing/customers",
        "data/bronze", "data/silver", "data/gold",
        "data/quarantine", "logs", "state",
    ]:
        (tmp_path / d).mkdir(parents=True, exist_ok=True)

    cfg = {
        "pipeline": {"name": "test"},
        "paths": {
            "landing_orders":    "data/landing/orders",
            "landing_customers": "data/landing/customers",
            "landing_products_db": "data/landing/products.db",
            "bronze":     "data/bronze",
            "silver":     "data/silver",
            "gold":       "data/gold",
            "quarantine": "data/quarantine",
            "logs":       "logs",
            "state":      "state",
        },

        "sources": {
            "orders": {
                "primary_key": "order_id",
            },
            "customers": {
                "primary_key": "customer_id",
            },
            "products": {
                "primary_key": "product_id",
                "watermark_field": "updated_at",
            },
        },
        "silver": {"min_order_amount": 0.0, "max_order_amount": 100000.0},
        "gold":   {"scd2_track_fields": ["city", "country", "email"]},
    }
    import yaml
    (tmp_path / "config" / "pipeline.yaml").write_text(yaml.dump(cfg))
    return tmp_path


@pytest.fixture()
def config(tmp_project: Path) -> Config:
    import os
    old = os.getcwd()
    os.chdir(tmp_project)
    yield Config.load("config/pipeline.yaml")
    os.chdir(old)


def write_orders_csv(orders_dir: Path, date_str: str, rows: list[list]) -> Path:
    header = ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"]
    path = orders_dir / f"orders_{date_str}.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return path


def write_customers_json(customers_dir: Path, records: list[dict]) -> Path:
    path = customers_dir / "customers.json"
    path.write_text(json.dumps(records))
    return path


def make_products_db(db_path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute("""
        CREATE TABLE products (
            product_id TEXT, name TEXT, category TEXT,
            unit_cost REAL, supplier_id TEXT, updated_at TEXT
        )
    """)
    conn.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
