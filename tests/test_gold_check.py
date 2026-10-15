"""Gold layer tests: FR-4.1 .. FR-4.4 and Gold idempotency (FR-1)."""
import logging
from pathlib import Path

import pandas as pd
import pytest

from src.utils.config import Config

from src.transform.gold import (
    build_dim_customer, build_dim_date, build_dim_product, build_fact_orders,
)

LOG = logging.getLogger("test_gold")
SCD2 = ["first_name", "last_name", "email", "city", "country", "tier"]


def _w(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _customers(city="Pune"):
    return pd.DataFrame([
        {"customer_id": "1", "first_name": "Asha", "last_name": "R", "email": "a@x.com",
         "signup_date": "2025-01-01", "tier": "gold", "city": city, "country": "IN",
         "_ingested_at": "t", "_row_hash": "h"},
        {"customer_id": "2", "first_name": "Bob", "last_name": "K", "email": "b@x.com",
         "signup_date": "2025-02-01", "tier": "silver", "city": "Austin", "country": "US",
         "_ingested_at": "t", "_row_hash": "h"},
    ])


def _orders(d="2025-11-07", n=3):
    return pd.DataFrame({
        "order_id": [f"o{i}" for i in range(n)], "customer_id": ["1"] * n,
        "product_id": ["p1"] * n, "order_date": [d] * n,
        "quantity": [2] * n, "unit_price": [10.5] * n, "status": ["shipped"] * n,
        "_ingested_at": ["t"] * n, "_partition_date": [d] * n,
    })


def _products(price="100"):
    return pd.DataFrame([
        {"product_id": "p1", "name": "Mug", "category": "home", "unit_cost": price,
         "supplier_id": "s1", "updated_at": "2025-11-01", "_ingested_at": "t"},
    ])


@pytest.fixture
def dirs(tmp_path):
    return tmp_path / "silver", tmp_path / "gold"


# FR-4.1
def test_dim_date_covers_2024_2026(dirs):
    _, gold = dirs
    df = pd.read_parquet(build_dim_date(gold, LOG))
    assert df["date"].min() == pd.Timestamp("2024-01-01")
    assert df["date"].max() == pd.Timestamp("2026-12-31")
    assert len(df) == 366 + 365 + 365
    assert df["date_key"].is_unique
    assert df.loc[df["date_key"] == 20251107, "day_name"].iloc[0] == "Friday"
    assert bool(df.loc[df["date_key"] == 20251108, "is_weekend"].iloc[0])


# FR-4.2
def test_dim_product_scd1_overwrites_and_keeps_others(dirs):
    silver, gold = dirs
    _w(silver / "products" / "data.parquet", _products("100"))
    build_dim_product(silver, gold, LOG)
    two = pd.concat([_products("120"), pd.DataFrame([{
        "product_id": "p2", "name": "Cup", "category": "home", "unit_cost": "5",
        "supplier_id": "s1", "updated_at": "2025-11-02", "_ingested_at": "t"}])])
    _w(silver / "products" / "data.parquet", two)
    df = pd.read_parquet(build_dim_product(silver, gold, LOG))
    assert len(df) == 2
    assert df.loc[df.product_id == "p1", "unit_cost"].iloc[0] == "120"
    # next incremental batch holds only p2 -> p1 must survive
    _w(silver / "products" / "data.parquet", two[two.product_id == "p2"])
    df = pd.read_parquet(build_dim_product(silver, gold, LOG))
    assert set(df.product_id) == {"p1", "p2"}


# FR-4.3
def test_dim_customer_scd2_history(dirs):
    silver, gold = dirs
    _w(silver / "customers" / "data.parquet", _customers("Pune"))
    build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-01-01")
    _w(silver / "customers" / "data.parquet", _customers("Mumbai"))
    df = pd.read_parquet(build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-06-01"))
    a = df[df.customer_id == "1"].sort_values("_eff_start")
    assert list(a.city) == ["Pune", "Mumbai"]
    assert list(a._current) == [False, True]
    assert a.iloc[0]._eff_end == "2025-06-01" and a.iloc[1]._eff_end == "9999-12-31"
    assert len(df[df.customer_id == "2"]) == 1          # unchanged customer untouched
    assert df[df._current].customer_id.is_unique


def test_dim_customer_new_customer_added(dirs):
    silver, gold = dirs
    _w(silver / "customers" / "data.parquet", _customers().iloc[:1])
    build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-01-01")
    _w(silver / "customers" / "data.parquet", _customers())
    df = pd.read_parquet(build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-01-02"))
    assert set(df.customer_id) == {"1", "2"} and len(df) == 2


# FR-4.4
def test_fact_orders_totals_and_load_date(dirs):
    silver, gold = dirs
    _w(silver / "orders" / "date=2025-11-07" / "data.parquet", _orders())
    df = pd.read_parquet(build_fact_orders("2025-11-07", silver, gold, LOG))
    assert (df.total_amount == 21.0).all()
    assert (df._load_date == "2025-11-07").all()
    assert (df.date_key == 20251107).all()


def test_fact_orders_rerun_replaces_partition_not_appends(dirs):
    silver, gold = dirs
    p = silver / "orders" / "date=2025-11-07" / "data.parquet"
    _w(p, _orders(n=3))
    build_fact_orders("2025-11-07", silver, gold, LOG)
    build_fact_orders("2025-11-07", silver, gold, LOG)
    assert len(pd.read_parquet(gold / "fact_orders" / "date=2025-11-07")) == 3
    _w(p, _orders(n=2))                                   # corrected data replaces old
    build_fact_orders("2025-11-07", silver, gold, LOG)
    assert len(pd.read_parquet(gold / "fact_orders" / "date=2025-11-07")) == 2


def test_fact_orders_other_dates_untouched(dirs):
    silver, gold = dirs
    for d in ("2025-11-07", "2025-11-08"):
        _w(silver / "orders" / f"date={d}" / "data.parquet", _orders(d))
        build_fact_orders(d, silver, gold, LOG)
    build_fact_orders("2025-11-07", silver, gold, LOG)
    assert len(pd.read_parquet(gold / "fact_orders" / "date=2025-11-08")) == 3


# FR-1 Gold idempotency
def test_gold_is_idempotent_end_to_end(dirs):
    silver, gold = dirs
    _w(silver / "products" / "data.parquet", _products())
    _w(silver / "customers" / "data.parquet", _customers())
    _w(silver / "orders" / "date=2025-11-07" / "data.parquet", _orders())

    def run():
        build_dim_date(gold, LOG)
        build_dim_product(silver, gold, LOG)
        build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-11-07")
        build_fact_orders("2025-11-07", silver, gold, LOG)
        return {
            "dim_date": pd.read_parquet(gold / "dim_date.parquet"),
            "dim_product": pd.read_parquet(gold / "dim_product.parquet"),
            "dim_customer": pd.read_parquet(gold / "dim_customer.parquet"),
            "fact": pd.read_parquet(gold / "fact_orders" / "date=2025-11-07" / "data.parquet"),
        }

    first, second = run(), run()
    for k in first:
        pd.testing.assert_frame_equal(first[k], second[k], obj=k)


def test_missing_silver_is_skipped_gracefully(dirs):
    silver, gold = dirs
    build_dim_product(silver, gold, LOG)
    build_dim_customer(silver, gold, SCD2, LOG)
    build_fact_orders("2025-11-07", silver, gold, LOG)
    assert not (gold / "dim_product.parquet").exists()


# ── SCD2 address-change scenarios ────────────────────────────────────────────
def _load(silver, gold, rows, as_of):
    base = {"first_name": "Asha", "last_name": "R", "email": "a@x.com",
            "signup_date": "2025-01-01", "tier": "gold", "city": "Pune", "country": "IN"}
    df = pd.DataFrame([{**base, **r, "_ingested_at": "t"} for r in rows])
    _w(silver / "customers" / "data.parquet", df)
    build_dim_customer(silver, gold, SCD2, LOG, as_of=as_of)
    return pd.read_parquet(gold / "dim_customer.parquet")


def test_move_and_move_back_keeps_full_history(dirs):
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")
    _load(silver, gold, [{"customer_id": "1", "city": "Mumbai"}], "2025-03-01")
    df = _load(silver, gold, [{"customer_id": "1", "city": "Pune"}], "2025-06-01")
    assert list(df.city) == ["Pune", "Mumbai", "Pune"]
    assert list(df._eff_start) == ["2025-01-01", "2025-03-01", "2025-06-01"]
    # each row closes exactly when the next one opens, last one stays open
    assert list(df._eff_end) == ["2025-03-01", "2025-06-01", "9999-12-31"]
    assert list(df._current) == [False, False, True]


def test_same_day_correction_leaves_no_zero_length_row(dirs):
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")
    _load(silver, gold, [{"customer_id": "1", "city": "Mumbai"}], "2025-03-01")
    df = _load(silver, gold, [{"customer_id": "1", "city": "Delhi"}], "2025-03-01")
    assert list(df.city) == ["Pune", "Delhi"]
    assert (df._eff_start < df._eff_end).all()


def test_untracked_field_refreshes_current_row_without_new_history(dirs):
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")
    df = _load(silver, gold, [{"customer_id": "1", "signup_date": "2024-12-31"}], "2025-03-01")
    assert len(df) == 1 and df.iloc[0].signup_date == "2024-12-31"


def test_exactly_one_current_row_per_customer_after_many_changes(dirs):
    silver, gold = dirs
    cities = ["Pune", "Mumbai", "Delhi", "Pune", "Goa"]
    for i, c in enumerate(cities):
        df = _load(silver, gold,
                   [{"customer_id": "1", "city": c}, {"customer_id": "2", "city": "Austin"}],
                   f"2025-0{i + 1}-01")
    assert (df[df._current].groupby("customer_id").size() == 1).all()
    assert len(df[df.customer_id == "1"]) == 5
    assert len(df[df.customer_id == "2"]) == 1          # never-moving customer: one row


def test_point_in_time_join_gives_city_at_order_time(dirs):
    """The reason SCD2 exists: orders can be reported by the city the customer lived in."""
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")
    dim = _load(silver, gold, [{"customer_id": "1", "city": "Mumbai"}], "2025-03-01")
    orders = pd.DataFrame({"customer_id": ["1", "1"], "order_date": ["2025-02-10", "2025-04-10"]})
    m = orders.merge(dim, on="customer_id")
    m = m[(m.order_date >= m._eff_start) & (m.order_date < m._eff_end)]
    assert list(m.sort_values("order_date").city) == ["Pune", "Mumbai"]


# ── Primary keys come from config (teammate's sources_cfg), not from the code ──
CFG_CUSTOM = Config(
    {"sources": {"products": {"primary_key": "sku"},
                 "customers": {"primary_key": "cust_no"},
                 "orders": {"primary_key": "ord_ref"}}},
    Path("."),
)


def test_custom_primary_keys_are_honoured(dirs):
    silver, gold = dirs
    prod = _products("100").rename(columns={"product_id": "sku"})
    _w(silver / "products" / "data.parquet", pd.concat([prod, prod.assign(unit_cost="120")]))
    out = pd.read_parquet(build_dim_product(silver, gold, LOG, config=CFG_CUSTOM))
    assert len(out) == 1 and out.iloc[0].unit_cost == "120"

    cust = _customers().rename(columns={"customer_id": "cust_no"})
    _w(silver / "customers" / "data.parquet", cust)
    build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-01-01", config=CFG_CUSTOM)
    _w(silver / "customers" / "data.parquet", cust.assign(city="Mumbai"))
    dim = pd.read_parquet(build_dim_customer(silver, gold, SCD2, LOG, as_of="2025-06-01",
                                             config=CFG_CUSTOM))
    assert len(dim) == 4 and int(dim._current.sum()) == 2          # both customers moved

    ords = _orders().rename(columns={"order_id": "ord_ref"})
    _w(silver / "orders" / "date=2025-11-07" / "data.parquet", pd.concat([ords, ords]))
    fact = pd.read_parquet(build_fact_orders("2025-11-07", silver, gold, LOG, config=CFG_CUSTOM))
    assert len(fact) == 3