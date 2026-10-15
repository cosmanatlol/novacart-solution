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

# ── several versions of the SAME customer_id in one Silver batch ─────────────
def test_two_versions_in_one_batch_old_is_kept_outdated_new_is_current(dirs):
    silver, gold = dirs
    df = _load(silver, gold, [{"customer_id": "1", "city": "Pune"},
                              {"customer_id": "1", "city": "Mumbai"}], "2025-03-01")
    assert list(df.city) == ["Pune", "Mumbai"]            # older first, newest last
    assert list(df._current) == [False, True]
    assert list(df._eff_end) == ["2025-03-01", "9999-12-31"]


def test_batch_versions_after_existing_history(dirs):
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")                 # Pune
    df = _load(silver, gold, [{"customer_id": "1", "city": "Pune"},           # unchanged
                              {"customer_id": "1", "city": "Mumbai"},
                              {"customer_id": "1", "city": "Delhi"}], "2025-03-01")
    assert list(df.city) == ["Pune", "Mumbai", "Delhi"]
    assert list(df._current) == [False, False, True]
    assert int(df._current.sum()) == 1
    assert df.iloc[0]._eff_end == "2025-03-01"            # the Jan row was closed


def test_rerunning_a_batch_with_versions_is_idempotent(dirs):
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")
    rows = [{"customer_id": "1", "city": "Mumbai"}, {"customer_id": "1", "city": "Delhi"}]
    first = _load(silver, gold, rows, "2025-03-01")
    second = _load(silver, gold, rows, "2025-03-01")
    pd.testing.assert_frame_equal(first, second)


def test_identical_repeats_in_a_batch_do_not_create_rows(dirs):
    silver, gold = dirs
    df = _load(silver, gold, [{"customer_id": "1"}, {"customer_id": "1"}], "2025-01-01")
    assert len(df) == 1 and bool(df.iloc[0]._current)


# ── dim_product SCD1: price (unit_cost) overwrite and de-duplication ─────────
def _prod(pid, cost, updated, name="Mug"):
    return {"product_id": pid, "name": name, "category": "home", "unit_cost": cost,
            "supplier_id": "s1", "updated_at": updated, "_ingested_at": "t"}


def _put_products(silver, gold, rows):
    _w(silver / "products" / "data.parquet", pd.DataFrame(rows))
    return pd.read_parquet(build_dim_product(silver, gold, LOG))


def test_scd1_price_is_overwritten_and_no_history_row_is_added(dirs):
    silver, gold = dirs
    _put_products(silver, gold, [_prod("p1", 10.0, "2025-11-01"), _prod("p2", 5.0, "2025-11-01")])
    out = _put_products(silver, gold, [_prod("p1", 12.5, "2025-11-08")])   # only p1 changed
    assert len(out) == 2 and out.product_id.is_unique
    p1 = out[out.product_id == "p1"].iloc[0]
    assert p1.unit_cost == 12.5 and p1.updated_at == "2025-11-08"
    assert out[out.product_id == "p2"].iloc[0].unit_cost == 5.0            # untouched


def test_scd1_same_product_twice_in_one_batch_newest_updated_at_wins(dirs):
    silver, gold = dirs
    out = _put_products(silver, gold, [_prod("p1", 20.0, "2025-11-09"),    # newer, listed first
                                       _prod("p1", 10.0, "2025-11-01")])   # older, listed last
    assert len(out) == 1 and out.iloc[0].unit_cost == 20.0


def test_scd1_stale_record_does_not_overwrite_newer_gold_row(dirs):
    silver, gold = dirs
    _put_products(silver, gold, [_prod("p1", 50.0, "2025-11-05")])
    out = _put_products(silver, gold, [_prod("p1", 40.0, "2025-11-01")])   # older arrives late
    assert out.iloc[0].unit_cost == 50.0


def test_scd1_same_timestamp_later_record_overwrites(dirs):
    silver, gold = dirs
    _put_products(silver, gold, [_prod("p1", 10.0, "2025-11-05")])
    out = _put_products(silver, gold, [_prod("p1", 11.0, "2025-11-05")])
    assert out.iloc[0].unit_cost == 11.0


# ── whole-pipeline checks (real run_one_date, temp project from conftest) ────
def _seed_pipeline(config, day, products):
    from tests.conftest import make_products_db, write_customers_json, write_orders_csv
    make_products_db(config.landing_products_db, products)
    write_orders_csv(config.landing_orders, day, [
        [f"ORD-{day}-1", "CUST-001", "PROD-001", day, "2", "49.99", "shipped"],
        [f"ORD-{day}-2", "CUST-001", "PROD-002", day, "1", "10.00", "pending"]])
    write_customers_json(config.landing_customers, [{
        "customer_id": "CUST-001", "first_name": "Alice", "last_name": "Smith",
        "email": "alice@example.com", "address": {"city": "Pune", "country": "IN"},
        "signup_date": "2024-01-01", "tier": "gold"}])


def _gold_counts(config):
    gold = config.gold
    parts = sorted((gold / "fact_orders").glob("date=*/data.parquet"))
    return {
        "dim_date": len(pd.read_parquet(gold / "dim_date.parquet")),
        "dim_product": len(pd.read_parquet(gold / "dim_product.parquet")),
        "dim_customer": len(pd.read_parquet(gold / "dim_customer.parquet")),
        "fact_orders": sum(len(pd.read_parquet(p)) for p in parts),
    }


def test_pipeline_twice_for_same_date_gives_identical_row_counts_and_data(config):
    from src.pipeline import run_one_date
    day = "2025-11-07"
    P = lambda pid, cost: (pid, "Mug", "home", cost, "S1", "2025-11-01T00:00:00")
    _seed_pipeline(config, day, [P("PROD-001", 10.0), P("PROD-002", 5.0)])

    assert run_one_date(day, config)["status"] == "SUCCESS"
    counts_1 = _gold_counts(config)
    fact_1 = pd.read_parquet(config.gold / "fact_orders" / f"date={day}" / "data.parquet")
    cust_1 = pd.read_parquet(config.gold / "dim_customer.parquet")
    prod_1 = pd.read_parquet(config.gold / "dim_product.parquet")

    assert run_one_date(day, config)["status"] == "SUCCESS"      # same date, second time
    assert _gold_counts(config) == counts_1
    pd.testing.assert_frame_equal(
        pd.read_parquet(config.gold / "fact_orders" / f"date={day}" / "data.parquet"), fact_1)
    pd.testing.assert_frame_equal(pd.read_parquet(config.gold / "dim_customer.parquet"), cust_1)
    pd.testing.assert_frame_equal(pd.read_parquet(config.gold / "dim_product.parquet"), prod_1)
    assert counts_1 == {"dim_date": 1096, "dim_product": 2, "dim_customer": 1, "fact_orders": 2}


def test_pipeline_product_price_change_overwrites_in_gold(config):
    from src.pipeline import run_one_date
    P = lambda pid, cost, upd: (pid, "Mug", "home", cost, "S1", upd)
    _seed_pipeline(config, "2025-11-07", [P("PROD-001", 10.0, "2025-11-01T00:00:00"),
                                         P("PROD-002", 5.0, "2025-11-01T00:00:00")])
    assert run_one_date("2025-11-07", config)["status"] == "SUCCESS"

    _seed_pipeline(config, "2025-11-08", [P("PROD-001", 12.5, "2025-11-08T09:00:00"),
                                         P("PROD-002", 5.0, "2025-11-01T00:00:00")])
    assert run_one_date("2025-11-08", config)["status"] == "SUCCESS"

    dim = pd.read_parquet(config.gold / "dim_product.parquet").set_index("product_id")
    assert len(dim) == 2
    assert dim.loc["PROD-001", "unit_cost"] == 12.5          # price overwritten (SCD1)
    assert dim.loc["PROD-002", "unit_cost"] == 5.0           # unchanged product kept


# ── replaying OLDER dates must not disturb history ───────────────────────────
def test_replaying_an_older_date_after_newer_ones_changes_nothing(dirs):
    silver, gold = dirs
    _load(silver, gold, [{"customer_id": "1"}], "2025-01-01")                      # Pune
    _load(silver, gold, [{"customer_id": "1", "city": "Mumbai"}], "2025-03-01")
    before = pd.read_parquet(gold / "dim_customer.parquet")
    # replay January while the (newer) snapshot now says Mumbai
    after = _load(silver, gold, [{"customer_id": "1", "city": "Mumbai"}], "2025-01-01")
    pd.testing.assert_frame_equal(before, after)
    assert list(after.city) == ["Pune", "Mumbai"]


def test_replaying_every_date_of_a_backfill_is_a_no_op(dirs):
    silver, gold = dirs
    snapshot = [{"customer_id": "1", "city": "Mumbai"}, {"customer_id": "1", "city": "Delhi"},
                {"customer_id": "2", "city": "Goa"}]
    _load(silver, gold, [{"customer_id": "1"}, {"customer_id": "2"}], "2025-01-01")
    first = _load(silver, gold, snapshot, "2025-03-01")
    for d in ("2025-01-01", "2025-03-01", "2025-01-01", "2025-03-01"):          # oldest -> newest, twice
        again = _load(silver, gold, snapshot, d)
    pd.testing.assert_frame_equal(first, again)