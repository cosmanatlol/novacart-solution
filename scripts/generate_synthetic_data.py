"""
Synthetic data generator for the NovaCart pipeline.

Generates synthetic orders (CSV), customers (JSON), and products (SQLite DB)
with configurable record counts and percentage of malformed/invalid records
to test validation, deduplication, quarantine, and error-handling pathways.

Usage:
    python scripts/generate_synthetic_data.py
    python scripts/generate_synthetic_data.py --malformed-pct 0.2
    python scripts/generate_synthetic_data.py --malformed-pct 15 --seed 42
    python scripts/generate_synthetic_data.py --clean
    python scripts/generate_synthetic_data.py --help
    python scripts/generate_synthetic_data.py --malformed-pct 0.2 --num-customers 10000 --num-products 500 --orders-per-day 50000
    To test:
    python -m src.pipeline --date 2025-11-10 --backfill 3
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent

# ── Sample Data Pools ───────────────────────────────────────────────────────────

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Heidi",
    "Ivan", "Judy", "Mallory", "Oscar", "Peggy", "Trent", "Victor",
    "Walter", "Arthur", "Beryl", "Clara", "David", "Elena", "Felix",
    "George", "Hannah", "Iris", "Jack", "Karen", "Leo", "Maya", "Noah",
]

LAST_NAMES = [
    "Smith", "Jones", "White", "Brown", "Davis", "Miller", "Wilson",
    "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "Harris", "Clark",
    "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright",
    "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
]

DOMAINS = ["example.com", "novamail.com", "testcorp.org", "cartmail.net", "inbox.io"]

CITIES_AND_COUNTRIES = [
    ("New York", "US"),
    ("London", "GB"),
    ("Toronto", "CA"),
    ("Sydney", "AU"),
    ("Paris", "FR"),
    ("Berlin", "DE"),
    ("Tokyo", "JP"),
    ("Singapore", "SG"),
    ("Amsterdam", "NL"),
    ("San Francisco", "US"),
    ("Dublin", "IE"),
    ("Stockholm", "SE"),
]

TIERS = ["standard", "silver", "gold", "platinum"]

PRODUCT_TEMPLATES = [
    ("Wireless Headphones", "Electronics", 22.50, 49.99),
    ("Laptop Stand", "Accessories", 8.75, 29.99),
    ("USB-C Hub", "Electronics", 12.00, 39.99),
    ("Webcam HD", "Electronics", 35.00, 79.99),
    ("Mechanical Keyboard", "Accessories", 45.00, 119.00),
    ("Ergonomic Mouse", "Accessories", 18.50, 49.50),
    ("Noise-Cancelling Earbuds", "Electronics", 40.00, 99.00),
    ("Monitor Arm", "Office", 28.00, 69.00),
    ("Desk Mat", "Office", 6.50, 19.99),
    ("LED Desk Lamp", "Home", 14.00, 34.99),
    ("Portable SSD 1TB", "Electronics", 55.00, 129.99),
    ("Cable Management Kit", "Accessories", 4.20, 14.50),
    ("Smart Water Bottle", "Home", 11.00, 28.00),
    ("Coffee Mug Warmer", "Home", 9.80, 24.99),
    ("Backpack Pro", "Apparel", 32.00, 89.00),
]

SUPPLIERS = ["SUP-A", "SUP-B", "SUP-C", "SUP-D", "SUP-E"]

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled", "returned"]
INVALID_STATUSES = ["refunded", "disputed", "unknown", "failed", "processing", "chargeback"]


# ── Customer Generation ────────────────────────────────────────────────────────

def generate_customers(
    num_customers: int = 20,
    malformed_pct: float = 0.1,
    rng: Optional[random.Random] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Generate synthetic customer records.
    Returns (customer_list, stats_dict).
    """
    if rng is None:
        rng = random.Random()

    customers: List[Dict[str, Any]] = []
    stats = {"total": num_customers, "clean": 0, "malformed": 0}

    for i in range(1, num_customers + 1):
        cust_id = f"CUST-{i:03d}"
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        domain = rng.choice(DOMAINS)
        city, country = rng.choice(CITIES_AND_COUNTRIES)
        tier = rng.choice(TIERS)

        # Generate a random signup date between 2023-01-01 and 2025-01-01
        days_offset = rng.randint(0, 730)
        signup_dt = date(2023, 1, 1) + timedelta(days=days_offset)
        signup_date_str = signup_dt.isoformat()

        email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
        address: Any = {"city": city, "country": country}

        is_malformed = rng.random() < malformed_pct

        if is_malformed:
            malform_kind = rng.choice([
                "bad_email_no_at",
                "bad_email_empty",
                "bad_address_missing_city",
                "bad_address_missing_country",
                "bad_address_empty_city",
                "bad_address_empty_country",
            ])

            if malform_kind == "bad_email_no_at":
                email = f"{first_name.lower()}.{last_name.lower()}-at-{domain}"
            elif malform_kind == "bad_email_empty":
                email = ""
            elif malform_kind == "bad_address_missing_city":
                address = {"country": country}
            elif malform_kind == "bad_address_missing_country":
                address = {"city": city}
            elif malform_kind == "bad_address_empty_city":
                address = {"city": "", "country": country}
            elif malform_kind == "bad_address_empty_country":
                address = {"city": city, "country": ""}

            stats["malformed"] += 1
        else:
            stats["clean"] += 1

        customers.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "address": address,
            "signup_date": signup_date_str,
            "tier": tier,
        })

    return customers, stats


# ── Product Generation ─────────────────────────────────────────────────────────

def generate_products(
    num_products: int = 10,
    malformed_pct: float = 0.1,
    rng: Optional[random.Random] = None,
) -> Tuple[List[Tuple[str, str, str, float, str, str]], Dict[str, int]]:
    """
    Generate synthetic product records for SQLite DB insertion.
    Returns (product_tuples, stats_dict).
    """
    if rng is None:
        rng = random.Random()

    products: List[Tuple[str, str, str, float, str, str]] = []
    stats = {"total": num_products, "clean": 0, "malformed": 0}

    for i in range(1, num_products + 1):
        prod_id = f"PROD-{i:03d}"
        template = PRODUCT_TEMPLATES[(i - 1) % len(PRODUCT_TEMPLATES)]
        name, category, default_cost, _ = template
        supplier = rng.choice(SUPPLIERS)
        unit_cost = round(default_cost * rng.uniform(0.9, 1.2), 2)

        # Timestamp in late 2025
        hour = rng.randint(8, 18)
        minute = rng.randint(0, 59)
        day = rng.randint(1, 28)
        updated_at = f"2025-10-{day:02d}T{hour:02d}:{minute:02d}:00"

        is_malformed = rng.random() < malformed_pct

        if is_malformed:
            malform_kind = rng.choice([
                "negative_cost",
            ])
            if malform_kind == "negative_cost":
                unit_cost = -abs(unit_cost if unit_cost > 0 else 10.0)

            stats["malformed"] += 1
        else:
            stats["clean"] += 1

        products.append((prod_id, name, category, unit_cost, supplier, updated_at))

    return products, stats


# ── Order Generation ───────────────────────────────────────────────────────────

def generate_orders(
    dates: List[str],
    orders_per_day: int = 25,
    customer_ids: Optional[List[str]] = None,
    product_ids: Optional[List[str]] = None,
    malformed_pct: float = 0.1,
    rng: Optional[random.Random] = None,
) -> Tuple[Dict[str, List[List[str]]], Dict[str, int]]:
    """
    Generate synthetic orders grouped by date for CSV generation.
    Returns (orders_by_date, stats_dict).
    """
    if rng is None:
        rng = random.Random()

    if not customer_ids:
        customer_ids = [f"CUST-{i:03d}" for i in range(1, 21)]
    if not product_ids:
        product_ids = [f"PROD-{i:03d}" for i in range(1, 11)]

    # Product price mapping estimate
    price_map: Dict[str, float] = {}
    for i, pid in enumerate(product_ids):
        _, _, _, default_price = PRODUCT_TEMPLATES[i % len(PRODUCT_TEMPLATES)]
        price_map[pid] = default_price

    orders_by_date: Dict[str, List[List[str]]] = {}
    stats = {"total": 0, "clean": 0, "malformed": 0, "duplicate": 0}
    global_order_counter = 1

    headers = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"]

    for date_str in dates:
        rows: List[List[str]] = [headers]
        existing_order_ids_today: List[str] = []

        for _ in range(orders_per_day):
            order_id = f"ORD-{global_order_counter:04d}"
            global_order_counter += 1

            cust_id = rng.choice(customer_ids)
            prod_id = rng.choice(product_ids)
            qty = rng.randint(1, 6)
            base_price = price_map.get(prod_id, 29.99)
            unit_price = round(base_price * rng.uniform(0.95, 1.05), 2)
            status = rng.choice(ORDER_STATUSES)

            is_malformed = rng.random() < malformed_pct

            if is_malformed:
                malform_kind = rng.choice([
                    "zero_quantity",
                    "negative_quantity",
                    "negative_price",
                    "invalid_status",
                    "duplicate_order_id",
                ])

                if malform_kind == "zero_quantity":
                    qty = 0
                    stats["malformed"] += 1
                elif malform_kind == "negative_quantity":
                    qty = -rng.randint(1, 5)
                    stats["malformed"] += 1
                elif malform_kind == "negative_price":
                    unit_price = -abs(unit_price)
                    stats["malformed"] += 1
                elif malform_kind == "invalid_status":
                    status = rng.choice(INVALID_STATUSES)
                    stats["malformed"] += 1
                elif malform_kind == "duplicate_order_id" and existing_order_ids_today:
                    order_id = rng.choice(existing_order_ids_today)
                    stats["duplicate"] += 1
                else:
                    qty = 0
                    stats["malformed"] += 1
            else:
                stats["clean"] += 1

            existing_order_ids_today.append(order_id)
            stats["total"] += 1

            row = [
                order_id,
                cust_id,
                prod_id,
                date_str,
                str(qty),
                f"{unit_price:.2f}",
                status,
            ]
            rows.append(row)

        orders_by_date[date_str] = rows

    return orders_by_date, stats


# ── Full Synthetic Dataset Generation ─────────────────────────────────────────

def generate_all_synthetic_data(
    output_dir: Path,
    malformed_pct: float = 0.1,
    num_customers: int = 20,
    num_products: int = 10,
    orders_per_day: int = 25,
    dates: Optional[List[str]] = None,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Generate complete synthetic dataset (orders, customers, products) and write to disk.
    """
    if dates is None:
        dates = ["2025-11-07", "2025-11-08", "2025-11-09", "2025-11-10"]

    rng = random.Random(seed) if seed is not None else random.Random()

    orders_dir = output_dir / "orders"
    customer_dir = output_dir / "customers"
    db_path = output_dir / "products.db"

    orders_dir.mkdir(parents=True, exist_ok=True)
    customer_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. Customers
    customers, cust_stats = generate_customers(
        num_customers=num_customers,
        malformed_pct=malformed_pct,
        rng=rng,
    )
    cust_path = customer_dir / "customers.json"
    cust_path.write_text(json.dumps(customers, indent=2), encoding="utf-8")

    # 2. Products
    products, prod_stats = generate_products(
        num_products=num_products,
        malformed_pct=malformed_pct,
        rng=rng,
    )
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute("""
        CREATE TABLE products (
            product_id  TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            category    TEXT NOT NULL,
            unit_cost   REAL NOT NULL,
            supplier_id TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
    """)
    conn.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", products)
    conn.commit()
    conn.close()

    # 3. Orders
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p[0] for p in products]

    orders_by_date, order_stats = generate_orders(
        dates=dates,
        orders_per_day=orders_per_day,
        customer_ids=customer_ids,
        product_ids=product_ids,
        malformed_pct=malformed_pct,
        rng=rng,
    )

    written_order_files = []
    for date_str, rows in orders_by_date.items():
        path = orders_dir / f"orders_{date_str}.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        written_order_files.append(path)

    return {
        "output_dir": output_dir,
        "customers_path": cust_path,
        "products_db_path": db_path,
        "orders_files": written_order_files,
        "stats": {
            "customers": cust_stats,
            "products": prod_stats,
            "orders": order_stats,
            "malformed_pct": malformed_pct,
        },
    }


# ── CLI Entry Point ────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for NovaCart with configurable malformation rate."
    )
    parser.add_argument(
        "--malformed-pct", "-m",
        type=float,
        default=0.10,
        help="Fraction (0.0 to 1.0) or percentage (0 to 100) of records to intentionally malform. Default: 0.10 (10%%)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Generate 100%% clean data (equivalent to --malformed-pct 0.0)",
    )
    parser.add_argument(
        "--num-customers", "-c",
        type=int,
        default=20,
        help="Number of synthetic customer records to generate. Default: 20",
    )
    parser.add_argument(
        "--num-products", "-p",
        type=int,
        default=10,
        help="Number of synthetic product records to generate. Default: 10",
    )
    parser.add_argument(
        "--orders-per-day", "-o",
        type=int,
        default=25,
        help="Number of orders to generate per date file. Default: 25",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        default=["2025-11-07", "2025-11-08", "2025-11-09", "2025-11-10"],
        help="List of dates (YYYY-MM-DD) for order generation. Default: 2025-11-07 2025-11-08 2025-11-09 2025-11-10",
    )
    parser.add_argument(
        "--output-dir", "-d",
        type=str,
        default=str(ROOT / "data" / "landing"),
        help="Destination directory for landing files. Default: data/landing",
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=None,
        help="Random seed for deterministic/reproducible data generation.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    pct = 0.0 if args.clean else args.malformed_pct
    if pct > 1.0:
        pct = pct / 100.0  # normalize e.g. 15 -> 0.15

    if not 0.0 <= pct <= 1.0:
        raise ValueError(f"Malformed percentage must be between 0.0 and 1.0 (or 0 and 100), got {args.malformed_pct}")

    out_dir = Path(args.output_dir)

    print(f"Generating synthetic data (malformed rate: {pct * 100:.1f}%, seed: {args.seed})...")
    result = generate_all_synthetic_data(
        output_dir=out_dir,
        malformed_pct=pct,
        num_customers=args.num_customers,
        num_products=args.num_products,
        orders_per_day=args.orders_per_day,
        dates=args.dates,
        seed=args.seed,
    )

    stats = result["stats"]
    print(f"\nGeneration complete!")
    print(f"Output directory: {result['output_dir']}")
    print(f"  Customers : {stats['customers']['total']} records ({stats['customers']['malformed']} malformed) -> {result['customers_path']}")
    print(f"  Products  : {stats['products']['total']} records ({stats['products']['malformed']} malformed) -> {result['products_db_path']}")
    print(f"  Orders    : {stats['orders']['total']} records across {len(result['orders_files'])} dates ({stats['orders']['malformed']} malformed, {stats['orders']['duplicate']} duplicates)")
    for p in result["orders_files"]:
        print(f"              -> {p}")


if __name__ == "__main__":
    main()
