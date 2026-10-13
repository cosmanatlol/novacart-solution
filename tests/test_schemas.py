"""Unit tests for Pydantic schema contracts."""
from __future__ import annotations
from datetime import date
import pytest
from pydantic import ValidationError
from src.utils.schemas import OrderRow, CustomerRow, ProductRow


class TestOrderRow:
    def _valid(self, **overrides):
        base = dict(order_id="ORD-1", customer_id="C1", product_id="P1",
                    order_date=date(2025, 1, 1), quantity=2,
                    unit_price=9.99, status="shipped")
        base.update(overrides)
        return base

    def test_valid_order(self):
        row = OrderRow(**self._valid())
        assert row.order_id == "ORD-1"

    def test_status_normalised_to_lowercase(self):
        row = OrderRow(**self._valid(status="Shipped"))
        assert row.status == "shipped"

    def test_zero_quantity_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(quantity=0))

    def test_negative_quantity_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(quantity=-1))

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(unit_price=-0.01))

    def test_invalid_status_rejected(self):
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(status="refunded"))


class TestCustomerRow:
    def _valid(self, **overrides):
        base = dict(customer_id="C1", first_name="Alice", last_name="Smith",
                    email="alice@example.com", address="{'city': 'New York', 'country': 'US'}",
                    signup_date=date(2024, 1, 1), tier="gold")
        base.update(overrides)
        return base

    def test_valid_customer(self):
        row = CustomerRow(**self._valid())
        assert row.customer_id == "C1"

    def test_email_normalised_to_lowercase(self):
        row = CustomerRow(**self._valid(email="Alice@Example.COM"))
        print(row.email)
        assert row.email == "alice@example.com"

    def test_bad_email_rejected(self):
        with pytest.raises(ValidationError):
            CustomerRow(**self._valid(email="not-an-email"))

    def test_tier_defaults_to_standard(self):
        data = self._valid()
        data.pop("tier")
        row = CustomerRow(**data)
        assert row.tier == "standard"


class TestProductRow:
    def _valid(self, **overrides):
        base = dict(product_id="P1", name="Widget", category="Electronics",
                    unit_cost=10.0, supplier_id="SUP-A",
                    updated_at="2025-01-01T00:00:00")
        base.update(overrides)
        return base

    def test_valid_product(self):
        row = ProductRow(**self._valid())
        assert row.product_id == "P1"

    def test_negative_cost_rejected(self):
        with pytest.raises(ValidationError):
            ProductRow(**self._valid(unit_cost=-1.0))

    def test_zero_cost_allowed(self):
        row = ProductRow(**self._valid(unit_cost=0.0))
        assert row.unit_cost == 0.0
