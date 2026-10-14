"""Unit tests for check_schema with Pydantic model classes and legacy list[str] inputs."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock
import pytest

from src.transform.schema_check import check_schema
from src.utils.exceptions import SchemaError
from src.utils.schemas import CustomerRow, OrderRow, ProductRow, SOURCE_SCHEMAS


@pytest.fixture
def mock_logger() -> MagicMock:
    """Fixture providing a mock logger to inspect warning and info calls."""
    return MagicMock(spec=logging.Logger)


class TestCheckSchemaWithCustomerModel:
    """Tests specifically targeting CustomerRow optional vs. required fields."""

    def test_customers_without_optional_tier_passes(self, mock_logger: MagicMock):
        """1. Customers without a 'tier' column pass check_schema without raising or warning."""
        columns_without_tier = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "address",
            "signup_date",
        ]
        check_schema(columns_without_tier, CustomerRow, "customers", mock_logger)
        mock_logger.warning.assert_not_called()

    def test_customers_with_tier_present_does_not_warn(self, mock_logger: MagicMock):
        """2. 'tier' present is recognized as a known field and does not trigger extra-column warning."""
        all_columns = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "address",
            "signup_date",
            "tier",
        ]
        check_schema(all_columns, CustomerRow, "customers", mock_logger)
        mock_logger.warning.assert_not_called()

    def test_customers_missing_required_email_raises_schema_error(self, mock_logger: MagicMock):
        """3. Customers missing required 'email' raises SchemaError (subtractive drift)."""
        missing_email = [
            "customer_id",
            "first_name",
            "last_name",
            "address",
            "signup_date",
            "tier",
        ]
        with pytest.raises(SchemaError, match="required columns missing.*email"):
            check_schema(missing_email, CustomerRow, "customers", mock_logger)

    def test_customers_missing_multiple_required_columns_reports_all_missing(self, mock_logger: MagicMock):
        """Multiple missing required columns are all captured in the SchemaError message."""
        incomplete_cols = ["customer_id", "first_name"]
        with pytest.raises(SchemaError) as exc_info:
            check_schema(incomplete_cols, CustomerRow, "customers", mock_logger)

        err_msg = str(exc_info.value)
        for expected_missing in ["address", "email", "last_name", "signup_date"]:
            assert expected_missing in err_msg

    def test_customers_extra_column_logs_warning_and_does_not_raise(self, mock_logger: MagicMock):
        """4. Extra column logs a warning and does not raise (FR-2.3 additive drift)."""
        columns_with_extra = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "address",
            "signup_date",
            "loyalty_points",
        ]
        check_schema(columns_with_extra, CustomerRow, "customers", mock_logger)
        assert mock_logger.warning.call_count == 1
        warning_msg = str(mock_logger.warning.call_args)
        assert "loyalty_points" in warning_msg


class TestCheckSchemaWithOtherModels:
    """Tests verifying check_schema works with OrderRow and ProductRow via SOURCE_SCHEMAS."""

    def test_orders_exact_schema_passes(self, mock_logger: MagicMock):
        exact_cols = [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price",
            "status",
        ]
        check_schema(exact_cols, OrderRow, "orders", mock_logger)
        mock_logger.warning.assert_not_called()

    def test_orders_missing_column_raises(self, mock_logger: MagicMock):
        missing_price = [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "status",
        ]
        with pytest.raises(SchemaError, match="unit_price"):
            check_schema(missing_price, OrderRow, "orders", mock_logger)

    def test_products_via_source_schemas_registry(self, mock_logger: MagicMock):
        """Verify passing model class retrieved directly from SOURCE_SCHEMAS registry."""
        product_model = SOURCE_SCHEMAS["products"]["model"]
        exact_cols = ["product_id", "name", "category", "unit_cost", "supplier_id", "updated_at"]

        check_schema(exact_cols, product_model, "products", mock_logger)
        mock_logger.warning.assert_not_called()

    def test_products_extra_column_warns_and_continues(self, mock_logger: MagicMock):
        product_model = SOURCE_SCHEMAS["products"]["model"]
        cols_with_extra = [
            "product_id",
            "name",
            "category",
            "unit_cost",
            "supplier_id",
            "updated_at",
            "warehouse_aisle",
        ]
        check_schema(cols_with_extra, product_model, "products", mock_logger)
        assert mock_logger.warning.call_count == 1
        assert "warehouse_aisle" in str(mock_logger.warning.call_args)


class TestCheckSchemaLegacyList:
    """Tests verifying backward compatibility for callers passing list[str]."""

    def test_legacy_list_exact_match_passes(self, mock_logger: MagicMock):
        expected = ["col_a", "col_b", "col_c"]
        actual = ["col_a", "col_b", "col_c"]
        check_schema(actual, expected, "legacy_source", mock_logger)
        mock_logger.warning.assert_not_called()

    def test_legacy_list_missing_column_raises(self, mock_logger: MagicMock):
        expected = ["col_a", "col_b", "col_c"]
        actual = ["col_a", "col_b"]
        with pytest.raises(SchemaError, match="col_c"):
            check_schema(actual, expected, "legacy_source", mock_logger)

    def test_legacy_list_extra_column_warns(self, mock_logger: MagicMock):
        expected = ["col_a", "col_b"]
        actual = ["col_a", "col_b", "col_extra"]
        check_schema(actual, expected, "legacy_source", mock_logger)
        assert mock_logger.warning.call_count == 1
        assert "col_extra" in str(mock_logger.warning.call_args)
