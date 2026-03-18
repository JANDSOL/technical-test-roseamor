from __future__ import annotations

import argparse
import json
import os
import warnings
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from io import StringIO

os.environ.setdefault("TQDM_DISABLE", "1")

import great_expectations as gx
import pandas as pd

from common import ensure_reports_dir, get_postgres_connection, list_schema_tables, load_table_dataframe


warnings.filterwarnings(
    "ignore",
    message="`result_format` configured at the .* will not be persisted.*",
    category=UserWarning,
)


DEFAULT_SCHEMAS = ["raw", "staging", "mart"]
ALLOWED_SEGMENTS = ["Wholesale", "Corporate", "Distributor", "E-commerce", "Retail", "Unknown"]
ALLOWED_CHANNELS = ["ecommerce", "retail", "wholesale", "export"]


def divider(title: str) -> None:
    print(f"\n{'=' * 18} {title} {'=' * 18}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Great Expectations checks over PostgreSQL warehouse tables.")
    parser.add_argument("--raw", action="store_true", help="Inspect raw schema tables only.")
    parser.add_argument("--staging", action="store_true", help="Inspect staging schema tables only.")
    parser.add_argument("--mart", action="store_true", help="Inspect mart schema tables only.")
    return parser.parse_args()


def selected_schemas(args: argparse.Namespace) -> list[str]:
    schemas = []
    if args.raw:
        schemas.append("raw")
    if args.staging:
        schemas.append("staging")
    if args.mart:
        schemas.append("mart")
    return schemas or DEFAULT_SCHEMAS


def build_validator(context, dataset_name: str, dataframe: pd.DataFrame):
    datasource = context.data_sources.add_pandas(name=f"{dataset_name}_datasource")
    asset = datasource.add_dataframe_asset(name=f"{dataset_name}_asset")
    batch_request = asset.build_batch_request(options={"dataframe": dataframe})
    suite = gx.ExpectationSuite(name=f"{dataset_name}_suite")
    return context.get_validator(batch_request=batch_request, expectation_suite=suite)


def result_to_dict(result) -> dict:
    if hasattr(result, "to_json_dict"):
        return result.to_json_dict()

    payload = {}
    for attribute in ("success", "expectation_config", "result", "meta", "exception_info"):
        if hasattr(result, attribute):
            payload[attribute] = getattr(result, attribute)
    return payload


def normalize_sample(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat(sep=" ")
    return value


def build_display_samples(result: dict, source_dataframe: pd.DataFrame | None, source_column: str | None) -> list:
    details = result.get("result", {}) or {}
    indexes = details.get("partial_unexpected_index_list", [])[:5]

    if source_dataframe is None or source_column is None or not indexes:
        return details.get("partial_unexpected_list", [])[:5]

    samples = []
    for index in indexes:
        if 0 <= index < len(source_dataframe):
            samples.append(normalize_sample(source_dataframe.iloc[index][source_column]))
    return samples


def print_result(rule_name: str, result: dict, source_dataframe: pd.DataFrame | None = None, source_column: str | None = None) -> None:
    status = "PASS" if result.get("success") else "FAIL"
    details = result.get("result", {}) or {}
    unexpected_count = details.get("unexpected_count", 0)
    unexpected_percent = details.get("unexpected_percent", 0.0)
    unexpected_list = build_display_samples(result, source_dataframe, source_column)
    summary = f"[{status}] {rule_name} | unexpected_count={unexpected_count} | unexpected_percent={unexpected_percent}"
    if unexpected_list:
        summary += f" | samples={unexpected_list}"
    print(summary)


def run_suite(
    table_id: str,
    validator,
    expectations: list[tuple[str, object]],
    display_sources: dict[str, tuple[pd.DataFrame, str]] | None = None,
) -> list[dict]:
    divider(table_id.upper())
    suite_results: list[dict] = []
    for rule_name, expectation_call in expectations:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            result = result_to_dict(expectation_call())
        source_dataframe = None
        source_column = None
        if display_sources and rule_name in display_sources:
            source_dataframe, source_column = display_sources[rule_name]
        print_result(rule_name, result, source_dataframe=source_dataframe, source_column=source_column)
        suite_results.append({"rule": rule_name, **result})
    return suite_results


def prepare_validation_frame(schema: str, table: str, dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    if (schema, table) == ("raw", "customers_raw"):
        working["created_at_parsed"] = pd.to_datetime(working["created_at"], errors="coerce")
    elif (schema, table) == ("raw", "products_raw"):
        working["cost_numeric"] = pd.to_numeric(working["cost"], errors="coerce")
        working["active_bool"] = working["active"].astype("string").str.lower().map({"true": True, "false": False})
    elif (schema, table) == ("raw", "orders_raw"):
        working["quantity_numeric"] = pd.to_numeric(working["quantity"], errors="coerce")
        working["unit_price_numeric"] = pd.to_numeric(working["unit_price"], errors="coerce")
        working["order_date_parsed"] = pd.to_datetime(working["order_date"], errors="coerce")
        working["business_key_unique"] = ~working.duplicated(
            subset=["order_id", "customer_id", "sku", "quantity", "unit_price", "order_date", "channel", "source_occurrence"],
            keep=False,
        )
    elif (schema, table) == ("staging", "orders_clean"):
        working["return_consistent"] = (
            ((working["is_return"] == True) & (working["quantity"] < 0))
            | ((working["is_return"] == False) & (working["quantity"] > 0))
        )
        working["business_key_unique"] = ~working.duplicated(
            subset=["order_id", "customer_id", "sku", "order_date", "channel"],
            keep=False,
        )
    elif (schema, table) == ("mart", "fact_order_line"):
        for column in ["quantity", "unit_price", "unit_cost", "total_price", "total_cost"]:
            working[column] = pd.to_numeric(working[column], errors="coerce")
        working["return_consistent"] = (
            ((working["is_return"] == True) & (working["quantity"] < 0))
            | ((working["is_return"] == False) & (working["quantity"] > 0))
        )
        working["total_price_expected"] = (working["quantity"] * working["unit_price"]).round(2)
        working["total_price_matches"] = working["total_price"].round(2) == working["total_price_expected"]
        working["total_cost_expected"] = (working["quantity"] * working["unit_cost"]).round(2)
        working["total_cost_matches"] = working["unit_cost"].isna() | (working["total_cost"].round(2) == working["total_cost_expected"])
        working["business_key_unique"] = ~working.duplicated(
            subset=["order_id", "customer_key", "channel_key", "product_key", "order_date_key"],
            keep=False,
        )

    return working


def build_table_expectations(
    schema: str,
    table: str,
    validator,
    source_dataframe: pd.DataFrame,
) -> tuple[list[tuple[str, object]], dict[str, tuple[pd.DataFrame, str]]]:
    today = pd.Timestamp(date.today())
    expectations: list[tuple[str, object]] = []
    display_sources: dict[str, tuple[pd.DataFrame, str]] = {}

    if (schema, table) == ("raw", "customers_raw"):
        expectations = [
            ("customer_id is not null", lambda: validator.expect_column_values_to_not_be_null("customer_id", result_format="SUMMARY")),
            ("customer_id matches C####", lambda: validator.expect_column_values_to_match_regex("customer_id", r"^C\d{4}$", result_format="SUMMARY")),
            ("name is not null", lambda: validator.expect_column_values_to_not_be_null("name", result_format="SUMMARY")),
            ("country is not null", lambda: validator.expect_column_values_to_not_be_null("country", result_format="SUMMARY")),
            ("segment is valid", lambda: validator.expect_column_values_to_be_in_set("segment", ALLOWED_SEGMENTS[:-1], result_format="SUMMARY")),
            ("created_at is valid datetime", lambda: validator.expect_column_values_to_not_be_null("created_at_parsed", result_format="SUMMARY")),
            ("created_at is not in the future", lambda: validator.expect_column_values_to_be_between("created_at_parsed", max_value=today, result_format="SUMMARY")),
        ]
        display_sources = {
            "country is not null": (source_dataframe, "country"),
            "segment is valid": (source_dataframe, "segment"),
            "created_at is valid datetime": (source_dataframe, "created_at"),
            "created_at is not in the future": (source_dataframe, "created_at"),
        }
    elif (schema, table) == ("raw", "products_raw"):
        expectations = [
            ("sku is not null", lambda: validator.expect_column_values_to_not_be_null("sku", result_format="SUMMARY")),
            ("sku matches SKU####", lambda: validator.expect_column_values_to_match_regex("sku", r"^SKU\d{4}$", result_format="SUMMARY")),
            ("category is not null", lambda: validator.expect_column_values_to_not_be_null("category", result_format="SUMMARY")),
            ("cost is numeric", lambda: validator.expect_column_values_to_not_be_null("cost_numeric", result_format="SUMMARY")),
            ("cost is non-negative", lambda: validator.expect_column_values_to_be_between("cost_numeric", min_value=0, result_format="SUMMARY")),
            ("active contains booleans", lambda: validator.expect_column_values_to_not_be_null("active_bool", result_format="SUMMARY")),
        ]
        display_sources = {
            "category is not null": (source_dataframe, "category"),
            "cost is numeric": (source_dataframe, "cost"),
            "cost is non-negative": (source_dataframe, "cost"),
            "active contains booleans": (source_dataframe, "active"),
        }
    elif (schema, table) == ("raw", "orders_raw"):
        expectations = [
            ("order_id is not null", lambda: validator.expect_column_values_to_not_be_null("order_id", result_format="SUMMARY")),
            ("order_id matches O######", lambda: validator.expect_column_values_to_match_regex("order_id", r"^O\d{6}$", result_format="SUMMARY")),
            ("customer_id matches C####", lambda: validator.expect_column_values_to_match_regex("customer_id", r"^C\d{4}$", result_format="SUMMARY")),
            ("sku matches SKU####", lambda: validator.expect_column_values_to_match_regex("sku", r"^SKU\d{4}$", result_format="SUMMARY")),
            ("quantity is numeric", lambda: validator.expect_column_values_to_not_be_null("quantity_numeric", result_format="SUMMARY")),
            ("unit_price is numeric", lambda: validator.expect_column_values_to_not_be_null("unit_price_numeric", result_format="SUMMARY")),
            ("order_date is valid datetime", lambda: validator.expect_column_values_to_not_be_null("order_date_parsed", result_format="SUMMARY")),
            ("channel is valid", lambda: validator.expect_column_values_to_be_in_set("channel", ALLOWED_CHANNELS, result_format="SUMMARY")),
            ("source row key is unique", lambda: validator.expect_column_values_to_be_in_set("business_key_unique", [True], result_format="SUMMARY")),
        ]
        display_sources = {
            "quantity is numeric": (source_dataframe, "quantity"),
            "unit_price is numeric": (source_dataframe, "unit_price"),
            "order_date is valid datetime": (source_dataframe, "order_date"),
            "channel is valid": (source_dataframe, "channel"),
        }
    elif (schema, table) == ("staging", "customers_clean"):
        expectations = [
            ("customer_id is unique", lambda: validator.expect_column_values_to_be_unique("customer_id", result_format="SUMMARY")),
            ("customer_id matches C####", lambda: validator.expect_column_values_to_match_regex("customer_id", r"^C\d{4}$", result_format="SUMMARY")),
            ("customer_name is not null", lambda: validator.expect_column_values_to_not_be_null("customer_name", result_format="SUMMARY")),
            ("country is not null", lambda: validator.expect_column_values_to_not_be_null("country", result_format="SUMMARY")),
            ("segment is not null", lambda: validator.expect_column_values_to_not_be_null("segment", result_format="SUMMARY")),
            ("segment is valid", lambda: validator.expect_column_values_to_be_in_set("segment", ALLOWED_SEGMENTS, result_format="SUMMARY")),
        ]
    elif (schema, table) == ("staging", "products_clean"):
        expectations = [
            ("sku is unique", lambda: validator.expect_column_values_to_be_unique("sku", result_format="SUMMARY")),
            ("sku matches SKU####", lambda: validator.expect_column_values_to_match_regex("sku", r"^SKU\d{4}$", result_format="SUMMARY")),
            ("category is not null", lambda: validator.expect_column_values_to_not_be_null("category", result_format="SUMMARY")),
            ("unit_cost is non-negative", lambda: validator.expect_column_values_to_be_between("unit_cost", min_value=0, result_format="SUMMARY")),
        ]
    elif (schema, table) == ("staging", "orders_clean"):
        expectations = [
            ("order_id matches O######", lambda: validator.expect_column_values_to_match_regex("order_id", r"^O\d{6}$", result_format="SUMMARY")),
            ("customer_id matches C####", lambda: validator.expect_column_values_to_match_regex("customer_id", r"^C\d{4}$", result_format="SUMMARY")),
            ("sku matches SKU####", lambda: validator.expect_column_values_to_match_regex("sku", r"^SKU\d{4}$", result_format="SUMMARY")),
            ("quantity is non-zero", lambda: validator.expect_column_values_to_not_be_null("quantity", result_format="SUMMARY")),
            ("unit_price is non-negative", lambda: validator.expect_column_values_to_be_between("unit_price", min_value=0, result_format="SUMMARY")),
            ("channel is valid", lambda: validator.expect_column_values_to_be_in_set("channel", ALLOWED_CHANNELS, result_format="SUMMARY")),
            ("return flag is consistent", lambda: validator.expect_column_values_to_be_in_set("return_consistent", [True], result_format="SUMMARY")),
            ("business key is unique", lambda: validator.expect_column_values_to_be_in_set("business_key_unique", [True], result_format="SUMMARY")),
        ]
    elif (schema, table) == ("mart", "dim_customer"):
        expectations = [
            ("customer_key is unique", lambda: validator.expect_column_values_to_be_unique("customer_key", result_format="SUMMARY")),
            ("customer_id is unique", lambda: validator.expect_column_values_to_be_unique("customer_id", result_format="SUMMARY")),
            ("customer_name is not null", lambda: validator.expect_column_values_to_not_be_null("customer_name", result_format="SUMMARY")),
            ("customer_id matches C####", lambda: validator.expect_column_values_to_match_regex("customer_id", r"^C\d{4}$", result_format="SUMMARY")),
        ]
    elif (schema, table) == ("mart", "dim_product"):
        expectations = [
            ("product_key is unique", lambda: validator.expect_column_values_to_be_unique("product_key", result_format="SUMMARY")),
            ("sku is unique", lambda: validator.expect_column_values_to_be_unique("sku", result_format="SUMMARY")),
            ("sku matches SKU####", lambda: validator.expect_column_values_to_match_regex("sku", r"^SKU\d{4}$", result_format="SUMMARY")),
        ]
    elif (schema, table) == ("mart", "dim_channel"):
        expectations = [
            ("channel_key is unique", lambda: validator.expect_column_values_to_be_unique("channel_key", result_format="SUMMARY")),
            ("channel_code is unique", lambda: validator.expect_column_values_to_be_unique("channel_code", result_format="SUMMARY")),
            ("channel_code is valid", lambda: validator.expect_column_values_to_be_in_set("channel_code", ALLOWED_CHANNELS, result_format="SUMMARY")),
            ("channel_name is not null", lambda: validator.expect_column_values_to_not_be_null("channel_name", result_format="SUMMARY")),
        ]
    elif (schema, table) == ("mart", "dim_date"):
        expectations = [
            ("date_key is unique", lambda: validator.expect_column_values_to_be_unique("date_key", result_format="SUMMARY")),
            ("full_date is unique", lambda: validator.expect_column_values_to_be_unique("full_date", result_format="SUMMARY")),
            ("full_date is not null", lambda: validator.expect_column_values_to_not_be_null("full_date", result_format="SUMMARY")),
            ("quarter is between 1 and 4", lambda: validator.expect_column_values_to_be_between("quarter", min_value=1, max_value=4, result_format="SUMMARY")),
            ("month is between 1 and 12", lambda: validator.expect_column_values_to_be_between("month", min_value=1, max_value=12, result_format="SUMMARY")),
            ("day is between 1 and 31", lambda: validator.expect_column_values_to_be_between("day", min_value=1, max_value=31, result_format="SUMMARY")),
        ]
    elif (schema, table) == ("mart", "fact_order_line"):
        expectations = [
            ("order_line_key is unique", lambda: validator.expect_column_values_to_be_unique("order_line_key", result_format="SUMMARY")),
            ("order_id matches O######", lambda: validator.expect_column_values_to_match_regex("order_id", r"^O\d{6}$", result_format="SUMMARY")),
            ("customer_key is not null", lambda: validator.expect_column_values_to_not_be_null("customer_key", result_format="SUMMARY")),
            ("channel_key is not null", lambda: validator.expect_column_values_to_not_be_null("channel_key", result_format="SUMMARY")),
            ("product_key is not null", lambda: validator.expect_column_values_to_not_be_null("product_key", result_format="SUMMARY")),
            ("order_date_key is not null", lambda: validator.expect_column_values_to_not_be_null("order_date_key", result_format="SUMMARY")),
            ("unit_price is non-negative", lambda: validator.expect_column_values_to_be_between("unit_price", min_value=0, result_format="SUMMARY")),
            ("unit_cost is non-negative", lambda: validator.expect_column_values_to_be_between("unit_cost", min_value=0, result_format="SUMMARY")),
            ("return flag is consistent", lambda: validator.expect_column_values_to_be_in_set("return_consistent", [True], result_format="SUMMARY")),
            ("total_price matches quantity * unit_price", lambda: validator.expect_column_values_to_be_in_set("total_price_matches", [True], result_format="SUMMARY")),
            ("total_cost matches quantity * unit_cost", lambda: validator.expect_column_values_to_be_in_set("total_cost_matches", [True], result_format="SUMMARY")),
            ("business key is unique", lambda: validator.expect_column_values_to_be_in_set("business_key_unique", [True], result_format="SUMMARY")),
        ]

    return expectations, display_sources


def main() -> None:
    args = parse_args()
    schemas = selected_schemas(args)
    context = gx.get_context()
    all_results: dict[str, list[dict]] = {}

    divider("DATABASE DATA QUALITY RULES")
    print(f"Inspecting schemas: {', '.join(schemas)}")

    with get_postgres_connection() as connection:
        tables = list_schema_tables(connection, schemas)
        if not tables:
            print("No tables found for the selected schemas.")
            return

        for schema, table in tables:
            table_id = f"{schema}.{table}"
            source_dataframe = load_table_dataframe(connection, schema, table)
            if source_dataframe.empty:
                divider(table_id.upper())
                print("Table is empty. Skipping data quality rules.")
                all_results[table_id] = []
                continue

            validation_dataframe = prepare_validation_frame(schema, table, source_dataframe)
            validator = build_validator(context, f"{schema}_{table}", validation_dataframe)
            expectations, display_sources = build_table_expectations(schema, table, validator, source_dataframe)
            if not expectations:
                divider(table_id.upper())
                print("No configured rules for this table.")
                all_results[table_id] = []
                continue

            all_results[table_id] = run_suite(
                table_id,
                validator,
                expectations,
                display_sources=display_sources,
            )

    reports_dir = ensure_reports_dir()
    output_path = reports_dir / "great_expectations_results_db.json"
    output_path.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")

    divider("RESULTS FILE")
    print(f"Validation results saved to: {output_path}")


if __name__ == "__main__":
    main()
