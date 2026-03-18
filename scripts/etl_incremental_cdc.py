from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

from common import REPORTS_DIR, get_postgres_connection


ROOT_DIR = Path(__file__).resolve().parents[1]
QUALITY_RESULTS_PATH = REPORTS_DIR / "great_expectations_results.json"

CHANNEL_NAME_MAP = {
    "ecommerce": "E-commerce",
    "retail": "Retail",
    "wholesale": "Wholesale",
    "export": "Export",
}

ALLOWED_CHANNELS = set(CHANNEL_NAME_MAP)

RAW_CUSTOMER_COLUMNS = ["customer_id", "name", "country", "segment", "created_at"]
RAW_PRODUCT_COLUMNS = ["sku", "category", "cost", "active"]
RAW_ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "sku",
    "quantity",
    "unit_price",
    "order_date",
    "channel",
    "source_occurrence",
]


def divider(title: str) -> None:
    print(f"\n{'=' * 18} {title} {'=' * 18}")


def normalize_text(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def normalize_upper(value) -> str | None:
    text = normalize_text(value)
    return text.upper() if text else None


def normalize_lower(value) -> str | None:
    text = normalize_text(value)
    return text.lower() if text else None


def parse_bool(value) -> bool | None:
    text = normalize_lower(value)
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None


def to_python_datetime(value):
    if pd.isna(value):
        return None
    return value.to_pydatetime()


def load_quality_context() -> dict:
    if not QUALITY_RESULTS_PATH.exists():
        print(f"Quality results file not found: {QUALITY_RESULTS_PATH}")
        return {}

    payload = json.loads(QUALITY_RESULTS_PATH.read_text(encoding="utf-8"))
    customers = {result["rule"]: result for result in payload.get("customers", [])}
    products = {result["rule"]: result for result in payload.get("products", [])}
    orders = {result["rule"]: result for result in payload.get("orders", [])}

    context = {
        "customer_country_nulls": customers.get("country is not null", {}).get("result", {}).get("unexpected_count", 0),
        "product_negative_costs": products.get("cost is non-negative", {}).get("result", {}).get("unexpected_count", 0),
        "order_duplicate_ids": orders.get("order_id is unique", {}).get("result", {}).get("unexpected_count", 0),
        "order_missing_unit_price": orders.get("unit_price is numeric", {}).get("result", {}).get("unexpected_count", 0),
        "order_invalid_date": orders.get("order_date is valid datetime", {}).get("result", {}).get("unexpected_count", 0),
        "order_negative_quantity": orders.get("quantity is greater than zero", {}).get("result", {}).get("unexpected_count", 0),
    }

    print(
        "Reviewed Great Expectations results: "
        f"customer_country_nulls={context['customer_country_nulls']} | "
        f"product_negative_costs={context['product_negative_costs']} | "
        f"order_duplicate_ids={context['order_duplicate_ids']} | "
        f"order_missing_unit_price={context['order_missing_unit_price']} | "
        f"order_invalid_date={context['order_invalid_date']} | "
        f"order_negative_quantity={context['order_negative_quantity']}"
    )
    return context


def load_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customers = pd.read_csv(ROOT_DIR / "data" / "customers.csv", dtype="string")
    products = pd.read_csv(ROOT_DIR / "data" / "products.csv", dtype="string")
    orders = pd.read_csv(ROOT_DIR / "data" / "orders.csv", dtype="string")
    return customers, products, orders


def sql_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def canonicalize(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.replace(tzinfo=None, microsecond=0).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return round(float(value), 6)
    if isinstance(value, float):
        return round(value, 6)
    return value


def build_cdc_key(values) -> str:
    return repr(tuple(canonicalize(value) for value in values))


def fetch_table_dataframe(connection, table_name: str, columns: list[str]) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name};")
        rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def with_cdc_key(dataframe: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    working = dataframe.copy()
    if working.empty:
        working["_cdc_key"] = pd.Series(dtype="object")
        return working
    working["_cdc_key"] = working[key_columns].apply(lambda row: build_cdc_key(row), axis=1)
    return working


def rows_differ(existing_row: pd.Series, desired_row: pd.Series, compare_columns: list[str]) -> bool:
    return any(canonicalize(existing_row[column]) != canonicalize(desired_row[column]) for column in compare_columns)


def compute_cdc_operations(
    existing_df: pd.DataFrame,
    desired_df: pd.DataFrame,
    key_columns: list[str],
    all_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    compare_columns = [column for column in all_columns if column not in key_columns]
    existing = with_cdc_key(existing_df[all_columns], key_columns)
    desired = with_cdc_key(desired_df[all_columns], key_columns)

    existing_keys = set(existing["_cdc_key"].tolist())
    desired_keys = set(desired["_cdc_key"].tolist())

    insert_df = desired[~desired["_cdc_key"].isin(existing_keys)][all_columns].copy()
    delete_df = existing[~existing["_cdc_key"].isin(desired_keys)][key_columns].copy()

    update_rows: list[tuple] = []
    if compare_columns:
        existing_common = existing[existing["_cdc_key"].isin(existing_keys & desired_keys)].set_index("_cdc_key")
        desired_common = desired[desired["_cdc_key"].isin(existing_keys & desired_keys)].set_index("_cdc_key")
        for key in existing_keys & desired_keys:
            existing_row = existing_common.loc[key]
            desired_row = desired_common.loc[key]
            if rows_differ(existing_row, desired_row, compare_columns):
                update_rows.append(tuple(desired_row[column] for column in all_columns))

    update_df = pd.DataFrame(update_rows, columns=all_columns)
    return insert_df, update_df, delete_df


def dataframe_to_tuples(dataframe: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(sql_value(getattr(row, column)) for column in columns) for row in dataframe[columns].itertuples(index=False)]


def apply_insert(cursor, table_name: str, dataframe: pd.DataFrame, all_columns: list[str]) -> int:
    if dataframe.empty:
        return 0
    placeholders = ", ".join(["%s"] * len(all_columns))
    sql = f"INSERT INTO {table_name} ({', '.join(all_columns)}) VALUES ({placeholders});"
    cursor.executemany(sql, dataframe_to_tuples(dataframe, all_columns))
    return len(dataframe)


def apply_update(cursor, table_name: str, dataframe: pd.DataFrame, key_columns: list[str], all_columns: list[str]) -> int:
    compare_columns = [column for column in all_columns if column not in key_columns]
    if dataframe.empty or not compare_columns:
        return 0
    set_clause = ", ".join(f"{column} = %s" for column in compare_columns)
    where_clause = " AND ".join(f"{column} = %s" for column in key_columns)
    sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
    rows = []
    for row in dataframe.itertuples(index=False):
        rows.append(
            tuple(sql_value(getattr(row, column)) for column in compare_columns)
            + tuple(sql_value(getattr(row, column)) for column in key_columns)
        )
    cursor.executemany(sql, rows)
    return len(dataframe)


def apply_delete(cursor, table_name: str, dataframe: pd.DataFrame, key_columns: list[str]) -> int:
    if dataframe.empty:
        return 0
    where_clause = " AND ".join(f"{column} = %s" for column in key_columns)
    sql = f"DELETE FROM {table_name} WHERE {where_clause};"
    cursor.executemany(sql, dataframe_to_tuples(dataframe, key_columns))
    return len(dataframe)


def sync_table(
    connection,
    table_name: str,
    desired_df: pd.DataFrame,
    key_columns: list[str],
    all_columns: list[str],
    delete_missing: bool = True,
) -> dict:
    existing_df = fetch_table_dataframe(connection, table_name, all_columns)
    insert_df, update_df, delete_df = compute_cdc_operations(existing_df, desired_df, key_columns, all_columns)

    with connection.cursor() as cursor:
        inserted = apply_insert(cursor, table_name, insert_df, all_columns)
        updated = apply_update(cursor, table_name, update_df, key_columns, all_columns)
        deleted = apply_delete(cursor, table_name, delete_df, key_columns) if delete_missing else 0

    return {"inserted": inserted, "updated": updated, "deleted": deleted}


def delete_missing_rows(connection, table_name: str, desired_df: pd.DataFrame, key_columns: list[str], all_columns: list[str]) -> int:
    existing_df = fetch_table_dataframe(connection, table_name, all_columns)
    _, _, delete_df = compute_cdc_operations(existing_df, desired_df, key_columns, all_columns)
    with connection.cursor() as cursor:
        return apply_delete(cursor, table_name, delete_df, key_columns)


def backfill_raw_order_occurrence(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH ranked AS (
                SELECT
                    ctid,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id, customer_id, sku, quantity, unit_price, order_date, channel
                        ORDER BY ingested_at, ctid
                    ) AS occurrence_rank
                FROM raw.orders_raw
            )
            UPDATE raw.orders_raw AS target
            SET source_occurrence = ranked.occurrence_rank
            FROM ranked
            WHERE target.ctid = ranked.ctid;
            """
        )


def prepare_customers_stage(customers: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    staged = customers.copy()
    staged["customer_id"] = staged["customer_id"].map(normalize_upper)
    staged["customer_name"] = staged["name"].map(normalize_text)
    staged["country"] = staged["country"].map(normalize_text)
    staged["segment"] = staged["segment"].map(normalize_text)
    staged["customer_created_at"] = pd.to_datetime(staged["created_at"].map(normalize_text), errors="coerce")

    before = len(staged)
    staged = staged.drop_duplicates(subset=["customer_id", "customer_name", "country", "segment", "created_at"], keep="first").copy()
    exact_duplicates_removed = before - len(staged)

    staged["country_imputed"] = staged["country"].isna()
    staged.loc[staged["country_imputed"], "country"] = "Unknown"

    staged["segment_imputed"] = staged["segment"].isna()
    staged.loc[staged["segment_imputed"], "segment"] = "Unknown"

    staged["quality_score"] = (
        staged["customer_name"].notna().astype(int)
        + staged["country"].notna().astype(int)
        + staged["segment"].notna().astype(int)
        + staged["customer_created_at"].notna().astype(int)
    )

    before = len(staged)
    staged = (
        staged.sort_values(by=["quality_score", "customer_created_at"], ascending=[False, False], na_position="last")
        .drop_duplicates(subset=["customer_id"], keep="first")
        .copy()
    )
    duplicate_customer_ids_removed = before - len(staged)

    before = len(staged)
    staged = staged[staged["customer_id"].notna() & staged["customer_name"].notna()].copy()
    dropped_missing_required = before - len(staged)

    staged = staged[
        [
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "customer_created_at",
            "country_imputed",
            "segment_imputed",
        ]
    ].reset_index(drop=True)

    stats = {
        "exact_duplicates_removed": exact_duplicates_removed,
        "duplicate_customer_ids_removed": duplicate_customer_ids_removed,
        "country_imputed": int(staged["country_imputed"].sum()),
        "segment_imputed": int(staged["segment_imputed"].sum()),
        "dropped_missing_required": dropped_missing_required,
        "rows": len(staged),
    }
    return staged, stats


def prepare_products_stage(products: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    staged = products.copy()
    staged["sku"] = staged["sku"].map(normalize_upper)
    staged["category"] = staged["category"].map(normalize_text)
    staged["active_bool"] = staged["active"].map(parse_bool)
    staged["cost_numeric"] = pd.to_numeric(products["cost"].map(normalize_text), errors="coerce")
    staged["cost_sign_corrected"] = staged["cost_numeric"] < 0
    staged["unit_cost"] = staged["cost_numeric"].abs()

    before = len(staged)
    staged = staged.drop_duplicates(subset=["sku", "category", "cost", "active"], keep="first").copy()
    exact_duplicates_removed = before - len(staged)

    staged["category_imputed"] = staged["category"].isna()
    staged.loc[staged["category_imputed"], "category"] = "Unknown"

    staged["quality_score"] = (
        staged["category"].notna().astype(int)
        + staged["unit_cost"].notna().astype(int)
        + staged["active_bool"].notna().astype(int)
    )

    before = len(staged)
    staged = (
        staged.sort_values(by=["quality_score", "unit_cost"], ascending=[False, False], na_position="last")
        .drop_duplicates(subset=["sku"], keep="first")
        .copy()
    )
    duplicate_skus_removed = before - len(staged)

    before = len(staged)
    staged = staged[staged["sku"].notna()].copy()
    dropped_missing_required = before - len(staged)

    staged = staged[
        [
            "sku",
            "category",
            "unit_cost",
            "active_bool",
            "category_imputed",
            "cost_sign_corrected",
        ]
    ].rename(columns={"active_bool": "is_active"}).reset_index(drop=True)

    stats = {
        "exact_duplicates_removed": exact_duplicates_removed,
        "duplicate_skus_removed": duplicate_skus_removed,
        "category_imputed": int(staged["category_imputed"].sum()),
        "negative_cost_corrected": int(staged["cost_sign_corrected"].sum()),
        "dropped_missing_required": dropped_missing_required,
        "rows": len(staged),
    }
    return staged, stats


def prepare_orders_stage(orders: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    staged = orders.copy()
    staged["order_id"] = staged["order_id"].map(normalize_upper)
    staged["customer_id"] = staged["customer_id"].map(normalize_upper)
    staged["sku"] = staged["sku"].map(normalize_upper)
    staged["channel"] = staged["channel"].map(normalize_lower)
    staged["quantity"] = staged["quantity"].map(normalize_text)
    staged["unit_price"] = staged["unit_price"].map(normalize_text)
    staged["order_date"] = staged["order_date"].map(normalize_text)

    before = len(staged)
    staged = staged.drop_duplicates(
        subset=["order_id", "customer_id", "sku", "quantity", "unit_price", "order_date", "channel"],
        keep="first",
    ).copy()
    exact_duplicates_removed = before - len(staged)

    before = len(staged)
    staged = staged[
        staged["order_id"].notna()
        & staged["customer_id"].notna()
        & staged["sku"].notna()
        & staged["channel"].notna()
    ].copy()
    dropped_missing_keys = before - len(staged)

    before = len(staged)
    staged = staged[staged["channel"].isin(ALLOWED_CHANNELS)].copy()
    dropped_invalid_channel = before - len(staged)

    staged["quantity_numeric"] = pd.to_numeric(staged["quantity"], errors="coerce")
    staged["unit_price_numeric"] = pd.to_numeric(staged["unit_price"], errors="coerce")
    staged["order_date_ts"] = pd.to_datetime(staged["order_date"], errors="coerce")

    before = len(staged)
    staged = staged[staged["quantity_numeric"].notna() & (staged["quantity_numeric"] != 0)].copy()
    dropped_invalid_quantity = before - len(staged)

    before = len(staged)
    staged = staged[staged["unit_price_numeric"].notna() & (staged["unit_price_numeric"] >= 0)].copy()
    dropped_invalid_unit_price = before - len(staged)

    before = len(staged)
    staged = staged[staged["order_date_ts"].notna()].copy()
    dropped_invalid_order_date = before - len(staged)

    staged["is_return"] = staged["quantity_numeric"] < 0
    staged = staged[
        [
            "order_id",
            "customer_id",
            "sku",
            "quantity_numeric",
            "is_return",
            "unit_price_numeric",
            "order_date_ts",
            "channel",
        ]
    ].rename(
        columns={
            "quantity_numeric": "quantity",
            "unit_price_numeric": "unit_price",
            "order_date_ts": "order_date",
        }
    ).reset_index(drop=True)

    stats = {
        "exact_duplicates_removed": exact_duplicates_removed,
        "dropped_missing_keys": dropped_missing_keys,
        "dropped_invalid_channel": dropped_invalid_channel,
        "dropped_invalid_quantity": dropped_invalid_quantity,
        "dropped_invalid_unit_price": dropped_invalid_unit_price,
        "dropped_invalid_order_date": dropped_invalid_order_date,
        "return_rows": int(staged["is_return"].sum()),
        "rows": len(staged),
    }
    return staged, stats


def build_raw_customers_df(customers_source: pd.DataFrame) -> pd.DataFrame:
    raw = customers_source.copy()
    for column in RAW_CUSTOMER_COLUMNS:
        raw[column] = raw[column].map(normalize_text)
    return raw[RAW_CUSTOMER_COLUMNS].copy()


def build_raw_products_df(products_source: pd.DataFrame) -> pd.DataFrame:
    raw = products_source.copy()
    for column in RAW_PRODUCT_COLUMNS:
        raw[column] = raw[column].map(normalize_text)
    return raw[RAW_PRODUCT_COLUMNS].copy()


def build_raw_orders_df(orders_source: pd.DataFrame) -> pd.DataFrame:
    raw = orders_source.copy()
    raw["order_id"] = raw["order_id"].map(normalize_upper)
    raw["customer_id"] = raw["customer_id"].map(normalize_upper)
    raw["sku"] = raw["sku"].map(normalize_upper)
    raw["quantity"] = raw["quantity"].map(normalize_text)
    raw["unit_price"] = raw["unit_price"].map(normalize_text)
    raw["order_date"] = raw["order_date"].map(normalize_text)
    raw["channel"] = raw["channel"].map(normalize_lower)
    grouping_columns = ["order_id", "customer_id", "sku", "quantity", "unit_price", "order_date", "channel"]
    raw["source_occurrence"] = raw.groupby(grouping_columns, dropna=False).cumcount() + 1
    return raw[RAW_ORDER_COLUMNS].copy()


def build_stage_customer_df(stage_customers: pd.DataFrame) -> pd.DataFrame:
    return stage_customers[
        [
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "customer_created_at",
            "country_imputed",
            "segment_imputed",
        ]
    ].copy()


def build_stage_product_df(stage_products: pd.DataFrame) -> pd.DataFrame:
    return stage_products[
        [
            "sku",
            "category",
            "unit_cost",
            "is_active",
            "category_imputed",
            "cost_sign_corrected",
        ]
    ].copy()


def build_stage_order_df(stage_orders: pd.DataFrame) -> pd.DataFrame:
    return stage_orders[
        [
            "order_id",
            "customer_id",
            "sku",
            "quantity",
            "is_return",
            "unit_price",
            "order_date",
            "channel",
        ]
    ].copy()


def build_dim_customer_df(stage_customers: pd.DataFrame) -> pd.DataFrame:
    return stage_customers[
        ["customer_id", "customer_name", "country", "segment", "customer_created_at"]
    ].copy()


def build_dim_product_df(stage_products: pd.DataFrame) -> pd.DataFrame:
    return stage_products[["sku", "category", "is_active"]].copy()


def build_dim_channel_df(stage_orders: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(build_channel_rows(stage_orders), columns=["channel_code", "channel_name"])


def build_dim_date_df(stage_orders: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        build_date_rows(stage_orders),
        columns=[
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_text",
            "day",
            "day_text",
            "is_weekend",
        ],
    )


def build_channel_rows(stage_orders: pd.DataFrame) -> list[tuple]:
    codes = sorted(stage_orders["channel"].drop_duplicates().tolist())
    return [(code, CHANNEL_NAME_MAP[code]) for code in codes]


def build_date_rows(stage_orders: pd.DataFrame) -> list[tuple]:
    normalized_dates = sorted(stage_orders["order_date"].dt.normalize().drop_duplicates().tolist())
    rows = []
    for full_date in normalized_dates:
        rows.append(
            (
                int(full_date.strftime("%Y%m%d")),
                full_date.date(),
                full_date.year,
                ((full_date.month - 1) // 3) + 1,
                full_date.month,
                full_date.strftime("%B"),
                full_date.day,
                full_date.strftime("%A"),
                full_date.weekday() >= 5,
            )
        )
    return rows


def fetch_dimension_maps(connection) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT customer_id, customer_key FROM mart.dim_customer;")
        customer_map = {customer_id: customer_key for customer_id, customer_key in cursor.fetchall()}

        cursor.execute("SELECT sku, product_key FROM mart.dim_product;")
        product_map = {sku: product_key for sku, product_key in cursor.fetchall()}

        cursor.execute("SELECT channel_code, channel_key FROM mart.dim_channel;")
        channel_map = {channel_code: channel_key for channel_code, channel_key in cursor.fetchall()}

    return customer_map, product_map, channel_map


def build_fact_rows(
    stage_orders: pd.DataFrame,
    stage_products: pd.DataFrame,
    customer_map: dict[str, int],
    product_map: dict[str, int],
    channel_map: dict[str, int],
) -> tuple[list[tuple], dict]:
    product_cost_map = stage_products.set_index("sku")["unit_cost"].to_dict()

    missing_customer_refs = int((~stage_orders["customer_id"].isin(customer_map)).sum())
    missing_product_refs = int((~stage_orders["sku"].isin(product_map)).sum())
    missing_channel_refs = int((~stage_orders["channel"].isin(channel_map)).sum())

    conformed_orders = stage_orders[
        stage_orders["customer_id"].isin(customer_map)
        & stage_orders["sku"].isin(product_map)
        & stage_orders["channel"].isin(channel_map)
    ].copy()

    fact_rows = [
        (
            row.order_id,
            customer_map[row.customer_id],
            channel_map[row.channel],
            product_map[row.sku],
            int(row.order_date.strftime("%Y%m%d")),
            float(row.quantity),
            bool(row.is_return),
            float(row.unit_price),
            None if pd.isna(product_cost_map.get(row.sku)) else float(product_cost_map[row.sku]),
            float(row.quantity) * float(row.unit_price),
            None
            if pd.isna(product_cost_map.get(row.sku))
            else float(row.quantity) * float(product_cost_map[row.sku]),
        )
        for row in conformed_orders.itertuples(index=False)
    ]

    stats = {
        "rows": len(fact_rows),
        "dropped_missing_customer_refs": missing_customer_refs,
        "dropped_missing_product_refs": missing_product_refs,
        "dropped_missing_channel_refs": missing_channel_refs,
    }
    return fact_rows, stats


def build_fact_df(
    connection,
    stage_customers: pd.DataFrame,
    stage_orders: pd.DataFrame,
    stage_products: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    customer_map, product_map, channel_map = fetch_dimension_maps(connection)
    desired_customer_ids = set(stage_customers["customer_id"].tolist())
    desired_product_skus = set(stage_products["sku"].tolist())
    desired_channel_codes = set(stage_orders["channel"].tolist())

    customer_map = {
        customer_id: customer_key
        for customer_id, customer_key in customer_map.items()
        if customer_id in desired_customer_ids
    }
    product_map = {
        sku: product_key
        for sku, product_key in product_map.items()
        if sku in desired_product_skus
    }
    channel_map = {
        channel_code: channel_key
        for channel_code, channel_key in channel_map.items()
        if channel_code in desired_channel_codes
    }

    fact_rows, fact_stats = build_fact_rows(
        stage_orders=stage_orders,
        stage_products=stage_products,
        customer_map=customer_map,
        product_map=product_map,
        channel_map=channel_map,
    )
    fact_df = pd.DataFrame(
        fact_rows,
        columns=[
            "order_id",
            "customer_key",
            "channel_key",
            "product_key",
            "order_date_key",
            "quantity",
            "is_return",
            "unit_price",
            "unit_cost",
            "total_price",
            "total_cost",
        ],
    )
    return fact_df, fact_stats


def print_cdc_stats(title: str, stats: dict) -> None:
    print(
        f"{title}: "
        f"inserted={stats['inserted']} | "
        f"updated={stats['updated']} | "
        f"deleted={stats['deleted']}"
    )


def summarize_quality_context(quality_context: dict) -> None:
    if not quality_context:
        return
    print(
        "Source quality signals: "
        f"customer_country_nulls={quality_context['customer_country_nulls']} | "
        f"product_negative_costs={quality_context['product_negative_costs']} | "
        f"order_duplicate_ids={quality_context['order_duplicate_ids']} | "
        f"order_missing_unit_price={quality_context['order_missing_unit_price']} | "
        f"order_invalid_date={quality_context['order_invalid_date']} | "
        f"order_negative_quantity={quality_context['order_negative_quantity']}"
    )


def format_delta(stats: dict) -> str:
    return f"+{stats['inserted']} ~{stats['updated']} -{stats['deleted']}"


def total_delta(stats_by_table: dict[str, dict]) -> dict:
    return {
        "inserted": sum(stats["inserted"] for stats in stats_by_table.values()),
        "updated": sum(stats["updated"] for stats in stats_by_table.values()),
        "deleted": sum(stats["deleted"] for stats in stats_by_table.values()),
    }


def changed_table_lines(stats_by_table: dict[str, dict]) -> list[str]:
    lines = []
    for table_name, stats in stats_by_table.items():
        if stats["inserted"] or stats["updated"] or stats["deleted"]:
            lines.append(f"{table_name} {format_delta(stats)}")
    return lines


def print_cdc_layer(title: str, stats_by_table: dict[str, dict]) -> None:
    total = total_delta(stats_by_table)
    print(f"{title}: {format_delta(total)}")
    for line in changed_table_lines(stats_by_table):
        print(f"- {line}")


def main() -> None:
    divider("INCREMENTAL CDC ETL")
    quality_context = load_quality_context()
    customers_source, products_source, orders_source = load_sources()

    raw_customers_df = build_raw_customers_df(customers_source)
    raw_products_df = build_raw_products_df(products_source)
    raw_orders_df = build_raw_orders_df(orders_source)

    stage_customers, customer_stats = prepare_customers_stage(customers_source)
    stage_products, product_stats = prepare_products_stage(products_source)
    stage_orders, order_stats = prepare_orders_stage(orders_source)

    stage_customers_df = build_stage_customer_df(stage_customers)
    stage_products_df = build_stage_product_df(stage_products)
    stage_orders_df = build_stage_order_df(stage_orders)

    dim_customers_df = build_dim_customer_df(stage_customers)
    dim_products_df = build_dim_product_df(stage_products)
    dim_channels_df = build_dim_channel_df(stage_orders)
    dim_dates_df = build_dim_date_df(stage_orders)

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                ALTER TABLE raw.orders_raw
                ADD COLUMN IF NOT EXISTS source_occurrence INTEGER NOT NULL DEFAULT 1;
                """
            )
        backfill_raw_order_occurrence(connection)

        raw_customer_cdc = sync_table(
            connection,
            "raw.customers_raw",
            raw_customers_df,
            key_columns=["customer_id"],
            all_columns=RAW_CUSTOMER_COLUMNS,
        )
        raw_product_cdc = sync_table(
            connection,
            "raw.products_raw",
            raw_products_df,
            key_columns=["sku"],
            all_columns=RAW_PRODUCT_COLUMNS,
        )
        raw_order_cdc = sync_table(
            connection,
            "raw.orders_raw",
            raw_orders_df,
            key_columns=["order_id", "customer_id", "sku", "order_date", "channel", "source_occurrence"],
            all_columns=RAW_ORDER_COLUMNS,
        )

        stage_customer_cdc = sync_table(
            connection,
            "staging.customers_clean",
            stage_customers_df,
            key_columns=["customer_id"],
            all_columns=list(stage_customers_df.columns),
        )
        stage_product_cdc = sync_table(
            connection,
            "staging.products_clean",
            stage_products_df,
            key_columns=["sku"],
            all_columns=list(stage_products_df.columns),
        )
        stage_order_cdc = sync_table(
            connection,
            "staging.orders_clean",
            stage_orders_df,
            key_columns=["order_id", "customer_id", "sku", "order_date", "channel"],
            all_columns=list(stage_orders_df.columns),
        )

        mart_customer_cdc = sync_table(
            connection,
            "mart.dim_customer",
            dim_customers_df,
            key_columns=["customer_id"],
            all_columns=list(dim_customers_df.columns),
            delete_missing=False,
        )
        mart_product_cdc = sync_table(
            connection,
            "mart.dim_product",
            dim_products_df,
            key_columns=["sku"],
            all_columns=list(dim_products_df.columns),
            delete_missing=False,
        )
        mart_channel_cdc = sync_table(
            connection,
            "mart.dim_channel",
            dim_channels_df,
            key_columns=["channel_code"],
            all_columns=list(dim_channels_df.columns),
            delete_missing=False,
        )
        mart_date_cdc = sync_table(
            connection,
            "mart.dim_date",
            dim_dates_df,
            key_columns=["date_key"],
            all_columns=list(dim_dates_df.columns),
            delete_missing=False,
        )

        fact_df, fact_stats = build_fact_df(
            connection,
            stage_customers=stage_customers,
            stage_orders=stage_orders,
            stage_products=stage_products,
        )
        mart_fact_cdc = sync_table(
            connection,
            "mart.fact_order_line",
            fact_df,
            key_columns=["order_id", "customer_key", "channel_key", "product_key", "order_date_key"],
            all_columns=list(fact_df.columns),
        )

        mart_channel_cdc["deleted"] = delete_missing_rows(
            connection,
            "mart.dim_channel",
            dim_channels_df,
            ["channel_code"],
            list(dim_channels_df.columns),
        )
        mart_product_cdc["deleted"] = delete_missing_rows(
            connection,
            "mart.dim_product",
            dim_products_df,
            ["sku"],
            list(dim_products_df.columns),
        )
        mart_customer_cdc["deleted"] = delete_missing_rows(
            connection,
            "mart.dim_customer",
            dim_customers_df,
            ["customer_id"],
            list(dim_customers_df.columns),
        )
        mart_date_cdc["deleted"] = delete_missing_rows(
            connection,
            "mart.dim_date",
            dim_dates_df,
            ["date_key"],
            list(dim_dates_df.columns),
        )

        connection.commit()

    raw_cdc = {
        "customers_raw": raw_customer_cdc,
        "products_raw": raw_product_cdc,
        "orders_raw": raw_order_cdc,
    }
    staging_cdc = {
        "customers_clean": stage_customer_cdc,
        "products_clean": stage_product_cdc,
        "orders_clean": stage_order_cdc,
    }
    mart_cdc = {
        "dim_customer": mart_customer_cdc,
        "dim_product": mart_product_cdc,
        "dim_channel": mart_channel_cdc,
        "dim_date": mart_date_cdc,
        "fact_order_line": mart_fact_cdc,
    }

    divider("STAGING ACTIONS")
    print(
        "customers: "
        f"kept={customer_stats['rows']} | "
        f"country->Unknown={customer_stats['country_imputed']} | "
        f"segment->Unknown={customer_stats['segment_imputed']}"
    )
    print(
        "products: "
        f"kept={product_stats['rows']} | "
        f"category->Unknown={product_stats['category_imputed']} | "
        f"cost_abs_applied={product_stats['negative_cost_corrected']}"
    )
    print(
        "orders: "
        f"kept={order_stats['rows']} | "
        f"exact_duplicates_removed={order_stats['exact_duplicates_removed']} | "
        f"dropped_unit_price={order_stats['dropped_invalid_unit_price']} | "
        f"dropped_invalid_date={order_stats['dropped_invalid_order_date']} | "
        f"returns_kept={order_stats['return_rows']}"
    )

    divider("CDC SUMMARY")
    raw_total = total_delta(raw_cdc)
    staging_total = total_delta(staging_cdc)
    mart_total = total_delta(mart_cdc)
    overall_updates = raw_total["updated"] + staging_total["updated"] + mart_total["updated"]
    overall_deletes = raw_total["deleted"] + staging_total["deleted"] + mart_total["deleted"]
    overall_inserts = raw_total["inserted"] + staging_total["inserted"] + mart_total["inserted"]

    if overall_inserts and not overall_updates and not overall_deletes:
        print(
            "Load type: initial snapshot load "
            f"(inserted={overall_inserts}, updated=0, deleted=0)"
        )
    elif not overall_inserts and not overall_updates and not overall_deletes:
        print("Load type: no changes detected")
    else:
        print(
            "Load type: incremental changes "
            f"(inserted={overall_inserts}, updated={overall_updates}, deleted={overall_deletes})"
        )

    print_cdc_layer("raw", raw_cdc)
    print_cdc_layer("staging", staging_cdc)
    print_cdc_layer("mart", mart_cdc)

    if (
        fact_stats["dropped_missing_customer_refs"]
        or fact_stats["dropped_missing_product_refs"]
        or fact_stats["dropped_missing_channel_refs"]
    ):
        print(
            "fact conformance: "
            f"dropped_missing_customer_refs={fact_stats['dropped_missing_customer_refs']} | "
            f"dropped_missing_product_refs={fact_stats['dropped_missing_product_refs']} | "
            f"dropped_missing_channel_refs={fact_stats['dropped_missing_channel_refs']}"
        )
    else:
        print("fact conformance: all staged orders resolved to mart dimensions")


if __name__ == "__main__":
    main()
