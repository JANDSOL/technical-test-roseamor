from __future__ import annotations

import json

import pandas as pd

from common import REPORTS_DIR, get_postgres_connection


QUALITY_RESULTS_PATH = REPORTS_DIR / "great_expectations_results.json"

CHANNEL_NAME_MAP = {
    "ecommerce": "E-commerce",
    "retail": "Retail",
    "wholesale": "Wholesale",
    "export": "Export",
}


def divider(title: str) -> None:
    print(f"\n{'=' * 18} {title} {'=' * 18}")


def normalize_nullable(value):
    if pd.isna(value):
        return None
    return value


def load_quality_context() -> dict:
    if not QUALITY_RESULTS_PATH.exists():
        print(f"Quality results file not found: {QUALITY_RESULTS_PATH}")
        return {}

    payload = json.loads(QUALITY_RESULTS_PATH.read_text(encoding="utf-8"))
    order_results = {result["rule"]: result for result in payload.get("orders", [])}
    product_results = {result["rule"]: result for result in payload.get("products", [])}

    context = {
        "negative_quantity_count": order_results.get("quantity is greater than zero", {}).get("result", {}).get("unexpected_count", 0),
        "missing_unit_price_count": order_results.get("unit_price is numeric", {}).get("result", {}).get("unexpected_count", 0),
        "invalid_order_date_count": order_results.get("order_date is valid datetime", {}).get("result", {}).get("unexpected_count", 0),
        "unexpected_channel_count": order_results.get("channel is valid", {}).get("result", {}).get("unexpected_count", 0),
        "negative_product_cost_count": product_results.get("cost is non-negative", {}).get("result", {}).get("unexpected_count", 0),
    }

    print(
        "Reviewed Great Expectations results: "
        f"negative_quantity_count={context['negative_quantity_count']} | "
        f"missing_unit_price_count={context['missing_unit_price_count']} | "
        f"invalid_order_date_count={context['invalid_order_date_count']} | "
        f"unexpected_channel_count={context['unexpected_channel_count']} | "
        f"negative_product_cost_count={context['negative_product_cost_count']}"
    )
    return context


def load_sources():
    orders = pd.read_csv("data/orders.csv", dtype="string")
    products = pd.read_csv("data/products.csv", dtype="string")
    products["cost_numeric"] = pd.to_numeric(products["cost"], errors="coerce")
    products["active_bool"] = products["active"].map({"True": True, "False": False, True: True, False: False})
    return orders, products


def deduplicate_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    deduped = orders.drop_duplicates(
        subset=["order_id", "customer_id", "sku", "quantity", "unit_price", "order_date", "channel"],
        keep="first",
    ).copy()
    removed_count = len(orders) - len(deduped)
    return deduped, removed_count


def build_raw_rows(orders_deduped: pd.DataFrame) -> list[tuple]:
    return [
        (
            normalize_nullable(row.order_id),
            normalize_nullable(row.customer_id),
            normalize_nullable(row.sku),
            normalize_nullable(row.quantity),
            normalize_nullable(row.unit_price),
            normalize_nullable(row.order_date),
            normalize_nullable(row.channel),
        )
        for row in orders_deduped.itertuples(index=False)
    ]


def build_clean_orders(orders_deduped: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    cleaned = orders_deduped.copy()
    cleaned["unit_price_numeric"] = pd.to_numeric(cleaned["unit_price"], errors="coerce")
    cleaned["quantity_numeric"] = pd.to_numeric(cleaned["quantity"], errors="coerce")
    cleaned["order_date_parsed"] = pd.to_datetime(cleaned["order_date"], errors="coerce")

    before_drop = len(cleaned)
    cleaned = cleaned[cleaned["unit_price_numeric"].notna()].copy()
    dropped_missing_unit_price = before_drop - len(cleaned)

    before_drop = len(cleaned)
    cleaned = cleaned[cleaned["order_date_parsed"].notna()].copy()
    dropped_invalid_order_date = before_drop - len(cleaned)

    cleaned["is_return"] = cleaned["quantity_numeric"] < 0
    return cleaned, dropped_missing_unit_price, dropped_invalid_order_date


def build_channel_rows(orders_deduped: pd.DataFrame) -> list[tuple]:
    codes = sorted(orders_deduped["channel"].dropna().unique().tolist())
    return [(code, CHANNEL_NAME_MAP.get(code, code.replace("_", " ").title())) for code in codes]


def build_date_rows(clean_orders: pd.DataFrame) -> list[tuple]:
    dates = sorted(clean_orders["order_date_parsed"].dt.normalize().drop_duplicates().tolist())
    rows = []
    for full_date in dates:
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


def fetch_product_cost_map(products: pd.DataFrame) -> dict[str, float | None]:
    cost_map: dict[str, float | None] = {}
    for row in products.itertuples(index=False):
        if pd.isna(row.cost_numeric):
            cost_map[row.sku] = None
        else:
            cost_map[row.sku] = float(abs(row.cost_numeric))
    return cost_map


def fetch_negative_cost_skus(products: pd.DataFrame) -> set[str]:
    negative_cost_rows = products[products["cost_numeric"] < 0]
    return set(negative_cost_rows["sku"].dropna().tolist())


def build_fact_rows(
    clean_orders: pd.DataFrame,
    customer_map: dict[str, int],
    product_map: dict[str, int],
    channel_map: dict[str, int],
    product_cost_map: dict[str, float | None],
    negative_cost_skus: set[str],
) -> tuple[list[tuple], int]:
    missing_customers = sorted(set(clean_orders["customer_id"]) - set(customer_map))
    missing_products = sorted(set(clean_orders["sku"]) - set(product_map))
    missing_channels = sorted(set(clean_orders["channel"]) - set(channel_map))

    if missing_customers or missing_products or missing_channels:
        raise RuntimeError(
            "Missing dimension keys for fact load: "
            f"customers={missing_customers[:5]}, "
            f"products={missing_products[:5]}, "
            f"channels={missing_channels[:5]}"
        )

    fact_rows: list[tuple] = []
    abs_cost_count = 0

    for row in clean_orders.itertuples(index=False):
        quantity = float(row.quantity_numeric)
        unit_price = float(row.unit_price_numeric)
        unit_cost = product_cost_map.get(row.sku)
        if unit_cost is None:
            total_cost = None
        else:
            total_cost = quantity * unit_cost
            if row.sku in negative_cost_skus:
                abs_cost_count += 1

        fact_rows.append(
            (
                row.order_id,
                customer_map[row.customer_id],
                channel_map[row.channel],
                product_map[row.sku],
                int(row.order_date_parsed.strftime("%Y%m%d")),
                quantity,
                bool(row.is_return),
                unit_price,
                unit_cost,
                quantity * unit_price,
                total_cost,
            )
        )

    return fact_rows, abs_cost_count


def load_to_postgres(
    raw_rows: list[tuple],
    channel_rows: list[tuple],
    date_rows: list[tuple],
    clean_orders: pd.DataFrame,
    products: pd.DataFrame,
) -> tuple[int, int]:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE raw.orders_raw;")
            cursor.executemany(
                """
                INSERT INTO raw.orders_raw (
                    order_id,
                    customer_id,
                    sku,
                    quantity,
                    unit_price,
                    order_date,
                    channel
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                raw_rows,
            )

            cursor.execute("TRUNCATE TABLE mart.fact_order_line RESTART IDENTITY;")
            cursor.execute("TRUNCATE TABLE mart.dim_channel RESTART IDENTITY CASCADE;")
            cursor.execute("TRUNCATE TABLE mart.dim_date CASCADE;")

            cursor.executemany(
                """
                INSERT INTO mart.dim_channel (
                    channel_code,
                    channel_name
                )
                VALUES (%s, %s);
                """,
                channel_rows,
            )

            cursor.executemany(
                """
                INSERT INTO mart.dim_date (
                    date_key,
                    full_date,
                    year,
                    quarter,
                    month,
                    month_text,
                    day,
                    day_text,
                    is_weekend
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                date_rows,
            )

            customer_map, product_map, channel_map = fetch_dimension_maps(connection)

        product_cost_map = fetch_product_cost_map(products)
        negative_cost_skus = fetch_negative_cost_skus(products)
        fact_rows_final, abs_cost_count = build_fact_rows(
            clean_orders,
            customer_map,
            product_map,
            channel_map,
            product_cost_map,
            negative_cost_skus,
        )

        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO mart.fact_order_line (
                    order_id,
                    customer_key,
                    channel_key,
                    product_key,
                    order_date_key,
                    quantity,
                    is_return,
                    unit_price,
                    unit_cost,
                    total_price,
                    total_cost
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                fact_rows_final,
            )

        connection.commit()

    return abs_cost_count, len(fact_rows_final)


def main() -> None:
    divider("ORDERS ETL")
    quality_context = load_quality_context()
    orders_source, products_source = load_sources()
    orders_deduped, duplicate_rows_removed = deduplicate_orders(orders_source)
    raw_rows = build_raw_rows(orders_deduped)
    clean_orders, dropped_missing_unit_price, dropped_invalid_order_date = build_clean_orders(orders_deduped)
    channel_rows = build_channel_rows(orders_deduped)
    date_rows = build_date_rows(clean_orders)

    abs_cost_count, fact_row_count = load_to_postgres(
        raw_rows=raw_rows,
        channel_rows=channel_rows,
        date_rows=date_rows,
        clean_orders=clean_orders,
        products=products_source,
    )

    divider("LOAD SUMMARY")
    print(f"Raw rows loaded into raw.orders_raw: {len(raw_rows)}")
    print(f"Duplicate raw rows removed: {duplicate_rows_removed}")
    print(f"Rows dropped for missing unit_price: {dropped_missing_unit_price}")
    print(f"Rows dropped for invalid order_date: {dropped_invalid_order_date}")
    print(f"Rows loaded into mart.dim_channel: {len(channel_rows)}")
    print(f"Rows loaded into mart.dim_date: {len(date_rows)}")
    print(f"Rows loaded into mart.fact_order_line: {fact_row_count}")
    print(f"Fact rows where negative product cost was converted with abs(): {abs_cost_count}")
    print(f"Return rows kept in fact (negative quantity): {int(clean_orders['is_return'].sum())}")

    if quality_context:
        print(
            "Great Expectations review matched ETL filters: "
            f"missing_unit_price_count={quality_context['missing_unit_price_count']}, "
            f"invalid_order_date_count={quality_context['invalid_order_date_count']}, "
            f"negative_quantity_count={quality_context['negative_quantity_count']}, "
            f"negative_product_cost_count={quality_context['negative_product_cost_count']}"
        )


if __name__ == "__main__":
    main()
