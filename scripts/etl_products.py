from __future__ import annotations

import json

import pandas as pd

from common import REPORTS_DIR, get_postgres_connection, load_dataframe


QUALITY_RESULTS_PATH = REPORTS_DIR / "great_expectations_results.json"


def divider(title: str) -> None:
    print(f"\n{'=' * 18} {title} {'=' * 18}")


def load_quality_context() -> dict:
    if not QUALITY_RESULTS_PATH.exists():
        print(f"Quality results file not found: {QUALITY_RESULTS_PATH}")
        return {}

    payload = json.loads(QUALITY_RESULTS_PATH.read_text(encoding="utf-8"))
    product_results = payload.get("products", [])
    category_rule = next(
        (result for result in product_results if result.get("rule") == "category is valid"),
        None,
    )
    cost_rule = next(
        (result for result in product_results if result.get("rule") == "cost is non-negative"),
        None,
    )

    context = {}

    if category_rule:
        category_result = category_rule.get("result", {})
        context["category_missing_count"] = category_result.get("missing_count", 0)
        print(
            "Reviewed Great Expectations results: "
            f"category is valid | missing_count={context['category_missing_count']}"
        )

    if cost_rule:
        cost_result = cost_rule.get("result", {})
        context["negative_cost_count"] = cost_result.get("unexpected_count", 0)
        context["negative_cost_indexes"] = cost_result.get("partial_unexpected_index_list", [])
        print(
            "Reviewed Great Expectations results: "
            f"cost is non-negative | unexpected_count={context['negative_cost_count']} | "
            f"indexes={context['negative_cost_indexes']}"
        )

    return context


def _bool_to_text(value) -> str | None:
    if pd.isna(value):
        return None
    return "True" if bool(value) else "False"


def transform_products():
    dataframe = load_dataframe("products").copy()

    raw_rows = [
        (
            row.sku,
            None if pd.isna(row.category) else row.category,
            None if pd.isna(row.cost) else str(row.cost),
            _bool_to_text(row.active),
        )
        for row in dataframe.itertuples(index=False)
    ]

    dim_rows = [
        (
            row.sku,
            None if pd.isna(row.category) else row.category,
            None if pd.isna(row.active) else bool(row.active),
        )
        for row in dataframe.itertuples(index=False)
    ]

    return raw_rows, dim_rows


def load_to_postgres(raw_rows: list[tuple], dim_rows: list[tuple]) -> tuple[int, int]:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE raw.products_raw;")
            cursor.executemany(
                """
                INSERT INTO raw.products_raw (
                    sku,
                    category,
                    cost,
                    active
                )
                VALUES (%s, %s, %s, %s);
                """,
                raw_rows,
            )

            cursor.execute("TRUNCATE TABLE mart.dim_product RESTART IDENTITY CASCADE;")
            cursor.executemany(
                """
                INSERT INTO mart.dim_product (
                    sku,
                    category,
                    is_active
                )
                VALUES (%s, %s, %s);
                """,
                dim_rows,
            )

        connection.commit()

    return len(raw_rows), len(dim_rows)


def main() -> None:
    divider("PRODUCTS ETL")
    quality_context = load_quality_context()
    raw_rows, dim_rows = transform_products()
    raw_count, dim_count = load_to_postgres(raw_rows, dim_rows)

    divider("LOAD SUMMARY")
    print(f"Rows loaded into raw.products_raw: {raw_count}")
    print(f"Rows loaded into mart.dim_product: {dim_count}")

    if quality_context:
        print(
            "Quality review kept source issues visible: "
            f"category_missing_count={quality_context.get('category_missing_count', 0)}, "
            f"negative_cost_count={quality_context.get('negative_cost_count', 0)}"
        )


if __name__ == "__main__":
    main()
