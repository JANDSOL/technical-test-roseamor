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
    customer_results = payload.get("customers", [])
    country_rule = next(
        (result for result in customer_results if result.get("rule") == "country is not null"),
        None,
    )

    if not country_rule:
        print("No customer country rule found in Great Expectations results.")
        return {}

    result_details = country_rule.get("result", {})
    context = {
        "rule": country_rule.get("rule"),
        "success": country_rule.get("success"),
        "unexpected_count": result_details.get("unexpected_count", 0),
        "unexpected_indexes": result_details.get("partial_unexpected_index_list", []),
    }
    print(
        "Reviewed Great Expectations results: "
        f"{context['rule']} | success={context['success']} | "
        f"unexpected_count={context['unexpected_count']} | "
        f"indexes={context['unexpected_indexes']}"
    )
    return context


def transform_customers():
    dataframe = load_dataframe("customers").copy()
    null_country_mask = dataframe["country"].isna()
    remediated_count = int(null_country_mask.sum())
    dataframe.loc[null_country_mask, "country"] = "Unknown"

    raw_rows = [
        (
            row.customer_id,
            row.name,
            row.country,
            None if pd.isna(row.segment) else row.segment,
            None if pd.isna(row.created_at) else row.created_at.isoformat(sep=" "),
        )
        for row in dataframe.itertuples(index=False)
    ]

    dim_rows = [
        (
            row.customer_id,
            row.name,
            row.country,
            None if pd.isna(row.segment) else row.segment,
            None if pd.isna(row.created_at) else row.created_at.to_pydatetime(),
        )
        for row in dataframe.itertuples(index=False)
    ]

    return raw_rows, dim_rows, remediated_count


def load_to_postgres(raw_rows: list[tuple], dim_rows: list[tuple]) -> tuple[int, int]:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE raw.customers_raw;")
            cursor.executemany(
                """
                INSERT INTO raw.customers_raw (
                    customer_id,
                    name,
                    country,
                    segment,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s);
                """,
                raw_rows,
            )

            cursor.execute("TRUNCATE TABLE mart.dim_customer RESTART IDENTITY CASCADE;")
            cursor.executemany(
                """
                INSERT INTO mart.dim_customer (
                    customer_id,
                    customer_name,
                    country,
                    segment,
                    customer_created_at
                )
                VALUES (%s, %s, %s, %s, %s);
                """,
                dim_rows,
            )

        connection.commit()

    return len(raw_rows), len(dim_rows)


def main() -> None:
    divider("CUSTOMERS ETL")
    quality_context = load_quality_context()
    raw_rows, dim_rows, remediated_count = transform_customers()
    raw_count, dim_count = load_to_postgres(raw_rows, dim_rows)

    divider("LOAD SUMMARY")
    print(f"Rows loaded into raw.customers_raw: {raw_count}")
    print(f"Rows loaded into mart.dim_customer: {dim_count}")
    print(f"Countries remediated to 'Unknown': {remediated_count}")

    if quality_context:
        print(
            "Great Expectations review matched ETL remediation: "
            f"unexpected_count={quality_context['unexpected_count']}, "
            f"remediated_count={remediated_count}"
        )


if __name__ == "__main__":
    main()
