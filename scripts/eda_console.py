from __future__ import annotations

import argparse
import json
import os

os.environ.setdefault("TQDM_DISABLE", "1")

import pandas as pd
from ydata_profiling import ProfileReport

from common import ensure_reports_dir, get_postgres_connection, list_schema_tables, load_table_dataframe


DEFAULT_SCHEMAS = ["raw", "staging", "mart"]


def divider(title: str) -> None:
    print(f"\n{'=' * 24} {title} {'=' * 24}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run EDA over PostgreSQL warehouse tables.")
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


def format_ratio(part: int, total: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{(part / total) * 100:.2f}%"


def safe_profile_metadata(profile: ProfileReport) -> dict:
    try:
        return json.loads(profile.to_json())
    except Exception:
        return {}


def print_value_counts(dataframe: pd.DataFrame, column: str) -> None:
    series = dataframe[column].dropna()
    if series.empty:
        print(f"- {column}: no non-null values available")
        return

    top_values = series.astype("string").value_counts().head(5)
    values = ", ".join(f"{index} ({value})" for index, value in top_values.items())
    print(f"- {column}: {values}")


def profile_table(connection, schema: str, table: str) -> None:
    dataframe = load_table_dataframe(connection, schema, table)
    reports_dir = ensure_reports_dir()
    report_path = reports_dir / f"{schema}_{table}_profile.html"

    divider(f"{schema.upper()}.{table.upper()}")
    print(f"Table: {schema}.{table}")
    print(f"Rows: {len(dataframe)}")
    print(f"Columns: {len(dataframe.columns)}")

    if dataframe.empty:
        print("Table is empty. Skipping profile report.")
        return

    print(f"Duplicate rows: {int(dataframe.duplicated().sum())}")

    missing_series = dataframe.isna().sum().sort_values(ascending=False)
    total_missing = int(missing_series.sum())
    print(f"Missing cells: {total_missing} ({format_ratio(total_missing, dataframe.size)})")
    print("Column dtypes:")
    for column, dtype in dataframe.dtypes.items():
        print(f"- {column}: {dtype}")

    print("Missing values by column:")
    for column, missing_count in missing_series.items():
        print(f"- {column}: {int(missing_count)}")

    numeric_columns = dataframe.select_dtypes(include=["number"]).columns.tolist()
    if numeric_columns:
        print("Numeric summary:")
        numeric_summary = dataframe[numeric_columns].describe().transpose()[["mean", "std", "min", "max"]]
        for column, values in numeric_summary.iterrows():
            print(
                f"- {column}: mean={values['mean']:.2f}, std={values['std']:.2f}, "
                f"min={values['min']:.2f}, max={values['max']:.2f}"
            )

    datetime_columns = dataframe.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns.tolist()
    if datetime_columns:
        print("Datetime ranges:")
        for column in datetime_columns:
            non_null = dataframe[column].dropna()
            if non_null.empty:
                print(f"- {column}: no valid datetimes")
                continue
            print(f"- {column}: min={non_null.min()}, max={non_null.max()}")

    candidate_categorical = [
        column
        for column in dataframe.columns
        if dataframe[column].dtype == "string" or dataframe[column].dtype == "object" or dataframe[column].dtype == "bool"
    ]
    if candidate_categorical:
        print("Top categorical values:")
        for column in candidate_categorical:
            print_value_counts(dataframe, column)

    profile = ProfileReport(
        dataframe,
        title=f"{schema}.{table} profiling report",
        minimal=True,
        progress_bar=False,
    )
    profile.to_file(report_path)
    metadata = safe_profile_metadata(profile)

    alerts = metadata.get("alerts", [])
    if alerts:
        print("Profiling alerts:")
        for alert in alerts[:5]:
            print(f"- {alert}")

    print(f"HTML report generated: {report_path}")


def main() -> None:
    args = parse_args()
    schemas = selected_schemas(args)

    divider("EDA WITH YDATA PROFILING")
    print(f"Inspecting schemas: {', '.join(schemas)}")

    with get_postgres_connection() as connection:
        schema_order = {schema: index for index, schema in enumerate(schemas)}
        tables = sorted(
            list_schema_tables(connection, schemas),
            key=lambda item: (schema_order.get(item[0], len(schema_order)), item[1]),
        )
        if not tables:
            print("No tables found for the selected schemas.")
            return

        for schema, table in tables:
            profile_table(connection, schema, table)


if __name__ == "__main__":
    main()
