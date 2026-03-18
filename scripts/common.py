from __future__ import annotations

from pathlib import Path
import os

import pandas as pd
import psycopg
from psycopg import sql


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
REPORTS_DIR = ROOT_DIR / "reports"

DATASETS = {
    "customers": {
        "path": DATA_DIR / "customers.csv",
        "date_columns": ["created_at"],
    },
    "products": {
        "path": DATA_DIR / "products.csv",
        "date_columns": [],
    },
    "orders": {
        "path": DATA_DIR / "orders.csv",
        "date_columns": ["order_date"],
    },
}


def load_dataframe(name: str) -> pd.DataFrame:
    config = DATASETS[name]
    dataframe = pd.read_csv(config["path"])

    if name == "customers":
        dataframe["customer_id"] = dataframe["customer_id"].astype("string")
        dataframe["name"] = dataframe["name"].astype("string")
        dataframe["country"] = dataframe["country"].astype("string")
        dataframe["segment"] = dataframe["segment"].astype("string")
    elif name == "products":
        dataframe["sku"] = dataframe["sku"].astype("string")
        dataframe["category"] = dataframe["category"].astype("string")
        dataframe["cost"] = pd.to_numeric(dataframe["cost"], errors="coerce")
        dataframe["active"] = dataframe["active"].map(
            {"True": True, "False": False, True: True, False: False}
        )
    elif name == "orders":
        dataframe["order_id"] = dataframe["order_id"].astype("string")
        dataframe["customer_id"] = dataframe["customer_id"].astype("string")
        dataframe["sku"] = dataframe["sku"].astype("string")
        dataframe["quantity"] = pd.to_numeric(dataframe["quantity"], errors="coerce")
        dataframe["unit_price"] = pd.to_numeric(dataframe["unit_price"], errors="coerce")
        dataframe["channel"] = dataframe["channel"].astype("string")

    for column in config["date_columns"]:
        dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

    return dataframe


def ensure_reports_dir() -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    return REPORTS_DIR


def get_postgres_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "roseamor_dw"),
        user=os.getenv("POSTGRES_USER", "roseamor_user"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def list_schema_tables(connection, schemas: list[str]) -> list[tuple[str, str]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema = ANY(%s)
            ORDER BY table_schema, table_name;
            """,
            (schemas,),
        )
        return cursor.fetchall()


def load_table_dataframe(connection, schema: str, table: str) -> pd.DataFrame:
    query = sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column.name for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)
