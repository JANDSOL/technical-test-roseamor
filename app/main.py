from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
import os
from pathlib import Path
import re
import sqlite3

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from scripts.common import load_dataframe
from scripts.etl_incremental_cdc import run_incremental_cdc


APP_DIR = Path(__file__).resolve().parent
SQLITE_DB_PATH = Path(os.getenv("APP_SQLITE_PATH", str(APP_DIR / "orders_app.db")))


@lru_cache
def load_valid_customer_ids() -> frozenset[str]:
    dataframe = load_dataframe("customers")
    values = dataframe["customer_id"].dropna().astype(str).str.strip()
    return frozenset(value for value in values if value)


@lru_cache
def load_valid_skus() -> frozenset[str]:
    dataframe = load_dataframe("products")
    values = dataframe["sku"].dropna().astype(str).str.strip()
    return frozenset(value for value in values if value)


@lru_cache
def load_channel_options() -> tuple[str, ...]:
    dataframe = load_dataframe("orders")
    values = dataframe["channel"].dropna().astype(str).str.strip().str.lower()
    return tuple(sorted(value for value in values.unique() if value))


@lru_cache
def get_source_order_id_max() -> int:
    dataframe = load_dataframe("orders")
    matches = dataframe["order_id"].dropna().astype(str).str.extract(r"^O(\d+)$")[0].dropna()
    if matches.empty:
        return 0
    return int(matches.astype(int).max())


def build_channel_enum() -> type[Enum]:
    members: dict[str, str] = {}
    for index, value in enumerate(load_channel_options(), start=1):
        enum_name = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").upper() or f"OPTION_{index}"
        if enum_name in members:
            enum_name = f"{enum_name}_{index}"
        members[enum_name] = value
    return Enum("ChannelOption", members, type=str)


ChannelOption = build_channel_enum()


class OrderCreateRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "customer_id": "C0186",
                "sku": "SKU0032",
                "quantity": 3,
                "unit_price": 36.58,
                "channel": "wholesale",
            }
        }
    )

    customer_id: str = Field(..., min_length=1, pattern=r"^C\d{4}$")
    sku: str = Field(..., min_length=1, pattern=r"^SKU\d{4}$")
    quantity: Decimal = Field(..., ne=0)
    unit_price: Decimal = Field(..., ge=0)
    channel: ChannelOption

    @field_validator("customer_id", "sku", mode="before")
    @classmethod
    def normalize_identifiers(cls, value: str) -> str:
        return value.strip().upper()


class OrderCreateResponse(BaseModel):
    order_id: str
    customer_id: str
    sku: str
    quantity: Decimal
    unit_price: Decimal
    order_date: datetime
    channel: str
    created_at: datetime


class ChannelListResponse(BaseModel):
    options: list[str]


class RegisteredOrderResponse(OrderCreateResponse):
    app_order_key: int


def get_sqlite_connection() -> sqlite3.Connection:
    SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(SQLITE_DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def ensure_app_table() -> None:
    with get_sqlite_connection() as connection:
        existing_table = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'orders_app';
            """
        ).fetchone()

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_app (
                app_order_key INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL UNIQUE,
                customer_id TEXT NOT NULL,
                sku TEXT NOT NULL,
                quantity NUMERIC NOT NULL CHECK (quantity <> 0),
                unit_price NUMERIC NOT NULL CHECK (unit_price >= 0),
                order_date TEXT NOT NULL,
                channel TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

        if existing_table and "CHECK (quantity > 0)" in existing_table["sql"]:
            connection.execute("ALTER TABLE orders_app RENAME TO orders_app_legacy;")
            connection.execute(
                """
                CREATE TABLE orders_app (
                    app_order_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL UNIQUE,
                    customer_id TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    quantity NUMERIC NOT NULL CHECK (quantity <> 0),
                    unit_price NUMERIC NOT NULL CHECK (unit_price >= 0),
                    order_date TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            connection.execute(
                """
                INSERT INTO orders_app (
                    app_order_key,
                    order_id,
                    customer_id,
                    sku,
                    quantity,
                    unit_price,
                    order_date,
                    channel,
                    created_at
                )
                SELECT
                    app_order_key,
                    order_id,
                    customer_id,
                    sku,
                    quantity,
                    unit_price,
                    order_date,
                    channel,
                    created_at
                FROM orders_app_legacy;
                """
            )
            connection.execute("DROP TABLE orders_app_legacy;")

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_app_order_date
                ON orders_app (order_date);
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_app_customer_id
                ON orders_app (customer_id);
            """
        )
        connection.commit()


def get_next_order_id(connection) -> str:
    cursor = connection.execute(
        """
        SELECT COALESCE(MAX(CAST(SUBSTR(order_id, 2) AS INTEGER)), 0)
        FROM orders_app
        WHERE order_id GLOB 'O[0-9]*';
        """
    )
    database_max = cursor.fetchone()[0] or 0
    next_value = max(get_source_order_id_max(), int(database_max)) + 1
    return f"O{next_value:06d}"


def sqlite_row_to_response(row: sqlite3.Row) -> RegisteredOrderResponse:
    return RegisteredOrderResponse(
        app_order_key=row["app_order_key"],
        order_id=row["order_id"],
        customer_id=row["customer_id"],
        sku=row["sku"],
        quantity=Decimal(str(row["quantity"])),
        unit_price=Decimal(str(row["unit_price"])),
        order_date=datetime.fromisoformat(row["order_date"]),
        channel=row["channel"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def sqlite_row_to_order_source_row(row: sqlite3.Row) -> dict[str, str]:
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "sku": row["sku"],
        "quantity": str(row["quantity"]),
        "unit_price": str(row["unit_price"]),
        "order_date": row["order_date"],
        "channel": row["channel"],
    }


def order_form_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Roseamor Order Registration</title>
  <style>
    :root {
      --bg: #f4efe6;
      --panel: #fffaf2;
      --ink: #1e1b18;
      --accent: #a44524;
      --accent-soft: #ead2c0;
      --border: #d7c6b8;
    }
    body {
      margin: 0;
      font-family: "Avenir Next", "Gill Sans", sans-serif;
      background:
        radial-gradient(circle at top left, #f8d6be 0, transparent 28%),
        linear-gradient(135deg, #f4efe6 0%, #efe4d6 100%);
      color: var(--ink);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .shell {
      width: min(720px, 100%);
      background: rgba(255, 250, 242, 0.96);
      border: 1px solid var(--border);
      border-radius: 20px;
      box-shadow: 0 24px 60px rgba(68, 42, 19, 0.14);
      overflow: hidden;
    }
    .hero {
      padding: 28px 32px 16px;
      border-bottom: 1px solid var(--border);
    }
    .hero h1 {
      margin: 0 0 8px;
      font-size: 2rem;
      letter-spacing: 0.02em;
    }
    .hero p {
      margin: 0;
      line-height: 1.5;
      color: #51473f;
    }
    form {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 18px;
      padding: 24px 32px 12px;
    }
    label {
      display: grid;
      gap: 8px;
      font-size: 0.95rem;
      font-weight: 600;
    }
    input, select, button {
      font: inherit;
    }
    input, select {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      background: white;
    }
    button {
      border: 0;
      border-radius: 999px;
      padding: 14px 18px;
      background: var(--accent);
      color: white;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.16s ease, opacity 0.16s ease;
    }
    button:hover {
      transform: translateY(-1px);
      opacity: 0.96;
    }
    .actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 12px;
      padding-top: 6px;
    }
    .meta {
      padding: 0 32px 32px;
      color: #51473f;
      line-height: 1.5;
    }
    .status {
      margin-top: 18px;
      padding: 14px 16px;
      border-radius: 14px;
      background: var(--accent-soft);
      display: none;
      white-space: pre-wrap;
    }
    .status.error {
      background: #f3d4d0;
    }
    .links {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      padding-top: 10px;
    }
    a {
      color: var(--accent);
      font-weight: 700;
      text-decoration: none;
    }
  </style>
</head>
<body>
  <section class="shell">
    <header class="hero">
      <h1>Register Order</h1>
      <p>Create a new order in SQLite and sync it immediately to PostgreSQL raw, staging, and mart. The API generates the order id automatically, sets the order datetime on the server, and validates customer, sku, quantity, unit price, and channel.</p>
    </header>
    <form id="order-form">
      <label>
        Customer ID
        <input id="customer_id" name="customer_id" placeholder="C0186" required pattern="^C\\d{4}$" />
      </label>
      <label>
        SKU
        <input id="sku" name="sku" placeholder="SKU0032" required pattern="^SKU\\d{4}$" />
      </label>
      <label>
        Quantity
        <input id="quantity" name="quantity" type="number" step="1" required />
      </label>
      <label>
        Unit Price
        <input id="unit_price" name="unit_price" type="number" min="0" step="0.01" required />
      </label>
      <label>
        Channel
        <select id="channel" name="channel" required></select>
      </label>
      <div class="actions">
        <button type="submit">Save Order</button>
      </div>
    </form>
    <div class="meta">
      <div class="links">
        <a href="/docs" target="_blank" rel="noreferrer">Open API Docs</a>
        <a href="/api/channels" target="_blank" rel="noreferrer">Channel Options</a>
        <a href="/api/orders" target="_blank" rel="noreferrer">Recent Orders</a>
      </div>
      <div id="status" class="status"></div>
    </div>
  </section>
  <script>
    const statusBox = document.getElementById("status");
    const channelSelect = document.getElementById("channel");

    async function loadChannels() {
      const response = await fetch("/api/channels");
      const payload = await response.json();
      channelSelect.innerHTML = payload.options
        .map((value) => `<option value="${value}">${value}</option>`)
        .join("");
    }

    document.getElementById("order-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      statusBox.style.display = "none";
      statusBox.classList.remove("error");

      const payload = {
        customer_id: document.getElementById("customer_id").value,
        sku: document.getElementById("sku").value,
        quantity: Number(document.getElementById("quantity").value),
        unit_price: Number(document.getElementById("unit_price").value),
        channel: document.getElementById("channel").value
      };

      const response = await fetch("/api/orders", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const data = await response.json();
      statusBox.style.display = "block";

      if (!response.ok) {
        statusBox.classList.add("error");
        statusBox.textContent = JSON.stringify(data.detail ?? data, null, 2);
        return;
      }

      statusBox.textContent = `Order saved\\norder_id: ${data.order_id}\\norder_date: ${data.order_date}\\nchannel: ${data.channel}`;
      event.target.reset();
      await loadChannels();
    });

    loadChannels();
  </script>
</body>
</html>"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_app_table()
    yield


app = FastAPI(
    title="Roseamor Orders API",
    description="FastAPI app to register orders in SQLite and sync them to PostgreSQL raw, staging, and mart.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
def render_order_form() -> HTMLResponse:
    return HTMLResponse(order_form_html())


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/channels", response_model=ChannelListResponse)
def list_channels() -> ChannelListResponse:
    return ChannelListResponse(options=list(load_channel_options()))


@app.get("/api/orders", response_model=list[RegisteredOrderResponse])
def list_recent_orders(limit: int = 20) -> list[RegisteredOrderResponse]:
    safe_limit = max(1, min(limit, 100))
    ensure_app_table()
    with get_sqlite_connection() as connection:
        rows = connection.execute(
            """
            SELECT app_order_key, order_id, customer_id, sku, quantity, unit_price,
                   order_date, channel, created_at
            FROM orders_app
            ORDER BY app_order_key DESC
            LIMIT ?;
            """,
            (safe_limit,),
        ).fetchall()

    return [sqlite_row_to_response(row) for row in rows]


@app.post("/api/orders", response_model=OrderCreateResponse, status_code=status.HTTP_201_CREATED)
def create_order(payload: OrderCreateRequest) -> OrderCreateResponse:
    ensure_app_table()
    if payload.customer_id not in load_valid_customer_ids():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"customer_id '{payload.customer_id}' does not exist in customers.csv",
        )

    if payload.sku not in load_valid_skus():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"sku '{payload.sku}' does not exist in products.csv",
        )

    order_date = datetime.now().replace(microsecond=0)

    created_at = datetime.now().replace(microsecond=0)

    with get_sqlite_connection() as connection:
        connection.execute("BEGIN IMMEDIATE;")
        order_id = get_next_order_id(connection)
        cursor = connection.execute(
            """
            INSERT INTO orders_app (
                order_id,
                customer_id,
                sku,
                quantity,
                unit_price,
                order_date,
                channel,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING app_order_key, order_id, customer_id, sku, quantity, unit_price, order_date, channel, created_at;
            """,
            (
                order_id,
                payload.customer_id,
                payload.sku,
                str(payload.quantity),
                str(payload.unit_price),
                order_date.isoformat(sep=" "),
                payload.channel.value,
                created_at.isoformat(sep=" "),
            ),
        )
        row = cursor.fetchone()
        try:
            run_incremental_cdc(
                additional_order_rows=[sqlite_row_to_order_source_row(row)],
                verbose=False,
            )
        except Exception as exc:
            connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"order could not be synced to PostgreSQL raw/staging/mart layers: {exc}",
            ) from exc

        connection.commit()

    return OrderCreateResponse(
        order_id=row["order_id"],
        customer_id=row["customer_id"],
        sku=row["sku"],
        quantity=Decimal(str(row["quantity"])),
        unit_price=Decimal(str(row["unit_price"])),
        order_date=datetime.fromisoformat(row["order_date"]),
        channel=row["channel"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )
