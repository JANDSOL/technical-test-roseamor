CREATE TABLE IF NOT EXISTS raw.customers_raw (
    customer_id TEXT,
    name TEXT,
    country TEXT,
    segment TEXT,
    created_at TEXT,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.products_raw (
    sku TEXT,
    category TEXT,
    cost TEXT,
    active TEXT,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.orders_raw (
    order_id TEXT,
    customer_id TEXT,
    sku TEXT,
    quantity TEXT,
    unit_price TEXT,
    order_date TEXT,
    channel TEXT,
    source_occurrence INTEGER NOT NULL DEFAULT 1,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.orders_app_raw (
    order_id TEXT,
    customer_id TEXT,
    sku TEXT,
    quantity TEXT,
    unit_price TEXT,
    order_date TEXT,
    channel TEXT,
    source_occurrence INTEGER NOT NULL DEFAULT 1,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS staging.orders_clean CASCADE;
DROP TABLE IF EXISTS staging.products_clean CASCADE;
DROP TABLE IF EXISTS staging.customers_clean CASCADE;
DROP TABLE IF EXISTS mart.fact_orders CASCADE;
DROP TABLE IF EXISTS mart.fact_order_line CASCADE;
DROP TABLE IF EXISTS mart.dim_channel CASCADE;
DROP TABLE IF EXISTS mart.dim_product CASCADE;
DROP TABLE IF EXISTS mart.dim_customer CASCADE;
DROP TABLE IF EXISTS mart.dim_date CASCADE;

CREATE TABLE staging.customers_clean (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    country TEXT NOT NULL,
    segment TEXT NOT NULL,
    customer_created_at TIMESTAMP,
    country_imputed BOOLEAN NOT NULL DEFAULT FALSE,
    segment_imputed BOOLEAN NOT NULL DEFAULT FALSE,
    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.products_clean (
    sku TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    unit_cost NUMERIC(12, 2),
    is_active BOOLEAN,
    category_imputed BOOLEAN NOT NULL DEFAULT FALSE,
    cost_sign_corrected BOOLEAN NOT NULL DEFAULT FALSE,
    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.orders_clean (
    staging_order_line_id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity NUMERIC(12, 2) NOT NULL CHECK (quantity <> 0),
    is_return BOOLEAN NOT NULL DEFAULT FALSE,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    order_date TIMESTAMP NOT NULL,
    channel TEXT NOT NULL,
    source_system_code TEXT NOT NULL,
    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mart.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    customer_name TEXT NOT NULL,
    country TEXT,
    segment TEXT,
    customer_created_at TIMESTAMP
);

CREATE TABLE mart.dim_channel (
    channel_key BIGSERIAL PRIMARY KEY,
    channel_code TEXT NOT NULL UNIQUE,
    channel_name TEXT NOT NULL
);

CREATE TABLE mart.dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    category TEXT,
    is_active BOOLEAN
);

CREATE TABLE mart.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_text TEXT NOT NULL,
    day SMALLINT NOT NULL,
    day_text TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE mart.fact_order_line (
    order_line_key BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    customer_key BIGINT NOT NULL REFERENCES mart.dim_customer(customer_key),
    channel_key BIGINT NOT NULL REFERENCES mart.dim_channel(channel_key),
    product_key BIGINT NOT NULL REFERENCES mart.dim_product(product_key),
    order_date_key INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    quantity NUMERIC(12, 2) NOT NULL,
    is_return BOOLEAN NOT NULL DEFAULT FALSE,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    unit_cost NUMERIC(12, 2) CHECK (unit_cost >= 0),
    total_price NUMERIC(14, 2),
    total_cost NUMERIC(14, 2)
);

CREATE INDEX IF NOT EXISTS idx_fact_order_line_customer_key
    ON mart.fact_order_line (customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_order_line_channel_key
    ON mart.fact_order_line (channel_key);

CREATE INDEX IF NOT EXISTS idx_fact_order_line_product_key
    ON mart.fact_order_line (product_key);

CREATE INDEX IF NOT EXISTS idx_fact_order_line_order_date_key
    ON mart.fact_order_line (order_date_key);

CREATE INDEX IF NOT EXISTS idx_staging_orders_clean_customer_id
    ON staging.orders_clean (customer_id);

CREATE INDEX IF NOT EXISTS idx_staging_orders_clean_sku
    ON staging.orders_clean (sku);

CREATE INDEX IF NOT EXISTS idx_staging_orders_clean_order_date
    ON staging.orders_clean (order_date);
