# Roseamor Technical Test

## Objective

This repository implements the data-analysis scope from [REQUIREMENTS.md](/home/jpas/Projects/technical-test-roseamor/REQUIREMENTS.md): explore the CSV sources, apply data quality rules, load a PostgreSQL warehouse through `raw -> staging -> mart`, and expose a curated model for BI consumption.

Current implemented deliverables:

- Docker Compose environment with one Python container and one PostgreSQL container
- `.env` for local secrets and `.env.example` for version control
- PostgreSQL bootstrap with `raw`, `staging`, and `mart` schemas
- Console EDA with `ydata-profiling` plus HTML profiling reports
- Console data quality checks with Great Expectations plus JSON validation output
- Incremental ETL with CDC behavior from CSV source to warehouse
- Star schema in `mart` for analytical consumption
- Power BI artifacts in `dashboards/`

Current repository scope is the analytical pipeline and BI layer. The simple web order-registration app described in `REQUIREMENTS.md` is not implemented in this repository yet.

## Architecture

Target flow:

`CSV files -> raw schema -> staging quality layer -> mart star schema -> BI consumption`

Database layers:

- `raw`: landing tables that mirror the source CSV structure with minimal typing
- `staging`: cleaned and standardized tables where quality rules are applied before dimensional modeling
- `mart`: star schema consumed by BI and downstream analytics

## Data Model

Staging tables:

- `staging.customers_clean`
- `staging.products_clean`
- `staging.orders_clean`

Mart tables:

- `mart.dim_customer`
- `mart.dim_channel`
- `mart.dim_product`
- `mart.dim_date`
- `mart.fact_order_line`

Star schema structure:

- `mart.dim_customer(customer_key, customer_id, customer_name, country, segment, customer_created_at)`
- `mart.dim_channel(channel_key, channel_code, channel_name)`
- `mart.dim_product(product_key, sku, category, is_active)`
- `mart.dim_date(date_key, full_date, year, quarter, month, month_text, day, day_text, is_weekend)`
- `mart.fact_order_line(order_line_key, order_id, customer_key, channel_key, product_key, order_date_key, quantity, is_return, unit_price, unit_cost, total_price, total_cost)`

Fact grain:

- `mart.fact_order_line` stores one cleaned order line per business event at the intersection of `order_id`, customer, product, channel, and order date
- Exact duplicate source rows are removed before loading, so the fact grain excludes technical duplicates from `orders.csv`

Date strategy:

- `mart.dim_date` is generated from valid distinct dates present in `staging.orders_clean`
- It is a sparse date dimension, not a full continuous calendar
- `date_key` is generated as `YYYYMMDD`

## Data Quality Strategy

A data quality strategy was applied based on business rules and analytical consistency: in customers, missing country values were imputed as `"Unknown"` to avoid NULLs in dimensions; in products, negative cost values, identified as sign errors in production cost, were corrected using absolute values with traceability; in orders, fully duplicated rows were removed to eliminate technical duplication, and duplicated `order_id` records were removed when they represented exact duplicates of the same event, positive and negative quantity values were preserved to represent sales and returns through an `is_return` flag, rows with null `unit_price` were dropped because revenue could not be computed, and records with invalid `order_date` values were removed after datetime parsing. All these decisions were documented as explicit cleaning and validation rules in the pipeline.

Source findings reviewed during the analysis:

- `customers.csv`: 5 missing `country`, 5 missing `segment`
- `products.csv`: 2 missing `category`, 3 negative `cost` values
- `orders.csv`: 15 exact duplicate rows, 30 duplicated `order_id` occurrences, 10 missing `unit_price`, 6 invalid `order_date` values, 8 negative `quantity` rows

Expected cleaned outcomes:

- `raw` preserves source issues for auditability
- `staging` applies remediation and standardization rules
- `mart` contains only conforming analytical entities and facts

## Project Structure

```text
.
├── dashboards/
│   ├── roseamor_dashboard.pbix
│   └── roseamor_dashboard.pdf
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
├── db/
│   └── init/
│       ├── 00_create_schemas.sql
│       └── 01_star_schema.sql
├── reports/
├── scripts/
│   ├── common.py
│   ├── data_quality.py
│   ├── eda_console.py
│   ├── etl_customers.py
│   ├── etl_incremental_cdc.py
│   ├── etl_orders.py
│   └── etl_products.py
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── README.md
├── REQUIREMENTS.md
└── requirements.txt
```

Main analytical artifacts:

- Dashboard files: [dashboards/roseamor_dashboard.pbix](/home/jpas/Projects/technical-test-roseamor/dashboards/roseamor_dashboard.pbix), [dashboards/roseamor_dashboard.pdf](/home/jpas/Projects/technical-test-roseamor/dashboards/roseamor_dashboard.pdf)
- SQL bootstrap files: [db/init/00_create_schemas.sql](/home/jpas/Projects/technical-test-roseamor/db/init/00_create_schemas.sql), [db/init/01_star_schema.sql](/home/jpas/Projects/technical-test-roseamor/db/init/01_star_schema.sql)
- EDA outputs: `reports/*_profile.html`
- DQ output: [reports/great_expectations_results_db.json](/home/jpas/Projects/technical-test-roseamor/reports/great_expectations_results_db.json)

## Local Configuration

1. Review `.env.example`.
2. Create your local `.env`.
3. Keep `.env` local; it is ignored by git.

Example variables:

```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=roseamor_dw
POSTGRES_USER=roseamor_user
POSTGRES_PASSWORD=change_me
LOCAL_UID=1000
LOCAL_GID=1000
```

## Run The Environment

Build and start the containers:

```bash
docker compose up -d --build
```

Check the service state:

```bash
docker compose ps
```

Available shortcuts:

```bash
make up
make eda
make dq
make etl
make reset-db
make clean-docker
```

What they do:

- `make up`: build and start the containers
- `make eda`: run console EDA against PostgreSQL tables
- `make dq`: run Great Expectations rules against PostgreSQL tables
- `make etl`: run the incremental ETL with CDC behavior
- `make reset-db`: recreate the PostgreSQL database from zero
- `make clean-docker`: remove the project containers, volumes, and local images

Useful access commands:

```bash
docker compose exec python bash
psql -h localhost -p 5432 -U roseamor_user -d roseamor_dw
```

## Run EDA

The EDA command inspects PostgreSQL tables from `raw`, `staging`, and `mart` by default. It prints table structure, missing values, duplicates, numeric summaries, categorical distributions, and generates HTML profiling reports with `ydata-profiling`.

Run all schemas:

```bash
make eda
```

Run selected schemas:

```bash
make eda FLAGS="--raw"
make eda FLAGS="--staging --mart"
```

Direct command:

```bash
docker compose exec python python scripts/eda_console.py --mart
```

Generated reports follow this pattern:

- `reports/raw_<table>_profile.html`
- `reports/staging_<table>_profile.html`
- `reports/mart_<table>_profile.html`

## Run Data Quality Checks

The validation command runs Great Expectations rules over PostgreSQL tables from `raw`, `staging`, and `mart` by default and prints pass/fail results in the terminal.

Run all schemas:

```bash
make dq
```

Run selected schemas:

```bash
make dq FLAGS="--raw"
make dq FLAGS="--mart"
make dq FLAGS="--staging --mart"
```

Direct command:

```bash
docker compose exec python python scripts/data_quality.py --staging --mart
```

Generated artifact:

- [reports/great_expectations_results_db.json](/home/jpas/Projects/technical-test-roseamor/reports/great_expectations_results_db.json)

## Run ETL

This is the only ETL entry point. It executes the complete flow:

`CSV files -> raw -> staging -> mart`

Applied logic by layer:

- `raw`: loads the source files without remediation
- `staging.customers_clean`: trims fields, standardizes IDs, imputes missing `country` and `segment` as `Unknown`, and types `customer_created_at`
- `staging.products_clean`: trims fields, standardizes `sku`, imputes missing `category` as `Unknown`, parses booleans, and converts negative `cost` values with `abs()`
- `staging.orders_clean`: standardizes IDs and channel format, removes exact duplicate rows, drops rows with missing required business keys, drops invalid `unit_price`, drops invalid `order_date`, and keeps negative `quantity` as returns through `is_return`
- `mart`: loads dimensions and fact table only from conformed staging data

CDC behavior:

- first load: inserts the initial snapshot
- later loads: applies `insert`, `update`, and `delete` against the current warehouse state

Run it with:

```bash
make etl
```

Direct command:

```bash
docker compose exec python python scripts/etl_incremental_cdc.py
```

## Refresh Process

If a new CSV snapshot arrives tomorrow:

1. Replace the files in `data/` with the new source snapshot.
2. Run `make etl` to sync `raw`, rebuild cleaned `staging` records, and apply CDC changes to `mart`.
3. Run `make dq` to confirm `raw`, `staging`, and `mart` quality status.
4. Run `make eda` if you need refreshed HTML profiling artifacts.
5. Refresh the Power BI model against PostgreSQL.

This design keeps the original landing layer, isolates cleaning logic in `staging`, and exposes a stable analytical model in `mart`.

## Dashboard

Power BI artifacts included in the repository:

- [dashboards/roseamor_dashboard.pbix](/home/jpas/Projects/technical-test-roseamor/dashboards/roseamor_dashboard.pbix)
- [dashboards/roseamor_dashboard.pdf](/home/jpas/Projects/technical-test-roseamor/dashboards/roseamor_dashboard.pdf)

Expected dashboard consumption layer:

- dimensions from `mart.dim_customer`, `mart.dim_channel`, `mart.dim_product`, `mart.dim_date`
- measures from `mart.fact_order_line`

## Database Bootstrap

PostgreSQL initializes the schemas and tables from:

- [db/init/00_create_schemas.sql](/home/jpas/Projects/technical-test-roseamor/db/init/00_create_schemas.sql)
- [db/init/01_star_schema.sql](/home/jpas/Projects/technical-test-roseamor/db/init/01_star_schema.sql)

These files are mounted in `docker-compose.yml` under `/docker-entrypoint-initdb.d`, so they run automatically only when the PostgreSQL volume is created from scratch.

## Reset The Database

To recreate the PostgreSQL database from zero:

```bash
make reset-db
```

This command removes the database volume and starts PostgreSQL again so the init SQL runs from a clean state.

## Clean Docker Environment

To remove the project Docker containers, volumes, and local images:

```bash
make clean-docker
```
