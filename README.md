# Retail Analytics — Snowflake + dbt Project

End-to-end data engineering project built on Snowflake and dbt Core.

## Architecture

S3 / Internal Stage
↓
COPY INTO
↓
RAW (Bronze) → STAGING (Silver) → MARTS (Gold)
↓
dbt models
↓
Tableau / BI

## Tech Stack

- **Snowflake** — Cloud data warehouse
- **dbt Core** — Transformations and testing
- **GitHub Actions** — CI/CD pipeline
- **Python** — dbt environment

## Project Structure

retail_project/
├── models/
│   ├── staging/          # Silver layer — 5 views
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_inventory.sql
│   │   ├── stg_returns.sql
│   │   └── schema.yml
│   └── marts/            # Gold layer — 4 tables
│       ├── fct_orders.sql
│       ├── dim_customer.sql
│       ├── dim_product.sql
│       ├── mart_revenue_summary.sql
│       └── schema.yml
├── tests/
│   └── assert_no_negative_revenue.sql
├── macros/
│   └── generate_schema_name.sql
├── snapshots/
└── dbt_project.yml

## Data Model

| Layer | Objects | Materialization |
|---|---|---|
| Bronze | RAW_ORDERS, RAW_CUSTOMERS, RAW_PRODUCTS, RAW_INVENTORY, RAW_RETURNS | Raw tables |
| Silver | stg_orders, stg_customers, stg_products, stg_inventory, stg_returns | Views |
| Gold | fct_orders, dim_customer, dim_product, mart_revenue_summary | Tables |

## dbt Tests

- 49 data tests across all models
- unique, not_null, accepted_values, relationships
- Custom singular test for negative revenue

## Setup

1. Clone the repo
2. Install dbt: `pip install dbt-snowflake`
3. Configure `~/.dbt/profiles.yml` with your Snowflake credentials
4. Run `dbt debug` to test connection
5. Run `dbt build` to run all models and tests

## Author

Harigaran S — Data Engineer at TCS