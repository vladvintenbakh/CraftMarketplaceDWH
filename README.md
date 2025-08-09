# Craft Market DWH & Customer Datamart

This repo contains pure SQL to build a small **data warehouse (DWH)** and a **customer-facing datamart** for a craft marketplace that aggregates orders from multiple source systems.

## What’s inside

```
de-start-project-sprint-8-2024-main/
  README.md
  scripts/
    DDL_customer_report_datamart.sql
    DDL_load_dates_customer_report_datamart.sql
    SQL_incremental_load_to_datamart.sql
    SQL_load_to_dwh.sql
```

**Schemas & tables used**

- `source1`, `source2`, `source3` — raw source systems (orders, customers, craftsmen, products).
- `external_source` — auxiliary external data.
- `dwh` — warehouse schema:
  - Dimensions: `dwh.d_customer`, `dwh.d_craftsman`, `dwh.d_product`
  - Fact: `dwh.f_order`
  - Service: `dwh.load_dates_customer_report_datamart`
  - Mart: `dwh.customer_report_datamart`

## SQL scripts

- `scripts/SQL_load_to_dwh.sql` — **merge-load** raw sources into `dwh` dimensions and the `f_order` fact (creates staging temps, then `MERGE` into target tables).
- `scripts/DDL_customer_report_datamart.sql` — DDL for the customer report mart.
- `scripts/DDL_load_dates_customer_report_datamart.sql` — DDL for the incremental control table.
- `scripts/SQL_incremental_load_to_datamart.sql` — **incremental** population of the mart based on `load_dates_customer_report_datamart`.

## Quick start

1. **Create schemas (once):**

   ```sql
   CREATE SCHEMA IF NOT EXISTS source1;
   CREATE SCHEMA IF NOT EXISTS source2;
   CREATE SCHEMA IF NOT EXISTS source3;
   CREATE SCHEMA IF NOT EXISTS external_source;
   CREATE SCHEMA IF NOT EXISTS dwh;
   ```

2. **Load/source raw data** into `source1/*`, `source2/*`, `source3/*`, `external_source/*` as per your environment.

3. **Build / refresh the DWH**\
   Run in order:

   - `scripts/SQL_load_to_dwh.sql`

4. **Create the datamart and control table (first time only)**

   - `scripts/DDL_customer_report_datamart.sql`
   - `scripts/DDL_load_dates_customer_report_datamart.sql`

5. **Run the incremental mart load**

   - `scripts/SQL_incremental_load_to_datamart.sql`

### Scheduling

- Schedule step **5** (incremental mart load) as a recurring job (e.g., cron / Airflow / DB job scheduler).
- The `dwh.load_dates_customer_report_datamart` table stores processed ranges; the script updates it each run to support **idempotent** increments.

## Target model (high level)

```
source* → [staging temps] → dwh.d_* (customers, craftsmen, products)
                                  ↘
                                   dwh.f_order  →  dwh.customer_report_datamart
```

**Datamart metrics/attributes:**

- customer profile: id, name, address, birthday, email
- `customer_spend`, `platform_money`
- order stats: `count_order`, `avg_price_order`, `median_time_order_completed`
- `top_product_category`, etc.

> Exact column types are defined in `DDL_customer_report_datamart.sql`.

## Requirements

- PostgreSQL 14+ (tested syntax should work on modern Postgres; adjust `MERGE` if using <15).
- Access to populate the `source*` and `external_source` schemas.

## Development notes

- Scripts are **re-runnable**: DDL uses `DROP IF EXISTS` and loads use `MERGE`.
- If your Postgres version doesn’t support `MERGE`, replace with `INSERT … ON CONFLICT` / `UPDATE` patterns.
- Keep source-system primary keys stable across runs.

--

Maintained as part of a data engineering sprint. PRs welcome.
