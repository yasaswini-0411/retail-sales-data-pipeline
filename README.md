# Retail Sales Data Pipeline | Databricks & SQL

A retail sales data engineering project that ingests CSV records, cleans and validates them, and builds analytics-ready datasets using **Databricks SQL** and the **Medallion Architecture (Bronze → Silver → Gold)**.

## Project overview

The pipeline transforms raw retail sales transactions into curated datasets for understanding sales trends, product performance, regional profitability, and data quality.

### Architecture

```text
Retail sales CSV files
        |
        v
Databricks Volume
/Volumes/retail_sales_sdp/default/sales_volume
        |
        v
BRONZE: bronze_sales (streaming table)
  - Auto Loader / read_files
  - Source file path and ingestion timestamp
  - Order ID expectation
        |
        v
SILVER: silver_sales (materialized view)
  - Trim and standardize text fields
  - Safely cast dates and numeric fields
  - Filter invalid records
  - Deduplicate by OrderID + ProductID
  - Data quality expectations
        |
        v
GOLD: Analytics materialized views
  - Monthly sales trends
  - Product performance
  - Category performance
  - Regional performance
  - Top product rankings
  - Bronze vs. Silver row-count summary
```

## Tech stack

- **Platform:** Databricks
- **Language:** SQL
- **Ingestion:** Databricks Auto Loader (`STREAM read_files`)
- **Storage:** Databricks Volume (CSV source)
- **Processing:** Streaming tables and materialized views
- **Design:** Medallion Architecture, data quality expectations, window functions

## Repository structure

```text
retail_sales_pipeline/
├── retail_Bronze_layer/
│   └── retail_bronze_sales/
│       └── transformations/
│           └── bronze_my_transformation.sql
├── retail_silver_layer/
│   └── retail_silver_sales/
│       └── transformations/
│           └── silver_my_transformation.sql
├── retail_gold_layer/
│   └── gold_retail_sales/
│       └── transformations/
│           └── gold_my_transformation.sql
└── transformations/
    └── my_transformation.py  # Empty placeholder in the supplied export
```

The provided ZIP export also contains a `manifest.mf` file at its root.

## Pipeline layers

### 1. Bronze — Raw ingestion

The `bronze_sales` streaming table reads CSV files from the configured Databricks Volume using `STREAM read_files`. It preserves incoming fields and adds:

- `source_file`: source file path from `_metadata.file_path`
- `ingested_at`: ingestion timestamp

A `data_loaded` expectation checks whether `OrderID` is not null. **This is an `EXPECT` constraint, not a drop-or-fail rule by itself.**

### 2. Silver — Cleaning and validation

The `silver_sales` materialized view:

- Trims string fields and converts dates, numbers, and booleans with `TRY_CAST`.
- Filters missing/blank order IDs, invalid order dates, nonpositive sales or quantities, and missing/invalid profit.
- Deduplicates on `(OrderID, ProductID)`, keeping the record with the most recent `ingested_at` timestamp using `ROW_NUMBER()`.
- Declares expectations for valid order IDs, positive sales, and positive quantities.

> **Note:** Deduplication uses the latest ingestion timestamp, so records with equal timestamps may require an additional deterministic tie-breaker. The `EXPECT` declarations monitor quality; the `WHERE` clause is what filters records in the current Silver query.

### 3. Gold — Business reporting

The Gold SQL creates six materialized views:

| View | Purpose |
|---|---|
| `gold_monthly_sales_trends` | Monthly order counts, revenue, profit, average sales, and quantity |
| `gold_product_performance` | Product-level sales, quantity, profit, ratings, and margin |
| `gold_sales_by_category` | Category/subcategory sales, profit, quantity, and margin |
| `gold_sales_by_region` | Regional/state sales, profit, unique customers, and margin |
| `gold_top_products` | Product rankings by total sales and total profit |
| `gold_data_quality_summary` | Compares Bronze and Silver row counts |

Profit margin is calculated as `SUM(profit) / NULLIF(SUM(sales), 0) * 100`, rounded to two decimal places.

## How to run

1. Create or select a Databricks workspace and SQL pipeline environment that supports `CREATE OR REFRESH STREAMING TABLE` and `CREATE OR REFRESH MATERIALIZED VIEW`.
2. Create the source Volume, or update the path in `bronze_my_transformation.sql` to point to your own Volume.
3. Upload retail sales CSV files with column headers matching the fields referenced in the SQL scripts (for example, `OrderID`, `OrderDate`, `ProductID`, `Sales`, `Quantity`, and `Profit`).
4. Configure the pipeline to include the SQL transformation files, with tables in the same catalog/schema or appropriate fully qualified references.
5. Run the pipeline and verify that `bronze_sales` and `silver_sales` are created before querying the Gold outputs.
6. Inspect data quality metrics and query the Gold views for business insights.

**Important:** The provided archive includes transformation definitions, but not the input CSV dataset, pipeline configuration, or execution screenshots. You must supply your own data and configure the pipeline before reproducing the results. No measured performance or business outcomes are claimed here.

## Example queries

```sql
-- Monthly revenue and profit
SELECT *
FROM gold_monthly_sales_trends
ORDER BY order_year, order_month;

-- Top 10 products by sales
SELECT product_name, total_sales, total_profit, sales_rank
FROM gold_top_products
ORDER BY sales_rank
LIMIT 10;

-- Regional sales performance
SELECT region, state, total_sales, total_profit, profit_margin_pct
FROM gold_sales_by_region
ORDER BY total_sales DESC;

-- Raw vs. cleaned record counts
SELECT * FROM gold_data_quality_summary;
```

## Skills demonstrated

- Designing a Bronze–Silver–Gold data pipeline
- Incremental file ingestion with Databricks Auto Loader
- Data cleaning, validation, and deduplication using SQL
- SQL aggregation, ranking, and window functions
- Building reusable analytics-ready materialized views

## Possible future enhancements

- Add sample or synthetic CSV data and schema documentation.
- Include pipeline configuration and deployment instructions.
- Add automated data quality tests and monitoring dashboards.
- Add BI dashboards for sales and profitability reporting.
- Implement deterministic tie-breaking and more comprehensive data validation.

## License

No license is included in the supplied project export. Add a `LICENSE` file if you want to define reuse permissions.
