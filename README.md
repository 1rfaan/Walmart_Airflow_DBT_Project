# Walmart Data Engineering Pipeline

![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

> A daily, Airflow-orchestrated CDC pipeline that moves Walmart retail data from PostgreSQL and S3 through Databricks Bronze, Silver and Gold layers, with dbt transformations, snapshots and data quality tests.

The pipeline ingests data from operational sources, processes incremental changes, transforms data through analytical layers, and validates the results before they are made available for reporting.

## Table of Contents

1. [Architecture](#architecture)
2. [Data Sources](#data-sources)
3. [Change Data Capture](#change-data-capture)
4. [File Ingestion](#file-ingestion)
5. [Databricks Processing](#databricks-processing)
6. [dbt Transformation Layers](#dbt-transformation-layers)
7. [Data Quality Checks](#data-quality-checks)
8. [Airflow Orchestration](#airflow-orchestration)
9. [Pipeline Runtime](#pipeline-runtime)
10. [Technology Responsibilities](#technology-responsibilities)

## Architecture

```mermaid
flowchart TD
    PG[(PostgreSQL<br/>Operational DB)] -->|CDC| CDC[Databricks CDC Job]
    S3[(AWS S3<br/>Raw Files)] --> ING[File Ingestion]
    CDC --> BRONZE
    ING --> BRONZE

    subgraph DBX[Databricks - Unity Catalog: walmart]
        BRONZE[Bronze<br/>Raw source tables]
    end

    subgraph DBT[dbt Transformations]
        SILVER_T[Silver Technical<br/>Incremental clean models]
        SILVER_B[Silver Business<br/>One Big Table: obt_b]
        TESTS{Tests and<br/>Quality Checks}
        GOLD[Gold<br/>Dimension snapshots + fact_orders]
    end

    BRONZE -->|source freshness check| SILVER_T
    SILVER_T --> SILVER_B
    SILVER_B --> TESTS
    TESTS --> GOLD
    GOLD --> BI[Analytics and Reporting]

    AF[[Apache Airflow<br/>Daily 11:00 AM]] -.->|orchestrates| CDC
    AF -.->|orchestrates| DBT
```

Apache Airflow orchestrates and monitors every stage of this flow.

## Data Sources

| Source | Role |
|---|---|
| PostgreSQL (operational DB) | Stores operational data and emits change data through CDC |
| AWS S3 | Stores raw files that are processed into Databricks |

> The exact ingestion implementations for these sources are external to this repository and run inside Databricks.

## Change Data Capture

Changes in PostgreSQL are captured using CDC. Instead of repeatedly loading entire tables, the pipeline processes only inserted or updated records.

An existing Databricks job handles CDC ingestion and publishes the resulting records into Databricks tables. The Airflow DAG triggers this job using the Databricks SDK and waits for it to finish before continuing.

## File Ingestion

Data stored in AWS S3 is read by a Databricks ingestion process. Raw files are loaded into the **Bronze** layer, where they are available for validation and transformation. S3 ingestion may run as part of the Databricks job triggered by Airflow.

## Databricks Processing

Databricks performs ingestion and incremental processing, and maintains the data in Unity Catalog:

```text
Catalog: walmart
Bronze:   Raw source tables
Silver:   Cleaned and joined tables
Gold:     Dimensional and fact models
```

Source tables: `orders`, `customers`, `products`, `order_items`, `stores`, `employees`.

## dbt Transformation Layers

dbt connects to Databricks and transforms the data using SQL models.

### Bronze

Raw source tables:

```text
walmart.bronze.orders
walmart.bronze.customers
walmart.bronze.products
walmart.bronze.order_items
walmart.bronze.stores
walmart.bronze.employees
```

dbt source freshness is checked before downstream transformations begin.

### Silver Technical

Incrementally cleans and standardizes the source tables.

Models: `customers_t`, `employees_t`, `order_items_t`, `orders_t`, `products_t`, `stores_t`.

Most models use incremental loading and only update records newer than those already in the target table:

```sql
{% if is_incremental() %}
WHERE updated_timestamp >
    (
        SELECT COALESCE(MAX(updated_timestamp), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}
```

Models are stored as tables and use `processed_at` columns to record when each row was transformed.

### Silver Business

The `obt_b` model joins the technical models into a denormalized **One Big Table**:

- Orders
- Customers
- Order items
- Products
- Employees
- Stores

This lets downstream analytics avoid complex joins.

### Gold

The Gold layer contains analytical models:

- Ephemeral order, customer, employee, product, and store models
- Dimension snapshots for slowly changing records (tracked via `updated_timestamp`)
- `fact_orders`, containing IDs and measures used for order analysis

## Data Quality Checks

dbt tests validate the Silver and analytical models:

- Unique values
- Non-null values
- Product prices greater than zero
- Required IDs present in the One Big Table

The custom `test_obt.sql` test warns when important identifiers such as `order_id`, `customer_id`, or `product_id` are missing.

## Airflow Orchestration

The `orchestrate` DAG runs **daily at 11:00 AM**:

1. Trigger the Databricks CDC ingestion job
2. Wait until the Databricks job finishes successfully
3. Remove the previous dbt target directory
4. Check source freshness
5. Run Silver technical models
6. Test Silver technical models
7. Run the Silver business model
8. Test the Silver business model
9. Run the Gold ephemeral models
10. Run dbt snapshots for dimensions
11. Create the Gold fact model

```text
Databricks CDC ingestion
        ↓
Clean dbt target
        ↓
Check source freshness
        ↓
Run Silver technical models
        ↓
Test Silver technical models
        ↓
Build Silver business model
        ↓
Test Silver business model
        ↓
Build Gold ephemeral models
        ↓
Run dimension snapshots
        ↓
Build Gold fact model
```

## Pipeline Runtime

The project runs with Docker Compose as an Airflow cluster, including:

- Airflow API server
- Scheduler
- DAG processor
- Celery workers
- Redis (task broker)
- PostgreSQL (Airflow metadata database)
- Triggerer
- Optional Flower monitoring service

The custom Docker image installs the required dbt and Databricks packages.

## Technology Responsibilities

| Technology | Responsibility |
|---|---|
| PostgreSQL | Operational database source |
| AWS S3 | Raw file storage source |
| Databricks | CDC and file ingestion, incremental processing, storage |
| dbt | SQL transformations, incremental models, snapshots, tests |
| Apache Airflow | Scheduling, dependency management, job monitoring, orchestration |
| Docker | Local deployment of the Airflow environment |
| PostgreSQL (metadata) | Stores Airflow state and task history |
