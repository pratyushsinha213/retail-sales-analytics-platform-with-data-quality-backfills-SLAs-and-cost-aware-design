# Retail Sales Analytics Platform with Data Quality, Backfills, SLAs, and Cost-Aware Design

![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazon-aws&style=flat-square)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple?logo=terraform&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange?logo=apache-spark&style=flat-square)
![AWS Glue](https://img.shields.io/badge/AWS-Glue-orange?logo=amazon-aws&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python&style=flat-square)

---

## Table of contents

- [Project overview](#-project-overview)
  - [End-to-end flow](#-end-to-end-flow)
  - [Key highlights](#-key-highlights)
- [Objectives](#-objectives)
- [Project structure](#-project-structure)
- [Tools & technologies](#-tools--technologies)
- [Data architecture](#-data-architecture)
- [Star schema design](#-star-schema-design)
- [Run pipeline (local)](#-run-pipeline-local)
- [Test config and paths](#-how-to-test-config--paths)
- [Check layer contents](#-check-layer-contents-bronze-silver-gold)
- [Data quality checks](#-data-quality-checks)
- [Run on AWS (Glue)](#-run-on-aws-glue)
- [Data analytics](#-data-analytics)
- [Key outcomes](#-key-outcomes)
- [Author](#-author)

---

## Project overview

This project is an **end-to-end data engineering pipeline** for **Apple Retail** sales data. The workflow is driven by **Terraform** (AWS S3, IAM, Glue Catalog, Glue Job). Raw CSVs are ingested into a **Bronze** layer, then transformed through **Silver** (cleaned, validated) and **Gold** (star schema) in **PySpark**—run **locally** (Parquet under `./data_lake`) or on **AWS Glue** (S3 + Delta). Built-in **data quality checks** validate Silver and Gold. The Gold layer is ready for analytics and BI (e.g. Power BI, Athena, or custom KPI reports).

### End-to-end flow

**Terraform (provision)** → **Ingestion (CSV → Bronze)** → **Silver (clean/validate)** → **Gold (star schema)** → **DQ checks** → **Optional: Glue (AWS) / BI (reports, dashboards)**

### Key highlights

- **Single entrypoint locally:** `main.py` runs ingest → silver → gold → DQ in one command.
- **Dual run modes:** Local (Parquet, `./data_lake`) or AWS (S3 + Delta via Glue).
- **Config-driven:** `config/dev.yaml` and `config/prod.yaml`; Glue uses env vars (no YAML in S3).
- **Bronze–Silver–Gold** with star schema in Gold (`fact_sales`, `dim_category`, `dim_product`, `dim_store`).
- **Spark-based data quality** checks (no Great Expectations); optional `--fail-dq` for CI.

---

## Objectives

- Ingest raw retail data (category, products, sales, stores, warranty) from **raw-data-source** into a data lake.
- Implement a **bronze–silver–gold** layered architecture (local Parquet or AWS S3/Delta).
- Build a **star schema** in Gold optimized for analytical queries and KPIs.
- Run the pipeline **locally** via `main.py` or **on AWS** via a Glue job (Terraform-managed).
- Apply **reproducible, config-driven** practices with optional data quality gates.

---

## Project structure

```text
Apple Retail and Sales Analysis/
├── main.py                 # Local pipeline entrypoint (ingest → silver → gold → DQ)
├── requirements.txt
├── config/
│   ├── dev.yaml            # Local paths, ./data_lake, raw-data-source
│   └── prod.yaml           # S3 bucket, prefix, region
├── raw-data-source/
│   ├── category.csv
│   ├── products.csv
│   ├── sales.csv
│   ├── stores.csv
│   └── warranty.csv
├── utils/
│   ├── config_loader.py    # YAML (local) or env (Glue)
│   └── path_utils.py       # Layer + raw paths (local or s3://)
├── ingestion/
│   └── ingest_raw.py       # CSVs → Bronze (Parquet local, Delta on AWS)
├── transformations/
│   ├── bronze_to_silver.py # Clean, validate, standardize
│   └── silver_to_gold.py   # Star schema: fact_sales + dim_*
├── data_quality/
│   └── run_checks.py       # Silver + Gold checks (Spark-only)
├── scripts/
│   ├── test_config_and_paths.py
│   ├── check_bronze.py
│   ├── check_silver.py
│   └── check_gold.py
├── glue/
│   ├── glue_pipeline.py    # Glue job entrypoint (ingest → silver → gold → DQ)
│   └── README.md           # Upload instructions for S3 + app.zip
├── terraform/              # S3, IAM, Glue Catalog DB, Glue Job (pratyush_* naming)
├── kpi/                    # Optional: reports and Power BI sources from Gold
│   ├── reports/
│   └── raw_pbix_kpi_files/
└── README.md
```

---

## Tools & technologies

- **AWS (S3, Glue, IAM)** – Data lake storage, serverless ETL job, catalog.
- **Terraform** – Infrastructure as code for bucket, Glue job, and permissions.
- **PySpark** – Ingestion and transformations (local or Glue workers).
- **Parquet / Delta** – Local: Parquet under `./data_lake`; AWS: Delta on S3.
- **Python 3.9+** – Config, path utils, and pipeline modules.
- **YAML + env** – Config: `config/*.yaml` locally; Glue uses `AWS_S3_BUCKET`, `USE_LOCAL_PATHS=false`, etc.

---

## Data architecture

The pipeline uses a **multi-layered architecture** for clarity and maintainability:

### Bronze layer

- **Raw data** from CSVs (local: `raw-data-source/`; AWS: `s3://bucket/prefix/raw/`).
- Written as **Parquet** (local) or **Delta** (AWS). Immutable source of truth for downstream steps.

### Silver layer

- **Cleaned and validated** in PySpark: schema fixes, null handling, type casting, renames.
- Same five entities: category, products, sales, stores, warranty. Stored in `silver/` (Parquet or Delta).

### Gold layer

- **Curated analytics layer** in **star schema**: one fact table and three dimension tables.
- Stored in `gold/` (Parquet or Delta), ready for BI and SQL analytics.

---

## Star schema design

**Fact table**

- **fact_sales** – Sales transactions with quantity, revenue, product/store/category references (surrogate keys), and `has_claim` from warranty.

**Dimension tables**

- **dim_category** – Category attributes.
- **dim_product** – Product name, price, launch date, category key.
- **dim_store** – Store location, country, region.

---

## Run pipeline (local)

From **project root** (venv activated, `pip install -r requirements.txt`):

**Single entrypoint (recommended):**

```bash
python main.py --env dev
# Optional: --skip-dq to skip data quality, --fail-dq to exit 1 if DQ fails
```

**Step by step:**

```bash
# 1. Ingest raw CSVs → Bronze (Parquet under ./data_lake/bronze/)
python3 -m ingestion.ingest_raw --env dev

# 2. Bronze → Silver (clean dates, nulls, renames → ./data_lake/silver/)
python3 -m transformations.bronze_to_silver --env dev

# 3. Silver → Gold (star schema: fact_sales + dim_* → ./data_lake/gold/)
python3 -m transformations.silver_to_gold --env dev
```

Gold outputs: `dim_category`, `dim_product`, `dim_store`, `fact_sales` (revenue, has_claim from warranty).

---

## How to test (config + paths)

From **project root**:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Test dev config and paths (local paths, raw-data-source)
python3 scripts/test_config_and_paths.py --env dev

# Test prod config and paths (S3 paths)
python3 scripts/test_config_and_paths.py --env prod
```

You should see project root, config values, layer paths (bronze/silver/gold), and raw CSV paths. For `dev`, it also prints whether each raw CSV exists.

---

## Check layer contents (Bronze, Silver, Gold)

After running the pipeline, inspect each layer (row counts, schema, sample rows, null counts on keys). The Gold script also runs a quick referential check (fact keys present in dims).

```bash
python3 scripts/check_bronze.py --env dev
python3 scripts/check_silver.py --env dev
python3 scripts/check_gold.py --env dev
```

---

## Data quality checks

Simple Spark-based checks (no Great Expectations). Run after the pipeline:

```bash
# Silver + Gold (default)
python -m data_quality.run_checks --env dev

# Silver only or Gold only
python -m data_quality.run_checks --env dev --layer silver
python -m data_quality.run_checks --env dev --layer gold

# Exit with code 1 if any check fails (e.g. for CI)
python -m data_quality.run_checks --env dev --fail
```

**Silver:** row count > 0, no nulls on primary keys, sales quantity ≥ 1.  
**Gold:** fact_sales row count > 0, no nulls on keys/revenue, revenue ≥ 0, has_claim in {0,1}, referential (fact keys exist in dims). Prints **PASS** or **FAIL** with a list of failed checks.

---

## Run on AWS (Glue)

1. **Terraform** – Deploy S3, IAM, Glue job (see `terraform/`).
2. **Upload script and code** – Put `glue/glue_pipeline.py` and `app.zip` (contents: `config`, `utils`, `ingestion`, `transformations`, `data_quality`) in the S3 path used by the Glue job (`--script_location` and `--extra-py-files`).
3. **Upload raw data** – CSVs from `raw-data-source/` to `s3://<bucket>/<prefix>/raw/`.
4. **Run the Glue job** – Console or `aws glue start-job-run --job-name pratyush_pipeline_<region>_job`.

Details: **glue/README.md**.

---

## Data analytics

The **Gold layer** is structured for efficient analytical querying and KPI generation. You can:

- **Query Gold** with Athena, Spark SQL, or any SQL-on-Parquet/Delta tool.
- **Connect BI tools** (e.g. Power BI) to Gold tables for dashboards (total revenue, top products, sales by country/category, etc.).
- **Use existing KPI assets** in `kpi/` (reports and `.pbix` files) as references; re-point them to your Gold output (local or S3) as needed.

---

## Key outcomes

- **End-to-end pipeline:** Terraform → ingestion → Bronze → Silver → Gold → DQ, with one local entrypoint (`main.py`) and one AWS entrypoint (Glue).
- **Dual execution:** Same code runs locally (Parquet) or on AWS (Delta) via config/env.
- **Modular architecture:** Clear separation of Bronze, Silver, and Gold; star schema in Gold.
- **Data quality:** Built-in Spark checks; optional fail-on-error for CI.
- **Portfolio value:** Demonstrates AWS (S3, Glue), Terraform, PySpark, and config-driven data engineering.

---

## Author

**Pratyush Sinha**  
📧 **pratisinha@gmail.com**  
🔗 [LinkedIn](https://linkedin.com/in/pratyushsinha213)
