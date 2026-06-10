# 🛒 Olist E-Commerce Data Pipeline

End-to-end **production-grade Data Engineering pipeline** built on the Brazilian Olist e-commerce dataset.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────────┐    ┌───────────┐
│  Olist CSVs │───▶│   Airflow    │───▶│   PostgreSQL     │───▶│ Metabase  │
│  (~100k     │    │  (Orchestr.) │    │  Raw → Staging   │    │Dashboard  │
│   orders)   │    │              │    │  → Star Schema   │    │           │
└─────────────┘    └──────────────┘    └──────────────────┘    └───────────┘
                          │                    ▲
                          └─── dbt Core ───────┘
                               (Transform)
```

### Data Flow
1. **Ingestion** — Python scripts load 9 Olist CSV files into `olist_raw` PostgreSQL DB
2. **Staging** — dbt views clean, type-cast, and rename raw columns
3. **Marts** — dbt tables build a **Star Schema** (fct_orders + dim_customers + dim_products)
4. **Orchestration** — Airflow DAG runs daily at 6 AM, with retries + alerting
5. **Quality** — dbt tests + custom DQ checks gate mart promotion
6. **Visualization** — Metabase dashboards on the warehouse layer

## Star Schema

```
                    ┌─────────────┐
                    │dim_customers│
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────┐    ┌────────────┐
│  dim_products│───▶│  fct_orders │◀───│  dim_dates │
└──────────────┘    └─────────────┘    └────────────┘
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt Core 1.7 |
| Warehouse | PostgreSQL 15 |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| Visualization | Metabase |
| Language | Python 3.11, SQL |

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k orders, 9 tables, 2016–2018

Download the dataset and place CSVs in `./data/` before running.

## Quick Start

```bash
# 1. Clone repo
git clone https://github.com/Priyanshupriyaa/olist-data-pipeline
cd olist-data-pipeline

# 2. Download Olist CSVs into ./data/

# 3. Start all services
docker compose up -d

# 4. Access UIs
# Airflow: http://localhost:8080  (admin/admin)
# Metabase: http://localhost:3000
# PostgreSQL: localhost:5432

# 5. Trigger pipeline manually
# Go to Airflow UI → olist_data_pipeline → Trigger DAG
```

## Project Structure

```
olist-data-pipeline/
├── dags/
│   └── olist_pipeline.py      # Main Airflow DAG
├── ingestion/
│   └── ingest_raw.py          # CSV → PostgreSQL loader
├── dbt/
│   ├── models/
│   │   ├── staging/           # Cleaning + type-casting views
│   │   └── marts/             # Star schema fact + dimension tables
│   ├── dbt_project.yml
│   └── profiles.yml
├── .github/workflows/
│   └── ci.yml                 # Lint + dbt parse + Docker build
├── docker-compose.yml
└── requirements.txt
```

## Key Metrics (from pipeline output)

- **~100k orders** processed per run
- **5 staging views** for clean layer
- **3 mart tables** (fct_orders, dim_customers, dim_products)
- **Daily schedule** with automatic retries
- **dbt tests** on all primary keys + business rules

## Resume Talking Points

> *"Built a production-grade ETL pipeline on the Olist dataset using Airflow for orchestration, dbt for SQL transformations, and PostgreSQL as the warehouse. Implemented a star schema with fact and dimension tables, added data quality gates, containerized the full stack with Docker Compose, and set up CI/CD with GitHub Actions."*
