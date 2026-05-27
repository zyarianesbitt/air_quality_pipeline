# Air Quality Data Pipeline

An end-to-end data engineering pipeline that automatically collects, stores, transforms, and analyzes daily air quality data across US cities.

# Architecture
EPA AirNow API → Apache Airflow → AWS S3 → PostgreSQL → dbt

## Pipeline Overview

The pipeline runs automatically every day at midnight and consists of 5 tasks:

| Task | Description |
|------|-------------|
| `fetch_data` | Pulls daily air quality data from EPA AirNow API |
| `upload_to_s3` | Backs up raw JSON data to AWS S3 data lake |
| `validate_data` | Runs data quality checks on raw data |
| `load_to_postgres` | Loads clean data into PostgreSQL warehouse |
| `run_dbt` | Transforms raw data into analytical summary table |

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow |
| Language | Python |
| Data Lake | AWS S3 |
| Warehouse | PostgreSQL |
| Transformation | dbt |
| Containerization | Docker |

## Data Source

EPA AirNow API — provides real-time and historical air quality index (AQI) readings for US cities including pollutant types (O3, PM2.5, PM10) and health risk categories.

## dbt Transformation

Raw data is transformed into a clean summary table with:
- Daily average, max, and min AQI per city
- List of pollutants measured
- Risk level classification (Good, Moderate, Unhealthy)

## Project Structure
air-quality-pipeline/
├── dags/
│   └── air_quality_dag.py    # Airflow DAG with 5 tasks
├── dbt/
│   └── air_quality_dbt/
│       └── models/
│           └── air_quality_summary.sql  # dbt transformation
├── docker-compose.yaml        # Docker setup
└── README.md

## Setup

### Prerequisites
- Docker Desktop
- AWS Account
- EPA AirNow API key (free at airnowapi.org)

### Environment Variables
Create a `.env` file with:
AIRFLOW_UID=50000
EPA_API_KEY=your_key
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your_bucket
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow123@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql+psycopg2://airflow:airflow123@postgres/airflow
PIPELINE_DB_HOST=postgres
PIPELINE_DB_NAME=airflow
PIPELINE_DB_USER=airflow
PIPELINE_DB_PASSWORD=airflow123

### Run the pipeline
```bash
docker compose up
```

Open Airflow at `http://localhost:8080` and trigger the `air_quality_pipeline` DAG.

## What I Learned
- Building end-to-end data pipelines with Apache Airflow
- Storing and retrieving data from AWS S3
- Data modeling and transformation with dbt
- Containerizing data infrastructure with Docker
- Handling real-world API data with quality checks