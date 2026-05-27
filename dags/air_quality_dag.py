from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os
import psycopg2

# --- default settings for the DAG ---
default_args = {
    'owner': 'you',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

# --- define the DAG ---
with DAG(
    dag_id='air_quality_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Daily air quality pipeline using EPA AirNow API'
) as dag:

    # --- Task 1: fetch data from EPA API ---
    def fetch_air_quality():
        api_key = os.getenv("EPA_API_KEY")

        os.makedirs('/opt/airflow/raw_data', exist_ok=True)

        url = "https://www.airnowapi.org/aq/observation/zipCode/current/"

        params = {
            "format": "application/json",
            "zipCode": "10001",
            "distance": "25",
            "API_KEY": api_key
        }

        response = requests.get(url, params=params)
        data = response.json()

        filename = f"/opt/airflow/raw_data/air_quality_{datetime.today().strftime('%Y-%m-%d')}.json"

        with open(filename, 'w') as f:
            json.dump(data, f)

        print(f"Fetched {len(data)} records")
        print(f"Saved to {filename}")

        return filename

    # --- Task 2: validate the data ---
    def validate_data(**context):
        filename = context['ti'].xcom_pull(task_ids='fetch_data')

        with open(filename, 'r') as f:
            data = json.load(f)

        print(f"Total records: {len(data)}")
        print(f"Data type: {type(data)}")
        print(f"Full data: {data}")

        assert len(data) > 0, "No data returned from API"

        records_with_aqi = [r for r in data if 'AQI' in r]
        assert len(records_with_aqi) > 0, "No records have AQI field"

        print(f"Validation passed — {len(data)} total records")
        print(f"Records with AQI: {len(records_with_aqi)}")

    # --- Task 3: load data into PostgreSQL ---
    def load_to_postgres(**context):
        filename = context['ti'].xcom_pull(task_ids='fetch_data')

        with open(filename, 'r') as f:
            data = json.load(f)

        # connect to postgres
        conn = psycopg2.connect(
            host=os.getenv("PIPELINE_DB_HOST"),
            dbname=os.getenv("PIPELINE_DB_NAME"),
            user=os.getenv("PIPELINE_DB_USER"),
            password=os.getenv("PIPELINE_DB_PASSWORD")
        )

        cursor = conn.cursor()

        # create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS air_quality (
                id              SERIAL PRIMARY KEY,
                date_observed   DATE,
                hour_observed   INT,
                city            VARCHAR(100),
                state           VARCHAR(10),
                pollutant       VARCHAR(50),
                aqi             INT,
                aqi_category    VARCHAR(50),
                latitude        FLOAT,
                longitude       FLOAT,
                created_at      TIMESTAMP DEFAULT NOW()
            )
        """)

        # insert each record
        for record in data:
            cursor.execute("""
                INSERT INTO air_quality (
                    date_observed,
                    hour_observed,
                    city,
                    state,
                    pollutant,
                    aqi,
                    aqi_category,
                    latitude,
                    longitude
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_observed, city, pollutant) DO NOTHING
            """, (
                record.get('DateObserved'),
                record.get('HourObserved'),
                record.get('ReportingArea'),
                record.get('StateCode'),
                record.get('ParameterName'),
                record.get('AQI'),
                record.get('Category', {}).get('Name'),
                record.get('Latitude'),
                record.get('Longitude')
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Loaded {len(data)} records into PostgreSQL")

    # --- Task 4: run dbt transformation ---
    def run_dbt():
        import subprocess

        # install dbt if not present
        subprocess.run(
            ["pip", "install", "dbt-postgres"],
            capture_output=True
        )

        # run dbt
        result = subprocess.run(
            ["/home/airflow/.local/bin/dbt", "run", "--profiles-dir", "/opt/airflow/dbt/air_quality_dbt"],
            capture_output=True,
            text=True,
            cwd="/opt/airflow/dbt/air_quality_dbt"
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt failed: {result.stderr}")
        print("dbt transformation completed successfully")

    # --- wire up the tasks ---
    task_fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_air_quality
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    task_dbt = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt
    )

    # --- set the order ---
    task_fetch >> task_validate >> task_load >> task_dbt
