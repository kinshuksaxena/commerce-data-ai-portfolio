"""
DAG 1: Dimension Ingestion & Processing Pipeline
Triggered on CSV uploads (products or customers) to Bronze storage.
Flow:
  task_sync_bronze_to_bigquery -> task_sync_customers_to_postgres
                               -> task_sync_product_fact_to_postgres
"""
from datetime import datetime, timedelta
import logging
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "commerce_analytics")


def sync_bronze_dimensions_to_bigquery(**context):
    """Syncs Bronze customers & products CSV from GCS to BigQuery silver tables and updates gold mart."""
    script_path = "/opt/airflow/lakehouse/ingestion/sync_dimensions_to_bigquery.py"
    cmd = ["python", script_path]
    logging.info(f"Running dimension BigQuery sync script: {cmd}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    logging.info(res.stdout)
    if res.returncode != 0:
        logging.error(res.stderr)
        raise RuntimeError(f"sync_dimensions_to_bigquery failed with code {res.returncode}")


def get_pg_connection():
    import psycopg2
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)
    kwargs = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "dbname": os.getenv("POSTGRES_DB", "commercedb"),
    }
    sslmode = os.getenv("POSTGRES_SSLMODE")
    if sslmode:
        kwargs["sslmode"] = sslmode
    return psycopg2.connect(**kwargs)


def sync_customers_to_postgres(**context):
    """Reverse-ETL: Syncs silver_customers from BigQuery into PostgreSQL customers.customers table."""
    from google.cloud import bigquery
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT customer_id, name, email, phone, address, latitude, longitude 
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_customers`
    """
    rows = list(bq_client.query(query).result())
    logging.info(f"Retrieved {len(rows)} customer rows from BigQuery silver_customers.")

    if not rows:
        logging.info("No customer rows in BigQuery to sync.")
        return

    conn = get_pg_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        for r in rows:
            cid = r["customer_id"]
            name = r["name"]
            email = r["email"]
            phone = r.get("phone") or "+1-415-555-0100"
            address = r.get("address") or "San Francisco, CA"
            lat = float(r["latitude"]) if r.get("latitude") is not None else None
            lng = float(r["longitude"]) if r.get("longitude") is not None else None

            cur.execute("""
                INSERT INTO customers.customers (customer_id, name, email, phone, address, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude;
            """, (cid, name, email, phone, address, lat, lng))
    conn.close()
    logging.info("Synced customer dimensions to PostgreSQL customers.customers successfully.")


def sync_product_fact_to_postgres(**context):
    """Reverse-ETL: Syncs gold product sales mart from BigQuery into PostgreSQL catalog.product_catalog."""
    from google.cloud import bigquery
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
    SELECT 
        product_id, product_name, category, 
        CAST(unit_price AS FLOAT64) AS unit_price, 
        units_sold AS total_units_sold, 
        CAST(gross_revenue AS FLOAT64) AS total_gross_revenue
    FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.gold_product_sales`
    """
    rows = list(bq_client.query(query).result())
    logging.info(f"Retrieved {len(rows)} product fact rows from BigQuery gold_product_sales.")

    if not rows:
        logging.info("No product fact rows in BigQuery to sync.")
        return

    conn = get_pg_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        for r in rows:
            prod_id = r["product_id"]
            prod_name = r["product_name"]
            cat = r["category"]
            price = r["unit_price"]
            sold = r["total_units_sold"]
            rev = r["total_gross_revenue"]
            cur.execute("""
                INSERT INTO catalog.product_catalog (
                    product_id, product_name, category, unit_price,
                    total_units_sold, total_gross_revenue, last_synced_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    unit_price = EXCLUDED.unit_price,
                    total_units_sold = EXCLUDED.total_units_sold,
                    total_gross_revenue = EXCLUDED.total_gross_revenue,
                    last_synced_at = NOW();
            """, (prod_id, prod_name, cat, price, sold, rev))
    conn.close()
    logging.info("Synced gold product facts to PostgreSQL catalog.product_catalog successfully.")


with DAG(
    dag_id="dag_dimension_ingestion",
    default_args=default_args,
    description="Dimension Ingestion & Processing Pipeline (Triggered on CSV Upload)",
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "dimensions", "bronze", "silver", "gold", "bigquery"],
) as dag:

    t1_sync_bq = PythonOperator(
        task_id="task_sync_bronze_to_bigquery",
        python_callable=sync_bronze_dimensions_to_bigquery,
    )

    t2_sync_customers = PythonOperator(
        task_id="task_sync_customers_to_postgres",
        python_callable=sync_customers_to_postgres,
    )

    t3_sync_product_fact = PythonOperator(
        task_id="task_sync_product_fact_to_postgres",
        python_callable=sync_product_fact_to_postgres,
    )

    t1_sync_bq >> t2_sync_customers
    t1_sync_bq >> t3_sync_product_fact
