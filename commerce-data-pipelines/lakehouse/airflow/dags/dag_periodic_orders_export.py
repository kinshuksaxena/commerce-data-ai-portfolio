"""
DAG 2: Periodic Order Lakehouse Sync
Periodically extracts new orders from PostgreSQL into Bronze Avro,
updates PyIceberg catalog metadata, streams into BigQuery, and syncs Gold mart to PostgreSQL.
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


def export_orders_to_avro(**context):
    """Runs export_orders_to_avro.py to extract Postgres orders into Bronze Avro, Iceberg & BigQuery."""
    script_path = "/opt/airflow/lakehouse/ingestion/export_orders_to_avro.py"
    cmd = ["python", script_path]
    logging.info(f"Running order export script: {cmd}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    logging.info(res.stdout)
    if res.returncode != 0:
        logging.error(res.stderr)
        raise RuntimeError(f"export_orders_to_avro failed with code {res.returncode}")


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


def sync_gold_mart_to_postgres(**context):
    """Reverse-ETL: Syncs updated gold product metrics from BigQuery to catalog.product_catalog."""
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
    logging.info(f"Retrieved {len(rows)} product sales records from BigQuery gold mart.")

    if not rows:
        logging.info("No gold product sales rows in BigQuery to sync.")
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
    logging.info("Synced gold product sales mart to PostgreSQL catalog.product_catalog successfully.")


with DAG(
    dag_id="dag_periodic_orders_export",
    default_args=default_args,
    description="Periodic Order Lakehouse Sync (Postgres -> Avro -> Iceberg -> BigQuery -> Postgres)",
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["lakehouse", "orders", "avro", "iceberg", "bigquery", "reverse-etl"],
) as dag:

    t1_export_orders = PythonOperator(
        task_id="task_export_orders_to_avro",
        python_callable=export_orders_to_avro,
    )

    t2_sync_postgres = PythonOperator(
        task_id="task_sync_gold_mart_to_postgres",
        python_callable=sync_gold_mart_to_postgres,
    )

    t1_export_orders >> t2_sync_postgres
