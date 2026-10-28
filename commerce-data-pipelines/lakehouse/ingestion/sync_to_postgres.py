import logging
import os
import psycopg2
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BIGQUERY_DATASET", "commerce_analytics")

DATABASE_URL = os.getenv("DATABASE_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "commercedb")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE")


def get_pg_connection():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL)
    kwargs = {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "dbname": POSTGRES_DB,
    }
    if POSTGRES_SSLMODE:
        kwargs["sslmode"] = POSTGRES_SSLMODE
    return psycopg2.connect(**kwargs)


def sync_gold_mart_to_postgres():
    """Extracts aggregated product sales metrics from BigQuery Gold Mart and syncs to PostgreSQL catalog.product_catalog."""
    logging.info(f"Extracting Gold Mart metrics from BigQuery {PROJECT_ID}.{DATASET_ID}.gold_product_sales...")
    bq_client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT 
        product_id,
        product_name,
        category,
        CAST(unit_price AS FLOAT64) AS unit_price,
        units_sold,
        CAST(gross_revenue AS FLOAT64) AS gross_revenue
    FROM `{PROJECT_ID}.{DATASET_ID}.gold_product_sales`
    """
    query_job = bq_client.query(query)
    rows = list(query_job.result())
    logging.info(f"Retrieved {len(rows)} products with aggregated mart metrics from BigQuery.")

    if not rows:
        logging.info("No rows in BigQuery gold_product_sales to sync.")
        return

    logging.info(f"Syncing Gold Mart to Postgres {POSTGRES_HOST}:{POSTGRES_PORT} table 'product_catalog'...")
    conn = get_pg_connection()
    conn.autocommit = True

    with conn.cursor() as cur:
        for r in rows:
            prod_id = r["product_id"]
            prod_name = r["product_name"]
            cat = r["category"]
            price = r["unit_price"]
            sold = r["units_sold"]
            rev = r["gross_revenue"]
            cur.execute(
                """
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
                """,
                (prod_id, prod_name, cat, price, sold, rev),
            )

    conn.close()
    logging.info("Reverse-ETL sync to Postgres completed successfully!")


if __name__ == "__main__":
    sync_gold_mart_to_postgres()
