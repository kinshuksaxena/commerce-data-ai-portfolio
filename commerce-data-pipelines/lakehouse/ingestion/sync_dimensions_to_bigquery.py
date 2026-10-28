"""
Dimension Ingestion & Synchronization to BigQuery:
Reads customer and product CSV files from Bronze GCS emulator,
syncs them into BigQuery silver_customers and silver_products,
and emits observability events into application_logs and pipeline_metrics.
"""
import csv
import io
import logging
import os
from google.cloud import storage, bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

STORAGE_EMULATOR_HOST = os.getenv("STORAGE_EMULATOR_HOST")
BUCKET_NAME = os.getenv("BRONZE_BUCKET", "commerce-lakehouse-bronze")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "commerce_analytics")

if STORAGE_EMULATOR_HOST:
    os.environ["STORAGE_EMULATOR_HOST"] = STORAGE_EMULATOR_HOST


def get_gcs_bucket():
    if STORAGE_EMULATOR_HOST:
        client = storage.Client.create_anonymous_client()
    else:
        client = storage.Client(project=GCP_PROJECT_ID)
    return client.bucket(BUCKET_NAME)


def sync_customers_to_bigquery():
    """Reads customers/customers_latest.csv from Bronze GCS and merges into BigQuery silver_customers."""
    logging.info("Reading customers CSV from Bronze storage...")
    bucket = get_gcs_bucket()
    blob = bucket.blob("customers/customers_latest.csv")
    if not blob.exists():
        logging.warning("customers/customers_latest.csv not found in Bronze bucket.")
        return 0

    content = blob.download_as_text()
    reader = csv.DictReader(io.StringIO(content))
    customers = list(reader)

    if not customers:
        logging.info("No customer records found in CSV.")
        return 0

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_customers"

    rows_to_insert = []
    for c in customers:
        lat = float(c["latitude"]) if c.get("latitude") not in (None, "") else None
        lng = float(c["longitude"]) if c.get("longitude") not in (None, "") else None
        rows_to_insert.append({
            "customer_id": c.get("customer_id"),
            "name": c.get("name") or c.get("customer_name"),
            "email": c.get("email"),
            "phone": c.get("phone") or "+1-415-555-0100",
            "address": c.get("address") or "San Francisco, CA",
            "latitude": lat,
            "longitude": lng,
            "loyalty_tier": c.get("loyalty_tier", "BRONZE"),
            "region": c.get("region", "UNKNOWN"),
        })

    def sql_str(val):
        if val is None:
            return "CAST(NULL AS STRING)"
        clean = str(val).replace("'", "\\'")
        return f"'{clean}'"

    def sql_num(val):
        if val is None:
            return "CAST(NULL AS FLOAT64)"
        return str(val)

    values_clause = ", ".join([
        f"STRUCT('{r['customer_id']}' AS customer_id, {sql_str(r['name'])} AS name, {sql_str(r['email'])} AS email, "
        f"{sql_str(r['phone'])} AS phone, {sql_str(r['address'])} AS address, "
        f"{sql_num(r['latitude'])} AS latitude, {sql_num(r['longitude'])} AS longitude, "
        f"{sql_str(r['loyalty_tier'])} AS loyalty_tier, {sql_str(r['region'])} AS region)"
        for r in rows_to_insert
    ])

    merge_sql = f"""
    MERGE `{table_id}` T
    USING (
        SELECT customer_id, name, email, phone, address, latitude, longitude, loyalty_tier, region
        FROM UNNEST([
            {values_clause}
        ])
    ) S
    ON T.customer_id = S.customer_id
    WHEN MATCHED THEN
        UPDATE SET 
            name = S.name,
            email = S.email,
            phone = S.phone,
            address = S.address,
            latitude = S.latitude,
            longitude = S.longitude,
            loyalty_tier = S.loyalty_tier,
            region = S.region,
            synced_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (customer_id, name, email, phone, address, latitude, longitude, loyalty_tier, region, synced_at)
        VALUES (S.customer_id, S.name, S.email, S.phone, S.address, S.latitude, S.longitude, S.loyalty_tier, S.region, CURRENT_TIMESTAMP());
    """
    bq_client.query(merge_sql).result()
    logging.info(f"Merged {len(rows_to_insert)} customers into BigQuery {table_id}")
    return len(rows_to_insert)


def sync_products_to_bigquery():
    """Reads products/products_latest.csv from Bronze GCS and merges into BigQuery silver_products."""
    logging.info("Reading products CSV from Bronze storage...")
    bucket = get_gcs_bucket()
    blob = bucket.blob("products/products_latest.csv")
    if not blob.exists():
        logging.warning("products/products_latest.csv not found in Bronze bucket.")
        return 0

    content = blob.download_as_text()
    reader = csv.DictReader(io.StringIO(content))
    products = list(reader)

    if not products:
        logging.info("No product records found in CSV.")
        return 0

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_products"

    rows_to_insert = []
    for p in products:
        rows_to_insert.append({
            "product_id": p.get("product_id"),
            "product_name": p.get("product_name"),
            "category": p.get("category", "General"),
            "unit_price": float(p.get("unit_price", 0.0)),
            "supplier": p.get("supplier", "Unknown"),
        })

    values_clause = ", ".join([
        f"STRUCT('{r['product_id']}' AS product_id, '{r['product_name']}' AS product_name, '{r['category']}' AS category, CAST({r['unit_price']} AS NUMERIC) AS unit_price, '{r['supplier']}' AS supplier)"
        for r in rows_to_insert
    ])

    merge_sql = f"""
    MERGE `{table_id}` T
    USING (
        SELECT product_id, product_name, category, unit_price, supplier
        FROM UNNEST([
            {values_clause}
        ])
    ) S
    ON T.product_id = S.product_id
    WHEN MATCHED THEN
        UPDATE SET 
            product_name = S.product_name,
            category = S.category,
            unit_price = S.unit_price,
            supplier = S.supplier,
            synced_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, category, unit_price, supplier, synced_at)
        VALUES (S.product_id, S.product_name, S.category, S.unit_price, S.supplier, CURRENT_TIMESTAMP());
    """
    bq_client.query(merge_sql).result()
    logging.info(f"Merged {len(rows_to_insert)} products into BigQuery {table_id}")
    return len(rows_to_insert)


def refresh_gold_product_sales():
    """Aggregates silver_orders and silver_products in BigQuery to refresh gold_product_sales mart."""
    logging.info("Recalculating and refreshing BigQuery gold_product_sales mart...")
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    gold_query = f"""
    MERGE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.gold_product_sales` T
    USING (
        SELECT 
            p.product_id,
            COALESCE(p.product_name, p.product_id) AS product_name,
            COALESCE(p.category, 'General') AS category,
            COALESCE(p.unit_price, 0) AS unit_price,
            COALESCE(SUM(o.quantity), 0) AS units_sold,
            COALESCE(SUM(o.total_amount), 0.0) AS gross_revenue,
            COUNT(DISTINCT o.order_id) AS orders_count,
            ROUND(COALESCE(AVG(o.total_amount), 0.0), 2) AS avg_order_value,
            MAX(o.order_timestamp) AS last_order_timestamp
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_products` p
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_orders` o ON p.product_id = o.product_id
        GROUP BY 1, 2, 3, 4
    ) S
    ON T.product_id = S.product_id
    WHEN MATCHED THEN
        UPDATE SET 
            product_name = S.product_name,
            category = S.category,
            unit_price = S.unit_price,
            units_sold = S.units_sold,
            gross_revenue = S.gross_revenue,
            orders_count = S.orders_count,
            avg_order_value = S.avg_order_value,
            last_order_timestamp = S.last_order_timestamp,
            synced_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, category, unit_price, units_sold, gross_revenue, orders_count, avg_order_value, last_order_timestamp, synced_at)
        VALUES (S.product_id, S.product_name, S.category, S.unit_price, S.units_sold, S.gross_revenue, S.orders_count, S.avg_order_value, S.last_order_timestamp, CURRENT_TIMESTAMP());
    """
    bq_client.query(gold_query).result()
    logging.info("BigQuery gold_product_sales mart refreshed successfully.")



def run():
    try:
        from observability_bq import log_event, record_pipeline_metric
    except ImportError:
        try:
            from lakehouse.ingestion.observability_bq import log_event, record_pipeline_metric
        except ImportError:
            log_event = None
            record_pipeline_metric = None

    if log_event:
        log_event("dimension-pipeline", "INFO", "Started sync_dimensions_to_bigquery job")

    c_count = sync_customers_to_bigquery()
    p_count = sync_products_to_bigquery()
    refresh_gold_product_sales()

    if record_pipeline_metric:
        record_pipeline_metric("dimension-pipeline", "customers_synced", c_count, {"dataset": BIGQUERY_DATASET})
        record_pipeline_metric("dimension-pipeline", "products_synced", p_count, {"dataset": BIGQUERY_DATASET})
    if log_event:
        log_event("dimension-pipeline", "INFO", f"Completed sync_dimensions_to_bigquery job: {c_count} customers, {p_count} products", metadata={"customers": c_count, "products": p_count})


if __name__ == "__main__":
    run()

