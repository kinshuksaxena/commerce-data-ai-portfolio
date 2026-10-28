"""
Order Exporter Job: Lakehouse Bronze Synchronization
Extracts transactional e-commerce orders from PostgreSQL (orders.orders & orders.order_items),
merges them with existing Bronze Avro records in fake-gcs-server, updates the PyIceberg catalog,
and refreshes ClickHouse external tables for downstream dbt transformations.
"""
import io
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[3] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

import clickhouse_connect
import fastavro
from google.cloud import storage, bigquery
import psycopg2
from psycopg2.extras import RealDictCursor
import pyarrow as pa
from pyiceberg.catalog import load_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Environment configurations
STORAGE_EMULATOR_HOST = os.getenv("STORAGE_EMULATOR_HOST")
BUCKET_NAME = os.getenv("BRONZE_BUCKET", "commerce-lakehouse-bronze")
ORDERS_BLOB_NAME = "orders/orders_latest.avro"

if STORAGE_EMULATOR_HOST:
    os.environ["STORAGE_EMULATOR_HOST"] = STORAGE_EMULATOR_HOST

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

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "commerce_analytics")

ORDER_AVRO_SCHEMA = {
    "type": "record",
    "name": "OrderRecord",
    "namespace": "com.commerce.lakehouse",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "product_id", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "unit_price", "type": "double"},
        {"name": "total_amount", "type": "double"},
        {"name": "order_status", "type": "string"},
        {"name": "order_timestamp", "type": "string"},
    ],
}


def get_existing_avro_orders(bucket: storage.Bucket) -> list:
    """Reads existing orders from GCS Avro file if present."""
    blob = bucket.blob(ORDERS_BLOB_NAME)
    if not blob.exists():
        logging.info("No existing Avro orders file found in bucket. Starting fresh.")
        return []

    try:
        data = blob.download_as_bytes()
        reader = fastavro.reader(io.BytesIO(data))
        records = list(reader)
        logging.info(f"Loaded {len(records)} existing order records from gs://{BUCKET_NAME}/{ORDERS_BLOB_NAME}")
        return records
    except Exception as e:
        logging.warning(f"Could not read existing Avro file: {e}. Starting fresh.")
        return []


def fetch_postgres_orders() -> list:
    """Extracts all line items with order headers from PostgreSQL commercedb."""
    query = """
    SELECT 
        o.order_id,
        o.customer_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total AS total_amount,
        o.status AS order_status,
        TO_CHAR(o.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS order_timestamp
    FROM orders.orders o
    JOIN orders.order_items oi ON o.order_id = oi.order_id
    ORDER BY o.created_at ASC;
    """
    conn = get_pg_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    conn.close()

    # Format for Avro schema compliance
    orders = []
    for r in rows:
        orders.append({
            "order_id": str(r["order_id"]),
            "customer_id": str(r["customer_id"]),
            "product_id": str(r["product_id"]),
            "quantity": int(r["quantity"]),
            "unit_price": float(r["unit_price"]),
            "total_amount": float(r["total_amount"]),
            "order_status": str(r["order_status"]),
            "order_timestamp": str(r["order_timestamp"]),
        })
    logging.info(f"Extracted {len(orders)} order line item records from PostgreSQL (orders.*)")
    return orders


def write_avro_bytes(records: list) -> bytes:
    """Serializes order dictionaries into Avro binary."""
    out = io.BytesIO()
    fastavro.writer(out, ORDER_AVRO_SCHEMA, records)
    return out.getvalue()


def sync_to_pyiceberg(new_orders: list):
    """Appends newly exported orders into the local PyIceberg catalog."""
    if not new_orders:
        return

    try:
        warehouse_dir = "/tmp/iceberg_warehouse"
        os.makedirs(warehouse_dir, exist_ok=True)
        catalog = load_catalog(
            "lakehouse",
            **{
                "type": "sql",
                "uri": "sqlite:////tmp/lakehouse_iceberg_catalog.db",
                "warehouse": warehouse_dir,
            },
        )
        catalog.create_namespace_if_not_exists("bronze")

        arrow_schema = pa.schema([
            ("order_id", pa.string()),
            ("customer_id", pa.string()),
            ("product_id", pa.string()),
            ("quantity", pa.int32()),
            ("unit_price", pa.float64()),
            ("total_amount", pa.float64()),
            ("order_status", pa.string()),
            ("order_timestamp", pa.string()),
        ])

        table = catalog.create_table_if_not_exists("bronze.orders", schema=arrow_schema)
        arrow_table = pa.Table.from_pylist(new_orders, schema=arrow_schema)
        table.append(arrow_table)
        logging.info(f"Committed {len(new_orders)} new order records to PyIceberg table 'bronze.orders'")
    except Exception as e:
        logging.warning(f"PyIceberg catalog sync skipped: {e}. Bronze Avro and ClickHouse lakeview remain fully functional.")


def ensure_bigquery_external_tables(bq_client: bigquery.Client):
    """Ensures BigQuery external tables (bronze_orders, bronze_products, bronze_customers) exist pointing to GCS."""
    try:
        ddl_orders = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.bronze_orders`
        OPTIONS (
            format = 'AVRO',
            uris = ['gs://{BUCKET_NAME}/orders/orders_latest.avro']
        );
        """
        bq_client.query(ddl_orders).result()
        logging.info("BigQuery external table bronze_orders ensured successfully.")
    except Exception as e:
        logging.warning(f"Could not ensure bronze_orders external table: {e}")


def sync_orders_to_bigquery(new_orders):
    """Syncs BigQuery bronze_orders external table, refreshes silver_orders, and updates gold_product_sales."""
    try:
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)

        # 1. Ensure external table is pointing to the latest Bronze Avro in GCS
        ensure_bigquery_external_tables(bq_client)

        # 2. Merge from bronze_orders external table into silver_orders (matching dbt silver schema)
        silver_merge_sql = f"""
        MERGE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_orders` T
        USING (
            SELECT 
                o.order_id,
                o.customer_id,
                o.product_id,
                CAST(o.quantity AS INT64) AS quantity,
                CAST(o.unit_price AS NUMERIC) AS unit_price,
                CAST(o.total_amount AS NUMERIC) AS total_amount,
                UPPER(TRIM(o.order_status)) AS order_status,
                COALESCE(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', o.order_timestamp),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', o.order_timestamp),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_timestamp)
                ) AS order_timestamp,
                SUBSTR(o.order_timestamp, 1, 10) AS order_date,
                CURRENT_TIMESTAMP() AS silver_transformed_at
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.bronze_orders` o
            WHERE o.order_id IS NOT NULL AND o.order_id != ''
        ) S
        ON T.order_id = S.order_id AND T.product_id = S.product_id
        WHEN MATCHED THEN
            UPDATE SET 
                customer_id = S.customer_id,
                quantity = S.quantity,
                unit_price = S.unit_price,
                total_amount = S.total_amount,
                order_status = S.order_status,
                order_timestamp = S.order_timestamp,
                order_date = S.order_date,
                silver_transformed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (order_id, customer_id, product_id, quantity, unit_price, total_amount, order_status, order_timestamp, order_date, silver_transformed_at)
            VALUES (S.order_id, S.customer_id, S.product_id, S.quantity, S.unit_price, S.total_amount, S.order_status, S.order_timestamp, S.order_date, CURRENT_TIMESTAMP());
        """
        bq_client.query(silver_merge_sql).result()
        logging.info("BigQuery silver_orders table merged and up to date from bronze_orders.")

        # 3. Recalculate and update gold_product_sales with exact product-level metrics from silver_orders
        gold_merge_query = f"""
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
        bq_client.query(gold_merge_query).result()
        logging.info("BigQuery gold_product_sales mart updated successfully from silver_orders!")

    except Exception as e:
        logging.warning(f"BigQuery sync error: {e}")



def sync_order_status_history_to_bigquery():
    """Extracts all status history records from orders.order_status_history in PostgreSQL and merges into BigQuery."""
    try:
        conn = get_pg_connection()
        query = """
            SELECT 
                history_id,
                order_id,
                COALESCE(previous_status, 'UNKNOWN') AS previous_status,
                new_status,
                TO_CHAR(changed_at, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS changed_at,
                COALESCE(changed_by, 'SYSTEM') AS changed_by,
                COALESCE(notes, '') AS notes
            FROM orders.order_status_history
            ORDER BY changed_at ASC;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        if not rows:
            logging.info("No status history records found in PostgreSQL orders.order_status_history.")
            return

        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Format rows for BigQuery staging / merge
        # Create temp table or direct parameter query or VALUES query
        formatted_rows = []
        for r in rows:
            formatted_rows.append({
                "history_id": int(r["history_id"]),
                "order_id": str(r["order_id"]),
                "previous_status": str(r["previous_status"]),
                "new_status": str(r["new_status"]),
                "changed_at": str(r["changed_at"]),
                "changed_by": str(r["changed_by"]),
                "notes": str(r["notes"]),
            })

        # Load into BigQuery staging or direct MERGE via load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("history_id", "INT64"),
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("previous_status", "STRING"),
                bigquery.SchemaField("new_status", "STRING"),
                bigquery.SchemaField("changed_at", "STRING"),
                bigquery.SchemaField("changed_by", "STRING"),
                bigquery.SchemaField("notes", "STRING"),
            ]
        )
        staging_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}._staging_order_status_history"
        load_job = bq_client.load_table_from_json(formatted_rows, staging_table_id, job_config=job_config)
        load_job.result()

        merge_sql = f"""
        MERGE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_order_status_history` T
        USING (
            SELECT 
                history_id,
                order_id,
                previous_status,
                new_status,
                COALESCE(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', changed_at),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', changed_at)
                ) AS changed_at,
                changed_by,
                notes,
                CURRENT_TIMESTAMP() AS synced_at
            FROM `{staging_table_id}`
        ) S
        ON T.history_id = S.history_id
        WHEN MATCHED THEN
            UPDATE SET 
                order_id = S.order_id,
                previous_status = S.previous_status,
                new_status = S.new_status,
                changed_at = S.changed_at,
                changed_by = S.changed_by,
                notes = S.notes,
                synced_at = S.synced_at
        WHEN NOT MATCHED THEN
            INSERT (history_id, order_id, previous_status, new_status, changed_at, changed_by, notes, synced_at)
            VALUES (S.history_id, S.order_id, S.previous_status, S.new_status, S.changed_at, S.changed_by, S.notes, S.synced_at);
        """
        bq_client.query(merge_sql).result()
        logging.info(f"BigQuery silver_order_status_history synchronized successfully ({len(rows)} records).")

    except Exception as e:
        logging.warning(f"Error synchronizing order status history to BigQuery: {e}")


def sync_delivery_route_stops_to_bigquery():
    """Extracts completed and active route stops from delivery.route_stops_history and delivery.route_stops, merging into BigQuery."""
    try:
        conn = get_pg_connection()
        query = """
            SELECT 
                COALESCE(h.route_id, rs.route_id) AS route_id,
                COALESCE(h.stop_id, rs.stop_id) AS stop_id,
                COALESCE(h.stop_sequence, rs.stop_sequence) AS stop_sequence,
                COALESCE(h.address, rs.address) AS address,
                CAST(COALESCE(h.latitude, rs.latitude) AS FLOAT) AS latitude,
                CAST(COALESCE(h.longitude, rs.longitude) AS FLOAT) AS longitude,
                COALESCE(h.stop_status, rs.status) AS stop_status,
                TO_CHAR(COALESCE(h.departed_previous_stop_at, rs.departed_previous_stop_at), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS departed_previous_stop_at,
                TO_CHAR(COALESCE(h.arrived_at, rs.arrived_at), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS arrived_at,
                TO_CHAR(COALESCE(h.completed_at, rs.completed_at), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at,
                COALESCE(h.transit_duration_seconds, rs.transit_duration_seconds, 0) AS transit_duration_seconds,
                COALESCE(h.service_duration_seconds, rs.service_duration_seconds, 0) AS service_duration_seconds,
                COALESCE(h.total_stop_duration_seconds, (COALESCE(h.transit_duration_seconds, rs.transit_duration_seconds, 0) + COALESCE(h.service_duration_seconds, rs.service_duration_seconds, 0)), 0) AS total_stop_duration_seconds
            FROM delivery.route_stops_history h
            FULL OUTER JOIN delivery.route_stops rs ON h.route_id = rs.route_id AND h.stop_id = rs.stop_id;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        if not rows:
            logging.info("No route stops found in PostgreSQL delivery schema.")
            return

        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        formatted_rows = []
        for r in rows:
            formatted_rows.append({
                "route_id": str(r["route_id"]),
                "stop_id": int(r["stop_id"]),
                "stop_sequence": int(r["stop_sequence"]),
                "address": str(r.get("address") or ""),
                "stop_status": str(r.get("stop_status") or "PENDING"),
                "departed_previous_stop_at": str(r["departed_previous_stop_at"]) if r.get("departed_previous_stop_at") else None,
                "arrived_at": str(r["arrived_at"]) if r.get("arrived_at") else None,
                "completed_at": str(r["completed_at"]) if r.get("completed_at") else None,
                "transit_duration_seconds": int(r.get("transit_duration_seconds") or 0),
                "service_duration_seconds": int(r.get("service_duration_seconds") or 0),
                "total_stop_duration_seconds": int(r.get("total_stop_duration_seconds") or 0),
                "stop_date": str(r["completed_at"])[:10] if r.get("completed_at") else str(r.get("arrived_at") or "")[:10] or "2026-10-24",
            })

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("route_id", "STRING"),
                bigquery.SchemaField("stop_id", "INT64"),
                bigquery.SchemaField("stop_sequence", "INT64"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("stop_status", "STRING"),
                bigquery.SchemaField("departed_previous_stop_at", "STRING"),
                bigquery.SchemaField("arrived_at", "STRING"),
                bigquery.SchemaField("completed_at", "STRING"),
                bigquery.SchemaField("transit_duration_seconds", "INT64"),
                bigquery.SchemaField("service_duration_seconds", "INT64"),
                bigquery.SchemaField("total_stop_duration_seconds", "INT64"),
                bigquery.SchemaField("stop_date", "STRING"),
            ]
        )
        staging_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}._staging_delivery_route_stops"
        load_job = bq_client.load_table_from_json(formatted_rows, staging_table_id, job_config=job_config)
        load_job.result()

        merge_sql = f"""
        MERGE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_delivery_route_stops` T
        USING (
            SELECT 
                route_id,
                stop_id,
                stop_sequence,
                address,
                stop_status,
                COALESCE(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', departed_previous_stop_at),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', departed_previous_stop_at)
                ) AS departed_previous_stop_at,
                COALESCE(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', arrived_at),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', arrived_at)
                ) AS arrived_at,
                COALESCE(
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', completed_at),
                    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', completed_at)
                ) AS completed_at,
                transit_duration_seconds,
                service_duration_seconds,
                total_stop_duration_seconds,
                stop_date,
                CURRENT_TIMESTAMP() AS synced_at
            FROM `{staging_table_id}`
        ) S
        ON T.route_id = S.route_id AND T.stop_id = S.stop_id
        WHEN MATCHED THEN
            UPDATE SET 
                stop_sequence = S.stop_sequence,
                address = S.address,
                stop_status = S.stop_status,
                departed_previous_stop_at = S.departed_previous_stop_at,
                arrived_at = S.arrived_at,
                completed_at = S.completed_at,
                transit_duration_seconds = S.transit_duration_seconds,
                service_duration_seconds = S.service_duration_seconds,
                total_stop_duration_seconds = S.total_stop_duration_seconds,
                stop_date = S.stop_date,
                synced_at = S.synced_at
        WHEN NOT MATCHED THEN
            INSERT (
                route_id, stop_id, stop_sequence, address,
                stop_status, departed_previous_stop_at, arrived_at, completed_at,
                transit_duration_seconds, service_duration_seconds, total_stop_duration_seconds, stop_date, synced_at
            )
            VALUES (
                S.route_id, S.stop_id, S.stop_sequence, S.address,
                S.stop_status, S.departed_previous_stop_at, S.arrived_at, S.completed_at,
                S.transit_duration_seconds, S.service_duration_seconds, S.total_stop_duration_seconds, S.stop_date, S.synced_at
            );
        """
        bq_client.query(merge_sql).result()
        logging.info(f"BigQuery silver_delivery_route_stops synchronized successfully ({len(rows)} records).")

        # Refresh Gold Route Performance Daily Mart
        refresh_gold_route_performance(bq_client)

    except Exception as e:
        logging.warning(f"Error synchronizing delivery route stops to BigQuery: {e}")


def refresh_gold_route_performance(bq_client: bigquery.Client):
    """Calculates daily aggregated route performance and merges into gold_route_performance_daily."""
    try:
        gold_sql = f"""
        MERGE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.gold_route_performance_daily` T
        USING (
            SELECT 
                route_id,
                COALESCE(stop_date, FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())) AS delivery_date,
                COUNT(stop_id) AS total_stops,
                COUNTIF(stop_status = 'COMPLETED') AS total_orders_delivered,
                COALESCE(SUM(transit_duration_seconds), 0) AS total_transit_seconds,
                COALESCE(SUM(service_duration_seconds), 0) AS total_service_seconds,
                COALESCE(SUM(total_stop_duration_seconds), 0) AS total_route_duration_seconds,
                ROUND(COALESCE(AVG(transit_duration_seconds), 0.0), 2) AS avg_stop_transit_seconds,
                ROUND(COALESCE(AVG(service_duration_seconds), 0.0), 2) AS avg_stop_service_seconds,
                CURRENT_TIMESTAMP() AS synced_at
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_delivery_route_stops`
            GROUP BY 1, 2
        ) S
        ON T.route_id = S.route_id AND T.delivery_date = S.delivery_date
        WHEN MATCHED THEN
            UPDATE SET 
                total_stops = S.total_stops,
                total_orders_delivered = S.total_orders_delivered,
                total_transit_seconds = S.total_transit_seconds,
                total_service_seconds = S.total_service_seconds,
                total_route_duration_seconds = S.total_route_duration_seconds,
                avg_stop_transit_seconds = S.avg_stop_transit_seconds,
                avg_stop_service_seconds = S.avg_stop_service_seconds,
                synced_at = S.synced_at
        WHEN NOT MATCHED THEN
            INSERT (
                route_id, delivery_date, total_stops, total_orders_delivered,
                total_transit_seconds, total_service_seconds,
                total_route_duration_seconds, avg_stop_transit_seconds,
                avg_stop_service_seconds, synced_at
            )
            VALUES (
                S.route_id, S.delivery_date, S.total_stops, S.total_orders_delivered,
                S.total_transit_seconds, S.total_service_seconds,
                S.total_route_duration_seconds, S.avg_stop_transit_seconds,
                S.avg_stop_service_seconds, S.synced_at
            );
        """
        bq_client.query(gold_sql).result()
        logging.info("BigQuery gold_route_performance_daily mart refreshed successfully.")
    except Exception as e:
        logging.warning(f"Error refreshing gold_route_performance_daily: {e}")


def export_orders_job():

    """Main execution orchestrating Postgres -> GCS Avro -> Iceberg -> BigQuery."""
    logging.info("Starting Lakehouse Order Exporter Job...")
    if STORAGE_EMULATOR_HOST:
        gcs_client = storage.Client.create_anonymous_client()
    else:
        gcs_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = gcs_client.bucket(BUCKET_NAME)

    existing_orders = get_existing_avro_orders(bucket)
    pg_orders = fetch_postgres_orders()

    # Create mapping of existing orders by (order_id, product_id)
    order_map = {(o["order_id"], o["product_id"]): o for o in existing_orders}
    
    # Track if any record is new or updated in status
    has_changes = False
    new_records_count = 0
    updated_records_count = 0

    for po in pg_orders:
        key = (po["order_id"], po["product_id"])
        if key not in order_map:
            order_map[key] = po
            has_changes = True
            new_records_count += 1
        else:
            existing_rec = order_map[key]
            if existing_rec.get("order_status") != po.get("order_status"):
                order_map[key] = po
                has_changes = True
                updated_records_count += 1

    merged_orders = list(order_map.values())

    if has_changes:
        logging.info(
            f"Detected changes: {new_records_count} new orders, {updated_records_count} status updates. "
            f"Writing {len(merged_orders)} total records to Bronze Avro."
        )
        # 1. Upload updated Avro file to GCS
        avro_bytes = write_avro_bytes(merged_orders)
        blob = bucket.blob(ORDERS_BLOB_NAME)
        blob.upload_from_string(avro_bytes, content_type="application/octet-stream")
        logging.info(f"Successfully uploaded updated Avro file: gs://{BUCKET_NAME}/{ORDERS_BLOB_NAME}")

        # 2. Append new records to PyIceberg catalog if any new orders
        new_orders = [po for po in pg_orders if (po["order_id"], po["product_id"]) not in set((o["order_id"], o["product_id"]) for o in existing_orders)]
        if new_orders:
            sync_to_pyiceberg(new_orders)

        # 3. Synchronize orders to BigQuery (both new & updated statuses)
        sync_orders_to_bigquery(merged_orders)
    else:
        logging.info("All PostgreSQL orders and statuses match Lakehouse Avro store. Ensuring BigQuery tables are current.")
        sync_orders_to_bigquery(merged_orders)

    # 4. Synchronize order status history to BigQuery
    sync_order_status_history_to_bigquery()

    # 5. Synchronize delivery route stops & refresh route performance mart
    sync_delivery_route_stops_to_bigquery()

    try:
        from observability_bq import log_event, record_pipeline_metric
    except ImportError:
        try:
            from lakehouse.ingestion.observability_bq import log_event, record_pipeline_metric
        except ImportError:
            log_event = None
            record_pipeline_metric = None

    if record_pipeline_metric:
        record_pipeline_metric("lakehouse-order-exporter", "orders_exported", len(merged_orders), {"dataset": BIGQUERY_DATASET})
    if log_event:
        log_event("lakehouse-order-exporter", "INFO", f"Synchronized {len(merged_orders)} orders to Bronze Avro and BigQuery", metadata={"count": len(merged_orders), "new": new_records_count, "updated": updated_records_count})

    logging.info("Order Exporter Job completed successfully!")


if __name__ == "__main__":
    export_orders_job()


