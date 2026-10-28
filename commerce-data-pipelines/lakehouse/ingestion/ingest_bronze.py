import csv
import io
import json
import logging
import os
import random
import time
from datetime import datetime, timezone

import clickhouse_connect
import fastavro
from google.cloud import storage
import pyarrow as pa
from pyiceberg.catalog import load_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

STORAGE_EMULATOR_HOST = os.getenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
BUCKET_NAME = "bronze"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

os.environ["STORAGE_EMULATOR_HOST"] = STORAGE_EMULATOR_HOST

SAMPLE_CUSTOMERS = [
    {"customer_id": "CUST-001", "name": "Alice Johnson", "email": "alice@example.com", "phone": "+1-415-555-0101", "address": "1 Ferry Building, The Embarcadero, San Francisco, CA 94111", "latitude": 37.795500, "longitude": -122.393700, "loyalty_tier": "PLATINUM", "region": "WEST"},
    {"customer_id": "CUST-002", "name": "Bob Smith", "email": "bob@example.com", "phone": "+1-415-555-0102", "address": "333 Post St, Union Square, San Francisco, CA 94108", "latitude": 37.788100, "longitude": -122.407500, "loyalty_tier": "GOLD", "region": "WEST"},
    {"customer_id": "CUST-003", "name": "Carol Williams", "email": "carol@example.com", "phone": "+1-415-555-0103", "address": "1 Dr Carlton B Goodlett Pl, Civic Center, San Francisco, CA 94102", "latitude": 37.779300, "longitude": -122.419200, "loyalty_tier": "PLATINUM", "region": "WEST"},
    {"customer_id": "CUST-004", "name": "David Brown", "email": "david@example.com", "phone": "+1-415-555-0104", "address": "1000 Chestnut St, Russian Hill, San Francisco, CA 94109", "latitude": 37.802100, "longitude": -122.419900, "loyalty_tier": "SILVER", "region": "WEST"},
    {"customer_id": "CUST-005", "name": "Eva Martinez", "email": "eva@example.com", "phone": "+1-415-555-0105", "address": "501 Dolores St, Mission District, San Francisco, CA 94110", "latitude": 37.760000, "longitude": -122.427400, "loyalty_tier": "GOLD", "region": "WEST"},
    {"customer_id": "CUST-006", "name": "Frank Miller", "email": "frank@example.com", "phone": "+1-415-555-0106", "address": "24 Willie Mays Plaza, Oracle Park, San Francisco, CA 94107", "latitude": 37.778600, "longitude": -122.389300, "loyalty_tier": "BRONZE", "region": "WEST"},
    {"customer_id": "CUST-007", "name": "Grace Hopper", "email": "grace@example.com", "phone": "+1-415-555-0107", "address": "2001 Fillmore St, Pacific Heights, San Francisco, CA 94115", "latitude": 37.789200, "longitude": -122.433900, "loyalty_tier": "PLATINUM", "region": "WEST"},
    {"customer_id": "CUST-008", "name": "Henry Chen", "email": "henry@example.com", "phone": "+1-415-555-0108", "address": "600 Montgomery St, Transamerica Pyramid, San Francisco, CA 94111", "latitude": 37.795200, "longitude": -122.402800, "loyalty_tier": "GOLD", "region": "WEST"},
    {"customer_id": "CUST-009", "name": "Isabella Rossi", "email": "isabella@example.com", "phone": "+1-415-555-0109", "address": "500 Parnassus Ave, Inner Sunset, San Francisco, CA 94143", "latitude": 37.763100, "longitude": -122.457800, "loyalty_tier": "SILVER", "region": "WEST"},
    {"customer_id": "CUST-010", "name": "James Wilson", "email": "james@example.com", "phone": "+1-415-555-0110", "address": "1000 Great Hwy, Ocean Beach, San Francisco, CA 94121", "latitude": 37.774500, "longitude": -122.511000, "loyalty_tier": "BRONZE", "region": "WEST"},
]

SAMPLE_PRODUCTS = [
    {"product_id": "PROD-001", "product_name": "Ergonomic Mechanical Keyboard", "category": "Electronics", "unit_price": 129.99, "supplier": "KeyTech Labs"},
    {"product_id": "PROD-002", "product_name": "Ultra-Wide Gaming Monitor 34\"", "category": "Electronics", "unit_price": 499.50, "supplier": "VisionCorp"},
    {"product_id": "PROD-003", "product_name": "Wireless Noise Cancelling Headphones", "category": "Audio", "unit_price": 249.00, "supplier": "SoundWave"},
    {"product_id": "PROD-004", "product_name": "Smart Fitness Smartwatch v2", "category": "Wearables", "unit_price": 199.99, "supplier": "PulseTech"},
    {"product_id": "PROD-005", "product_name": "Aerodynamic Carbon Fiber Road Bike", "category": "Sports", "unit_price": 1250.00, "supplier": "VeloMotion"},
    {"product_id": "PROD-006", "product_name": "Precision Ergonomic Wireless Mouse", "category": "Electronics", "unit_price": 79.99, "supplier": "KeyTech Labs"},
    {"product_id": "PROD-007", "product_name": "Studio USB Condenser Microphone", "category": "Audio", "unit_price": 149.50, "supplier": "SoundWave"},
    {"product_id": "PROD-008", "product_name": "4K Ultra-HD Streaming Webcam", "category": "Electronics", "unit_price": 119.00, "supplier": "VisionCorp"},
    {"product_id": "PROD-009", "product_name": "Standing Desk Converter Motorized", "category": "Office", "unit_price": 349.00, "supplier": "WorkErgo Systems"},
    {"product_id": "PROD-010", "product_name": "Stainless Steel Insulated Smart Bottle", "category": "Fitness", "unit_price": 45.00, "supplier": "PulseTech"},
]

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


def ensure_bucket(client: storage.Client, bucket_name: str):
    try:
        bucket = client.get_bucket(bucket_name)
    except Exception:
        bucket = client.create_bucket(bucket_name)
        logging.info(f"Created GCS bucket: {bucket_name}")
    return bucket


def generate_batch_orders(count: int = 50):
    orders = []
    now = datetime.now(timezone.utc).isoformat()
    for i in range(count):
        cust = random.choice(SAMPLE_CUSTOMERS)
        prod = random.choice(SAMPLE_PRODUCTS)
        qty = random.randint(1, 4)
        total = round(qty * prod["unit_price"], 2)
        status = random.choice(["COMPLETED", "DELIVERED", "PROCESSING", "SHIPPED"])
        order = {
            "order_id": f"ORD-AVRO-{int(time.time())}-{i:04d}",
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": prod["unit_price"],
            "total_amount": total,
            "order_status": status,
            "order_timestamp": now,
        }
        orders.append(order)
    return orders


def write_avro_orders(orders):
    out = io.BytesIO()
    fastavro.writer(out, ORDER_AVRO_SCHEMA, orders)
    return out.getvalue()


def write_csv_dimension(records, fieldnames):
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)
    return out.getvalue().encode("utf-8")


def sync_pyiceberg_orders(orders):
    """Sync orders to local Iceberg warehouse catalog."""
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
    arrow_table = pa.Table.from_pylist(orders, schema=arrow_schema)
    table.append(arrow_table)
    logging.info(f"Updated Iceberg Catalog table: bronze.orders at {table.location()}")


def run():
    logging.info(f"Connecting to Cloud Storage Emulator at {STORAGE_EMULATOR_HOST}...")
    client = storage.Client.create_anonymous_client()
    bucket = ensure_bucket(client, BUCKET_NAME)

    # 1. Upload CSV Dimensions
    cust_fields = ["customer_id", "name", "email", "phone", "address", "latitude", "longitude", "loyalty_tier", "region"]
    cust_bytes = write_csv_dimension(SAMPLE_CUSTOMERS, cust_fields)
    blob_cust = bucket.blob("customers/customers_latest.csv")
    blob_cust.upload_from_string(cust_bytes, content_type="text/csv")
    logging.info(f"Uploaded CSV Customers: gs://{BUCKET_NAME}/customers/customers_latest.csv")

    prod_bytes = write_csv_dimension(SAMPLE_PRODUCTS, ["product_id", "product_name", "category", "unit_price", "supplier"])
    blob_prod = bucket.blob("products/products_latest.csv")
    blob_prod.upload_from_string(prod_bytes, content_type="text/csv")
    logging.info(f"Uploaded CSV Products: gs://{BUCKET_NAME}/products/products_latest.csv")

    # 2. Orders Avro: ensure an empty Avro file exists if not already present, without generating fake orders
    blob_orders = bucket.blob("orders/orders_latest.avro")
    if not blob_orders.exists():
        empty_avro_bytes = write_avro_orders([])
        blob_orders.upload_from_string(empty_avro_bytes, content_type="application/octet-stream")
        logging.info(f"Initialized clean 0-record Avro Orders: gs://{BUCKET_NAME}/orders/orders_latest.avro")
    else:
        logging.info("Existing orders/orders_latest.avro found, preserving genuine orders.")

    # 4. Initialize ClickHouse external tables / views
    logging.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password="")

    ch.command("CREATE DATABASE IF NOT EXISTS lakehouse;")
    ch.command("CREATE DATABASE IF NOT EXISTS lakehouse_silver;")

    # External View for Avro Orders
    ch.command("""
        CREATE OR REPLACE VIEW lakehouse.bronze_orders AS
        SELECT *
        FROM url('http://fake-gcs-server:4443/storage/v1/b/bronze/o/orders%2Forders_latest.avro?alt=media', 'Avro');
    """)
    logging.info("Created ClickHouse External View: lakehouse.bronze_orders (Avro engine)")

    # External View for CSV Customers
    ch.command("""
        CREATE OR REPLACE VIEW lakehouse.bronze_customers AS
        SELECT *
        FROM url('http://fake-gcs-server:4443/storage/v1/b/bronze/o/customers%2Fcustomers_latest.csv?alt=media', 'CSVWithNames');
    """)
    logging.info("Created ClickHouse External View: lakehouse.bronze_customers (CSV engine)")

    # External View for CSV Products
    ch.command("""
        CREATE OR REPLACE VIEW lakehouse.bronze_products AS
        SELECT *
        FROM url('http://fake-gcs-server:4443/storage/v1/b/bronze/o/products%2Fproducts_latest.csv?alt=media', 'CSVWithNames');
    """)
    logging.info("Created ClickHouse External View: lakehouse.bronze_products (CSV engine)")

    logging.info("Lakehouse Bronze Ingestion and External Tables successfully configured!")


if __name__ == "__main__":
    run()
