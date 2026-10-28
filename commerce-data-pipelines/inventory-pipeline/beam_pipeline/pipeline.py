import base64
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Environment configurations
PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
INVENTORY_TOPIC = os.getenv("INVENTORY_TOPIC", "inventory-updates")
INVENTORY_SUBSCRIPTION = os.getenv("INVENTORY_SUBSCRIPTION", "inventory-beam-sub")

DATABASE_URL = os.getenv("DATABASE_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
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

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Ensure client library talks to emulator
os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST

DEAD_LETTER_TAG = "dead_letter"

COMMERCE_API_GATEWAY_URL = os.getenv("COMMERCE_API_URL", "http://commerce-api-gateway:8002/api")

def emit_observability_log(service_name: str, log_level: str, message: str, trace_id: str = "", metadata: Optional[Dict[str, Any]] = None):
    """Sends structured log event to commerce-api gateway for streaming into BigQuery application_logs."""
    try:
        import requests
        requests.post(
            f"{COMMERCE_API_GATEWAY_URL}/observability/log",
            json={
                "service_name": service_name,
                "log_level": log_level,
                "message": message,
                "trace_id": trace_id,
                "metadata": metadata or {},
            },
            timeout=2.0
        )
    except Exception:
        pass

def emit_observability_metric(pipeline_name: str, metric_name: str, metric_value: float, labels: Optional[Dict[str, Any]] = None):
    """Sends pipeline metric event to commerce-api gateway for streaming into BigQuery pipeline_metrics."""
    try:
        import requests
        requests.post(
            f"{COMMERCE_API_GATEWAY_URL}/observability/metric",
            json={
                "pipeline_name": pipeline_name,
                "metric_name": metric_name,
                "metric_value": float(metric_value),
                "labels": labels or {},
            },
            timeout=2.0
        )
    except Exception:
        pass


def setup_pubsub_resources():
    """Ensure topic and pull subscription exist on the Pub/Sub emulator before Beam attaches."""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(PROJECT_ID, INVENTORY_TOPIC)
    sub_path = subscriber.subscription_path(PROJECT_ID, INVENTORY_SUBSCRIPTION)

    # 1. Topic
    try:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Created topic: {topic_path}")
    except AlreadyExists:
        pass
    except Exception as e:
        logging.info(f"Topic check: {e}")

    # 2. Pull Subscription for Beam
    try:
        subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic_path,
                "ack_deadline_seconds": 60,
            }
        )
        logging.info(f"Created Pull Subscription for Beam: {sub_path}")
    except AlreadyExists:
        logging.info(f"Subscription {sub_path} already exists.")
    except Exception as e:
        logging.warning(f"Subscription check: {e}")


def init_database_tables():
    """Ensure PostgreSQL idempotency tracking and inventory tables exist."""
    conn = get_pg_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS inventory.processed_inventory_events (
                    event_id VARCHAR(128) PRIMARY KEY,
                    product_id VARCHAR(64) NOT NULL,
                    warehouse_id VARCHAR(64) NOT NULL,
                    processed_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS inventory.inventory_levels (
                    product_id VARCHAR(64) NOT NULL,
                    warehouse_id VARCHAR(64) NOT NULL,
                    product_name VARCHAR(128),
                    category VARCHAR(64),
                    quantity_available INT NOT NULL DEFAULT 0,
                    reserved_quantity INT NOT NULL DEFAULT 0,
                    safety_stock INT NOT NULL DEFAULT 10,
                    unit_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
                    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (product_id, warehouse_id)
                );

                CREATE TABLE IF NOT EXISTS inventory.inventory_audit_log (
                    audit_id SERIAL PRIMARY KEY,
                    product_id VARCHAR(64) NOT NULL,
                    warehouse_id VARCHAR(64) NOT NULL,
                    change_type VARCHAR(32) NOT NULL,
                    quantity_change INT NOT NULL,
                    quantity_after INT NOT NULL,
                    reason TEXT,
                    event_timestamp TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS delivery.dead_letter_messages (
                    id SERIAL PRIMARY KEY,
                    raw_payload TEXT,
                    error_message TEXT,
                    received_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
    conn.close()
    logging.info("Initialized inventory database tables and idempotency store.")


class ParseAndValidateInventoryEventDoFn(beam.DoFn):
    """
    Step 1: Parses raw bytes from Pub/Sub and validates schema.
    Emits valid events to main output, routing corrupt/malformed payloads to the DLQ tag.
    """

    def process(self, element: bytes):
        raw_str = ""
        try:
            raw_str = element.decode("utf-8")
            payload = json.loads(raw_str)

            if not isinstance(payload, dict):
                raise ValueError("Payload must be a JSON dictionary")

            # Validate required fields
            event_id = payload.get("event_id")
            product_id = payload.get("product_id")
            warehouse_id = payload.get("warehouse_id")

            if not event_id or not product_id or not warehouse_id:
                raise ValueError(f"Missing required fields (event_id, product_id, warehouse_id): {payload}")

            yield payload

        except Exception as e:
            logging.error(f"DLQ: Failed to parse inventory message: {e} | Raw: {raw_str}")
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                {"raw_payload": raw_str, "error_message": str(e)},
            )


class IdempotentInventoryProcessorDoFn(beam.DoFn):
    """
    Step 2: Core Idempotency and ACID Transaction Sink.
    
    Guarantees:
    1. Idempotency: Checks `processed_inventory_events` table before modifying stock.
       If `event_id` was already processed (at-least-once replay), drops the duplicate safely.
    2. Pessimistic Row Lock: Uses `SELECT ... FOR UPDATE` on `inventory_levels` by (product_id, warehouse_id).
    3. Audit Trail: Appends record to `inventory_audit_log`.
    4. Cache Coherency: Atomically writes updated state to Redis (`stock:{product_id}`, `product:{product_id}`, `stock_total:{product_id}`).
    """

    def __init__(self, pg_host: str, pg_port: int, pg_user: str, pg_password: str, pg_db: str, redis_host: str, redis_port: int, database_url: Optional[str] = None, sslmode: Optional[str] = None):
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_db = pg_db
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.database_url = database_url
        self.sslmode = sslmode
        self.pg_conn = None
        self.redis_client = None

    def setup(self):
        if self.database_url:
            self.pg_conn = psycopg2.connect(self.database_url)
        else:
            kwargs = {
                "host": self.pg_host,
                "port": self.pg_port,
                "user": self.pg_user,
                "password": self.pg_password,
                "dbname": self.pg_db,
            }
            if self.sslmode:
                kwargs["sslmode"] = self.sslmode
            self.pg_conn = psycopg2.connect(**kwargs)
        self.pg_conn.autocommit = False
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True,
        )
        logging.info("Beam Worker: Connected to Postgres and Redis.")

    def teardown(self):
        if self.pg_conn:
            try:
                self.pg_conn.close()
            except Exception:
                pass
        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception:
                pass

    def process(self, event: Dict[str, Any]):
        event_id = event["event_id"]
        product_id = event["product_id"]
        warehouse_id = event["warehouse_id"]
        product_name = event.get("product_name", "Unknown Product")
        category = event.get("category", "General")
        unit_price = float(event.get("unit_price", 0.0))
        change_type = event.get("change_type", "UPDATE")
        quantity_change = int(event.get("quantity_change", 0))
        reason = event.get("reason", "")

        try:
            with self.pg_conn:
                with self.pg_conn.cursor() as cur:
                    # 1. IDEMPOTENCY CHECK: Atomic reservation of event_id
                    cur.execute(
                        """
                        INSERT INTO inventory.processed_inventory_events (event_id, product_id, warehouse_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING;
                        """,
                        (event_id, product_id, warehouse_id),
                    )
                    # If 0 rows affected, event_id already exists -> skip to prevent duplicate double-counting
                    if cur.rowcount == 0:
                        logging.info(f"⚡ [Beam Idempotency] Skipping duplicate event_id: {event_id} ({product_id} @ {warehouse_id})")
                        return

                    # 2. Ensure row exists for pessimistic lock
                    cur.execute(
                        """
                        INSERT INTO inventory.inventory_levels (
                            product_id, warehouse_id, product_name, category, 
                            quantity_available, unit_price, last_updated_at
                        )
                        VALUES (%s, %s, %s, %s, 0, %s, NOW())
                        ON CONFLICT (product_id, warehouse_id) DO NOTHING;
                        """,
                        (product_id, warehouse_id, product_name, category, unit_price),
                    )

                    # 3. PESSIMISTIC LOCK: Lock specific (product_id, warehouse_id)
                    cur.execute(
                        """
                        SELECT quantity_available 
                        FROM inventory.inventory_levels 
                        WHERE product_id = %s AND warehouse_id = %s
                        FOR UPDATE;
                        """,
                        (product_id, warehouse_id),
                    )
                    row = cur.fetchone()
                    current_qty = row[0] if row else 0

                    # 4. Bounds check: clamp to 0 if stock underflows
                    new_qty = current_qty + quantity_change
                    if new_qty < 0:
                        logging.warning(
                            f"Insufficient stock for {product_id} at {warehouse_id}. "
                            f"Current: {current_qty}, Requested: {quantity_change}. Clamped to 0."
                        )
                        new_qty = 0
                        actual_delta = -current_qty
                    else:
                        actual_delta = quantity_change

                    # 5. Update inventory_levels
                    cur.execute(
                        """
                        UPDATE inventory.inventory_levels
                        SET quantity_available = %s,
                            unit_price = %s,
                            product_name = %s,
                            category = %s,
                            last_updated_at = NOW()
                        WHERE product_id = %s AND warehouse_id = %s;
                        """,
                        (new_qty, unit_price, product_name, category, product_id, warehouse_id),
                    )

                    # 6. Insert audit trail
                    cur.execute(
                        """
                        INSERT INTO inventory.inventory_audit_log (
                            product_id, warehouse_id, change_type, quantity_change,
                            quantity_after, reason, event_timestamp
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NOW());
                        """,
                        (product_id, warehouse_id, change_type, actual_delta, new_qty, reason),
                    )

            # 7. Commit succeeded -> Update cache in Redis
            if self.redis_client:
                stock_key = f"stock:{product_id}"
                meta_key = f"product:{product_id}"

                self.redis_client.hset(stock_key, warehouse_id, new_qty)
                self.redis_client.hset(
                    meta_key,
                    mapping={
                        "product_id": product_id,
                        "product_name": product_name,
                        "category": category,
                        "unit_price": str(unit_price),
                    },
                )
                warehouse_stocks = self.redis_client.hgetall(stock_key)
                total_qty = sum(
                    int(v) for v in warehouse_stocks.values()
                    if v.isdigit() or (v.startswith('-') and v[1:].isdigit())
                )
                self.redis_client.set(f"stock_total:{product_id}", total_qty)

                logging.info(
                    f"🚀 [Beam Streaming | Idempotent] Processed Event: {event_id} | "
                    f"Product: {product_id} @ {warehouse_id} | Delta: {actual_delta:+d} | "
                    f"New Qty: {new_qty} | Total Stock: {total_qty}"
                )
                emit_observability_log(
                    service_name="inventory-beam-pipeline",
                    log_level="INFO",
                    message=f"Streamed inventory event {event_id} for {product_id} @ {warehouse_id} (delta: {actual_delta:+d}, new_qty: {new_qty})",
                    trace_id=f"beam_{event_id}",
                    metadata={"event_id": event_id, "product_id": product_id, "warehouse_id": warehouse_id, "delta": actual_delta, "new_qty": new_qty, "total_stock": total_qty}
                )
                emit_observability_metric(
                    pipeline_name="inventory-beam-pipeline",
                    metric_name="inventory_events_processed",
                    metric_value=float(abs(actual_delta)),
                    labels={"product_id": product_id, "warehouse_id": warehouse_id, "change_type": change_type}
                )

        except Exception as e:
            logging.error(f"Error processing event {event_id}: {e}")
            try:
                self.pg_conn.rollback()
            except Exception:
                pass
            raise e


class WriteToDLQDoFn(beam.DoFn):
    """Writes corrupt or unparseable messages to the PostgreSQL dead_letter_messages table."""

    def __init__(self, host: str, port: int, user: str, password: str, db: str, database_url: Optional[str] = None, sslmode: Optional[str] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.database_url = database_url
        self.sslmode = sslmode
        self.conn = None

    def setup(self):
        if self.database_url:
            self.conn = psycopg2.connect(self.database_url)
        else:
            kwargs = {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "password": self.password,
                "dbname": self.db,
            }
            if self.sslmode:
                kwargs["sslmode"] = self.sslmode
            self.conn = psycopg2.connect(**kwargs)

    def teardown(self):
        if self.conn:
            self.conn.close()

    def process(self, element: Dict[str, Any]):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dead_letter_messages (raw_payload, error_message, received_at)
                        VALUES (%s, %s, NOW());
                        """,
                        (element.get("raw_payload", ""), element.get("error_message", "")),
                    )
            logging.warning(f"DLQ Recorded corrupt inventory payload into Postgres: {element.get('error_message')}")
        except Exception as e:
            logging.error(f"Failed to record inventory DLQ entry: {e}")


def run():
    logging.info("Initializing Inventory Dataflow/Beam Streaming Pipeline...")

    # Wait for dependencies and set up tables & Pub/Sub subscription
    retries = 10
    while retries > 0:
        try:
            setup_pubsub_resources()
            init_database_tables()
            break
        except Exception as e:
            logging.warning(f"Waiting for Pub/Sub and Postgres readiness ({retries} retries left): {e}")
            retries -= 1
            time.sleep(3)

    pipeline_options = PipelineOptions(
        runner="DirectRunner",
        streaming=True,
    )
    pipeline_options.view_as(StandardOptions).streaming = True

    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{INVENTORY_SUBSCRIPTION}"
    logging.info(f"Beam pipeline listening continuously to: {subscription_path}")

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Ingest raw streaming stream from Pub/Sub
        raw_events = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=subscription_path)
        )

        # 2. Parse and route corrupt messages to DLQ side-output
        parsed_events, dead_letters = (
            raw_events
            | "ParseAndValidate"
            >> beam.ParDo(ParseAndValidateInventoryEventDoFn()).with_outputs(
                DEAD_LETTER_TAG,
                main="valid_events",
            )
        )

        # 3. Drain corrupt messages to Postgres DLQ
        _ = (
            dead_letters
            | "WriteDLQToPostgres"
            >> beam.ParDo(
                WriteToDLQDoFn(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    db=POSTGRES_DB,
                    database_url=DATABASE_URL,
                    sslmode=POSTGRES_SSLMODE,
                )
            )
        )

        # 4. Process valid inventory events with strict idempotency and atomic DB/Redis commits
        _ = (
            parsed_events
            | "IdempotentInventoryProcessor"
            >> beam.ParDo(
                IdempotentInventoryProcessorDoFn(
                    pg_host=POSTGRES_HOST,
                    pg_port=POSTGRES_PORT,
                    pg_user=POSTGRES_USER,
                    pg_password=POSTGRES_PASSWORD,
                    pg_db=POSTGRES_DB,
                    redis_host=REDIS_HOST,
                    redis_port=REDIS_PORT,
                    database_url=DATABASE_URL,
                    sslmode=POSTGRES_SSLMODE,
                )
            )
        )


if __name__ == "__main__":
    run()
