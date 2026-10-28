import json
import logging
import os
import random
import time
from typing import Dict, Any, List, Optional

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PUBSUB_PROJECT_ID = os.getenv("PUBSUB_PROJECT_ID", "test-project")
PUBSUB_TOPIC_ID = os.getenv("INVENTORY_TOPIC_ID", "inventory-updates")
PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")

DATABASE_URL = os.getenv("DATABASE_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "commercedb")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE")

os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST

WAREHOUSES = ["WH-WEST-01", "WH-EAST-02", "WH-CENTRAL-01", "WH-PACIFIC-03"]
CHANGE_TYPES = ["CYCLE_COUNT", "RESERVATION", "RETURN"]


def get_pg_connection():
    """Returns a connection to database with search_path prioritizing inventory and catalog."""
    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL)
    else:
        kwargs = {
            "host": POSTGRES_HOST,
            "port": POSTGRES_PORT,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "dbname": POSTGRES_DB,
        }
        if POSTGRES_SSLMODE:
            kwargs["sslmode"] = POSTGRES_SSLMODE
        conn = psycopg2.connect(**kwargs)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO inventory, catalog, public;")
    return conn


def ensure_topic(publisher: pubsub_v1.PublisherClient, topic_path: str):
    try:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Created topic: {topic_path}")
    except AlreadyExists:
        pass
    except Exception as e:
        logging.warning(f"Topic creation check: {e}")


def fetch_product_catalog() -> List[Dict[str, Any]]:
    """Fetches the latest products from PostgreSQL catalog.product_catalog."""
    try:
        with get_pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT product_id, product_name, category, COALESCE(unit_price, 0.0) as unit_price
                    FROM catalog.product_catalog
                    ORDER BY product_id;
                """)
                products = [dict(row) for row in cur.fetchall()]
                for p in products:
                    p["unit_price"] = float(p["unit_price"])
                return products
    except Exception as e:
        logging.error(f"Failed to fetch product catalog from PostgreSQL: {e}")
        return []


def fetch_inventory_state() -> List[Dict[str, Any]]:
    """Fetches the current inventory levels across warehouses from inventory.inventory_levels."""
    try:
        with get_pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT product_id, warehouse_id, product_name, category,
                           quantity_available, safety_stock, unit_price
                    FROM inventory.inventory_levels;
                """)
                rows = [dict(row) for row in cur.fetchall()]
                for r in rows:
                    r["unit_price"] = float(r["unit_price"])
                return rows
    except Exception as e:
        logging.error(f"Failed to fetch inventory levels: {e}")
        return []


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


def publish_event(publisher: pubsub_v1.PublisherClient, topic_path: str, event: Dict[str, Any]) -> str:
    """Serializes and publishes an inventory event to Pub/Sub."""
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    msg_id = future.result()

    # Emit telemetry to BigQuery
    emit_observability_log(
        service_name="inventory-pipeline",
        log_level="INFO",
        message=f"Published [{event.get('change_type')}] for {event.get('product_id')} at {event.get('warehouse_id')} (delta: {event.get('quantity_change', 0):+d})",
        trace_id=f"pub_{event.get('product_id')}",
        metadata=event
    )
    emit_observability_metric(
        pipeline_name="inventory-pipeline",
        metric_name="inventory_event_published",
        metric_value=float(abs(event.get("quantity_change", 1))),
        labels={
            "change_type": event.get("change_type"),
            "product_id": event.get("product_id"),
            "warehouse_id": event.get("warehouse_id"),
        }
    )

    return msg_id


def check_and_seed_new_products(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    catalog_products: List[Dict[str, Any]],
    current_inventory: List[Dict[str, Any]],
):
    """
    Detects any (product_id, warehouse_id) combination missing from inventory.inventory_levels.
    For each missing product and warehouse, seeds initial baseline stock.
    """
    existing_pairs = {(row["product_id"], row["warehouse_id"]) for row in current_inventory}
    
    for prod in catalog_products:
        pid = prod["product_id"]
        for wh in WAREHOUSES:
            if (pid, wh) not in existing_pairs:
                init_qty = random.randint(50, 150)
                seed_event = {
                    "event_id": f"seed_{pid}_{wh}",
                    "product_id": pid,
                    "product_name": prod["product_name"],
                    "category": prod["category"],
                    "unit_price": prod["unit_price"],
                    "warehouse_id": wh,
                    "change_type": "INITIAL_SEED",
                    "quantity_change": init_qty,
                    "reason": f"Baseline inventory initialization for catalog product {pid} at {wh}",
                    "timestamp": time.time(),
                }
                msg_id = publish_event(publisher, topic_path, seed_event)
                logging.info(f"🆕 Published INITIAL_SEED for {pid} ({prod['product_name']}) at {wh}: +{init_qty} units (MsgID: {msg_id})")
                existing_pairs.add((pid, wh))


def check_and_replenish_safety_stock(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    current_inventory: List[Dict[str, Any]],
    catalog_by_id: Dict[str, Dict[str, Any]],
):
    """
    Checks all existing warehouse inventory levels.
    Only triggers replenishment when quantity_available falls below the restock/safety threshold.
    Replenishes an optimal, disciplined order amount (~10 units).
    """
    OPTIMAL_REPLENISHMENT_QTY = 10

    for item in current_inventory:
        pid = item["product_id"]
        wh = item["warehouse_id"]
        qty = item["quantity_available"]
        safety = item["safety_stock"]

        # Only replenish when product stock goes below or at restock threshold (safety_stock)
        if qty <= safety:
            meta = catalog_by_id.get(pid, item)
            # Optimal replenishment batch: targeted ~10 units (or calculated deficit to reach safety + 10)
            replenish_qty = max(OPTIMAL_REPLENISHMENT_QTY, (safety - qty) + OPTIMAL_REPLENISHMENT_QTY)
            replenish_event = {
                "event_id": f"replenish_{pid}_{wh}_{int(time.time()*1000)}",
                "product_id": pid,
                "product_name": meta.get("product_name", item.get("product_name")),
                "category": meta.get("category", item.get("category")),
                "unit_price": meta.get("unit_price", item.get("unit_price")),
                "warehouse_id": wh,
                "change_type": "RESTOCK",
                "quantity_change": replenish_qty,
                "reason": f"Optimal replenishment: stock ({qty}) fell below restock threshold ({safety}), ordered optimal batch of {replenish_qty} units",
                "timestamp": time.time(),
            }
            msg_id = publish_event(publisher, topic_path, replenish_event)
            logging.warning(
                f"🚨 [RESTOCK THRESHOLD TRIGGER] Replenishing {pid} at {wh}! "
                f"Current: {qty} <= Restock Threshold: {safety}. Optimal order: +{replenish_qty} units (MsgID: {msg_id})"
            )


def generate_routine_movement_event(catalog_products: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Generates routine operational warehouse movements (cycle counts, returns, reservations).
    Replenishments/restocks are strictly triggered when stock falls below threshold.
    """
    if not catalog_products:
        return None

    prod = random.choice(catalog_products)
    wh = random.choice(WAREHOUSES)
    change_type = random.choice(CHANGE_TYPES)

    if change_type == "CYCLE_COUNT":
        delta = random.choice([-2, -1, 1, 2])
        reason = "Warehouse manual cycle count adjustment"
    elif change_type == "RETURN":
        delta = random.randint(1, 2)
        reason = "Customer return inspected and restocked"
    else:  # RESERVATION
        delta = -random.randint(1, 3)
        reason = "Inventory hold for online order placement"

    return {
        "event_id": f"inv_evt_{int(time.time()*1000)}_{random.randint(100, 999)}",
        "product_id": prod["product_id"],
        "product_name": prod["product_name"],
        "category": prod["category"],
        "unit_price": prod["unit_price"],
        "warehouse_id": wh,
        "change_type": change_type,
        "quantity_change": delta,
        "reason": reason,
        "timestamp": time.time(),
    }


def main():
    logging.info(f"Starting Intelligent Dynamic Inventory Simulator on {PUBSUB_EMULATOR_HOST}...")
    logging.info(f"Connected to PostgreSQL database '{POSTGRES_DB}' at {POSTGRES_HOST}:{POSTGRES_PORT}")
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, PUBSUB_TOPIC_ID)
    ensure_topic(publisher, topic_path)

    cycle_count = 0

    try:
        while True:
            cycle_count += 1
            # 1. Fetch latest state of product catalog and warehouse inventory
            catalog_products = fetch_product_catalog()
            current_inventory = fetch_inventory_state()
            catalog_by_id = {p["product_id"]: p for p in catalog_products}

            if not catalog_products:
                logging.warning("No products found in catalog.product_catalog. Waiting 5s before retry...")
                time.sleep(5)
                continue

            # 2. Check for newly introduced catalog products and seed them into warehouses
            check_and_seed_new_products(publisher, topic_path, catalog_products, current_inventory)

            # 3. Check for any warehouse stock below safety_stock and replenish immediately
            check_and_replenish_safety_stock(publisher, topic_path, current_inventory, catalog_by_id)

            # 4. Generate routine warehouse movements (every cycle)
            evt = generate_routine_movement_event(catalog_products)
            if evt:
                msg_id = publish_event(publisher, topic_path, evt)
                logging.info(
                    f"Published [{evt['change_type']}] for {evt['product_id']} ({evt['product_name']}) "
                    f"at {evt['warehouse_id']}: delta={evt['quantity_change']:+d} (MsgID: {msg_id})"
                )

            time.sleep(random.uniform(3.0, 5.0))

    except KeyboardInterrupt:
        logging.info("Inventory simulator stopped.")


if __name__ == "__main__":
    main()

