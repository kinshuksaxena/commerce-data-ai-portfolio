import datetime
import json
import logging
import os
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from google.cloud import pubsub_v1
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

router = APIRouter(prefix="/api", tags=["Commerce API"])

# Database & Cache Configurations
DATABASE_URL = os.getenv("DATABASE_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "pgbouncer")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "commercedb")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
PUBSUB_PROJECT_ID = os.getenv("PUBSUB_PROJECT_ID", "test-project")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "commerce_analytics")
DELIVERY_TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "stream-topic")
INVENTORY_TOPIC_ID = os.getenv("INVENTORY_TOPIC_ID", "inventory-updates")

os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST

redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_redis_client():
    return redis.Redis(connection_pool=redis_pool)


def get_pg_connection():
    """Connects to PostgreSQL (Neon or local)."""
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


# ============================================================================
# 1. BROWSE API (Product Catalog & Analytics)
# ============================================================================
@router.get("/catalog/products", summary="Browse product catalog with optional search & category filter")
def list_products(
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search product name"),
    limit: int = Query(20, ge=1, le=100),
):
    query = """
        SELECT 
            product_id,
            product_name,
            category,
            unit_price,
            total_units_sold,
            total_gross_revenue,
            last_synced_at
        FROM product_catalog
        WHERE (%(category)s IS NULL OR category ILIKE %(category_like)s)
          AND (%(search)s IS NULL OR product_name ILIKE %(search_like)s)
        ORDER BY total_units_sold DESC, product_id ASC
        LIMIT %(limit)s;
    """
    params = {
        "category": category,
        "category_like": f"%{category}%" if category else None,
        "search": search,
        "search_like": f"%{search}%" if search else None,
        "limit": limit,
    }
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            products = cur.fetchall()

    return {"count": len(products), "products": products}


@router.get("/catalog/categories", summary="List all distinct catalog categories")
def list_categories():
    query = "SELECT DISTINCT category FROM product_catalog ORDER BY category ASC;"
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            categories = [r[0] for r in cur.fetchall()]
    return {"categories": categories}


@router.get("/customers", summary="List all registered customers for order placement")
def list_customers():
    """Returns available customer profiles with addresses for ordering."""
    query = """
        SELECT customer_id, name, address, COALESCE(phone, '+1-555-0100') AS phone,
               latitude, longitude
        FROM customers.customers
        ORDER BY customer_id ASC;
    """
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            customers = cur.fetchall()
            for c in customers:
                if c.get("latitude") is not None:
                    c["latitude"] = float(c["latitude"])
                if c.get("longitude") is not None:
                    c["longitude"] = float(c["longitude"])
    return {"count": len(customers), "customers": customers}


@router.get("/catalog/products/{product_id}", summary="Get detailed product metadata")
def get_product_details(product_id: str):
    # Try Redis cache first
    r = get_redis_client()
    meta = r.hgetall(f"product:{product_id}")
    if meta:
        stock_total = r.get(f"stock_total:{product_id}")
        meta["total_stock_available"] = int(stock_total) if stock_total else 0
        return {"source": "cache", "product": meta}

    # Fallback to Postgres
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT product_id, product_name, category, unit_price,
                       total_units_sold, total_gross_revenue
                FROM product_catalog WHERE product_id = %s;
                """,
                (product_id,),
            )
            prod = cur.fetchone()
            if not prod:
                raise HTTPException(status_code=404, detail=f"Product {product_id} not found")

    return {"source": "database", "product": prod}


# ============================================================================
# 2. INVENTORY API (Sub-millisecond Redis Lookup with Warehouse Distribution)
# ============================================================================
@router.get("/inventory/{product_id}", summary="Check real-time stock availability across warehouses")
def get_product_inventory(product_id: str):
    r = get_redis_client()
    stock_key = f"stock:{product_id}"
    warehouse_stock = r.hgetall(stock_key)
    
    if warehouse_stock:
        total = sum(int(v) for v in warehouse_stock.values() if v.isdigit())
        return {
            "source": "redis_cache",
            "product_id": product_id,
            "total_available": total,
            "warehouse_breakdown": {wh: int(qty) for wh, qty in warehouse_stock.items()},
        }

    # Fallback to PostgreSQL inventory_levels
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT warehouse_id, quantity_available
                FROM inventory_levels
                WHERE product_id = %s;
                """,
                (product_id,),
            )
            rows = cur.fetchall()

    if not rows:
        return {
            "source": "database",
            "product_id": product_id,
            "total_available": 0,
            "warehouse_breakdown": {},
        }

    breakdown = {r["warehouse_id"]: r["quantity_available"] for r in rows}
    total = sum(breakdown.values())
    return {
        "source": "database",
        "product_id": product_id,
        "total_available": total,
        "warehouse_breakdown": breakdown,
    }


class InventoryPublishRequest(BaseModel):
    product_id: str
    warehouse_id: str = Field("WH-WEST-01", description="Target warehouse ID")
    change_type: str = Field("RESTOCK", description="RESTOCK, CYCLE_COUNT, RESERVATION, RETURN, MANUAL_ADJUST")
    quantity_change: int = Field(..., description="Quantity delta to add/subtract, or positive restock amount")
    reason: Optional[str] = Field("Manual adjustment via Commerce Studio", description="Reason for the change")


@router.post("/inventory/publish", summary="Publish inventory level update event via Pub/Sub streaming pipeline")
def publish_inventory_event(req: InventoryPublishRequest):
    # 1. Fetch product metadata from catalog to populate event
    product_name = "Unknown Product"
    category = "General"
    unit_price = 0.0

    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT product_name, category, COALESCE(unit_price, 0.0) as unit_price FROM catalog.product_catalog WHERE product_id = %s;",
                (req.product_id,),
            )
            prod = cur.fetchone()
            if prod:
                product_name = prod["product_name"]
                category = prod["category"]
                unit_price = float(prod["unit_price"])

    # 2. Build inventory update event payload compatible with Apache Beam pipeline
    event_id = f"pub_{req.product_id}_{req.warehouse_id}_{uuid.uuid4().hex[:8]}"
    event_payload = {
        "event_id": event_id,
        "product_id": req.product_id,
        "product_name": product_name,
        "category": category,
        "unit_price": unit_price,
        "warehouse_id": req.warehouse_id,
        "change_type": req.change_type.upper(),
        "quantity_change": req.quantity_change,
        "reason": req.reason,
        "timestamp": os.getenv("MOCK_TIME", None) or 0,
    }

    # 3. Publish to Pub/Sub inventory-updates topic
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, INVENTORY_TOPIC_ID)
        try:
            publisher.create_topic(request={"name": topic_path})
        except Exception:
            pass  # Already exists or not allowed

        data = json.dumps(event_payload).encode("utf-8")
        future = publisher.publish(topic_path, data)
        msg_id = future.result(timeout=5)
        logging.info(f"Published inventory event {event_id} (msg_id: {msg_id}) to topic {topic_path}")

        # Emit BigQuery structured log & telemetry metric for inventory pipeline
        emit_log(
            "inventory-pipeline",
            "INFO",
            f"Published {req.change_type} event for {req.product_id} at {req.warehouse_id} (delta: {req.quantity_change:+d})",
            trace_id=event_id[:12],
            metadata=event_payload
        )
        emit_metric(
            "inventory-pipeline",
            "inventory_event_published",
            float(req.quantity_change),
            {"product_id": req.product_id, "warehouse_id": req.warehouse_id, "change_type": req.change_type}
        )
    except Exception as e:
        logging.error(f"Failed to publish inventory update event: {e}")
        emit_log(
            "inventory-pipeline",
            "ERROR",
            f"Failed to publish inventory update for {req.product_id}: {str(e)}",
            trace_id=event_id[:12],
            metadata={"product_id": req.product_id, "error": str(e)}
        )
        raise HTTPException(status_code=500, detail=f"Pub/Sub publishing error: {str(e)}")

    return {
        "status": "PUBLISHED",
        "message_id": msg_id,
        "event_id": event_id,
        "event": event_payload,
    }


# ============================================================================
# 3. ORDER MANAGEMENT API (Atomic Checkout & Stock Reservation)
# ============================================================================
# ============================================================================
# 3. ORDER MANAGEMENT API (Atomic Checkout, Schema Persistence & Reservation)
# ============================================================================
class OrderItemRequest(BaseModel):
    product_id: str
    quantity: int = Field(1, ge=1)
    attributes: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ShippingAddressRequest(BaseModel):
    recipient_name: str = "Antigravity Customer"
    phone: Optional[str] = "+1-555-0199"
    address_line1: str = "742 Evergreen Terrace"
    address_line2: Optional[str] = None
    city: str = "San Francisco"
    state: str = "CA"
    postal_code: str = "94105"
    country_code: str = "US"
    latitude: Optional[float] = 37.7833
    longitude: Optional[float] = -122.4167
    shipping_method: str = "STANDARD"


class PaymentRequest(BaseModel):
    payment_method: str = "CREDIT_CARD" # CREDIT_CARD, APPLE_PAY, PAYPAL
    payment_token: Optional[str] = "tok_test_card"


class CheckoutRequest(BaseModel):
    customer_id: str
    idempotency_key: Optional[str] = None
    promo_code: Optional[str] = None
    shipping_address: ShippingAddressRequest = Field(default_factory=ShippingAddressRequest)
    payment: PaymentRequest = Field(default_factory=PaymentRequest)
    items: List[OrderItemRequest]


@router.post("/orders/checkout", summary="Place order with atomic inventory reservation and multi-table order capture")
def checkout_order(req: CheckoutRequest):
    if not req.items:
        raise HTTPException(status_code=400, detail="Cart cannot be empty")

    order_id = f"ORD-{uuid.uuid4().hex[:10].upper()}"
    conn = get_pg_connection()

    try:
        with conn: # Atomic database transaction
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 0. Check idempotency if key provided
                if req.idempotency_key:
                    cur.execute(
                        "SELECT order_id, total_amount, status, created_at FROM orders.orders WHERE idempotency_key = %s;",
                        (req.idempotency_key,),
                    )
                    existing = cur.fetchone()
                    if existing:
                        logging.info(f"Duplicate order submission ignored via idempotency_key: {req.idempotency_key}")
                        return {
                            "status": "already_processed",
                            "order_id": existing["order_id"],
                            "total_amount": float(existing["total_amount"]),
                            "order_status": existing["status"],
                            "created_at": existing["created_at"],
                        }

                # 1. Upsert customer in customers.customers
                cur.execute(
                    """
                    INSERT INTO customers.customers (customer_id, name, address, phone, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET 
                        name = EXCLUDED.name,
                        address = EXCLUDED.address,
                        phone = EXCLUDED.phone,
                        latitude = COALESCE(EXCLUDED.latitude, customers.latitude),
                        longitude = COALESCE(EXCLUDED.longitude, customers.longitude);
                    """,
                    (
                        req.customer_id,
                        req.shipping_address.recipient_name,
                        f"{req.shipping_address.address_line1}, {req.shipping_address.city}, {req.shipping_address.state} {req.shipping_address.postal_code}",
                        req.shipping_address.phone,
                        req.shipping_address.latitude,
                        req.shipping_address.longitude,
                    ),
                )

                # 2. Verify pricing and reserve inventory
                subtotal_amount = 0.0
                reserved_items = []
                for item in req.items:
                    # Fetch live catalog snapshot
                    cur.execute(
                        "SELECT unit_price, product_name, category FROM catalog.product_catalog WHERE product_id = %s;",
                        (item.product_id,),
                    )
                    prod = cur.fetchone()
                    if not prod:
                        raise HTTPException(status_code=404, detail=f"Product {item.product_id} not found in catalog")

                    unit_price = float(prod["unit_price"])
                    line_total = round(unit_price * item.quantity, 2)
                    subtotal_amount += line_total

                    # Find warehouse with sufficient inventory using pessimistic lock
                    cur.execute(
                        """
                        SELECT warehouse_id, quantity_available
                        FROM inventory.inventory_levels
                        WHERE product_id = %s AND quantity_available >= %s
                        FOR UPDATE
                        LIMIT 1;
                        """,
                        (item.product_id, item.quantity),
                    )
                    wh_row = cur.fetchone()
                    if not wh_row:
                        raise HTTPException(
                            status_code=409,
                            detail=f"Insufficient inventory for {prod['product_name']} ({item.product_id})",
                        )

                    chosen_wh = wh_row["warehouse_id"]
                    new_wh_qty = wh_row["quantity_available"] - item.quantity

                    # Update warehouse stock level
                    cur.execute(
                        """
                        UPDATE inventory.inventory_levels
                        SET quantity_available = %s, last_updated_at = NOW()
                        WHERE product_id = %s AND warehouse_id = %s;
                        """,
                        (new_wh_qty, item.product_id, chosen_wh),
                    )

                    # Log audit trail
                    cur.execute(
                        """
                        INSERT INTO inventory.inventory_audit_log (
                            product_id, warehouse_id, change_type, quantity_change,
                            quantity_after, reason, event_timestamp
                        )
                        VALUES (%s, %s, 'CHECKOUT_RESERVATION', %s, %s, %s, NOW());
                        """,
                        (item.product_id, chosen_wh, -item.quantity, new_wh_qty, f"Order {order_id} checkout"),
                    )

                    reserved_items.append({
                        "product_id": item.product_id,
                        "product_name": prod["product_name"],
                        "quantity": item.quantity,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "attributes": item.attributes,
                        "warehouse_id": chosen_wh,
                        "remaining_warehouse_stock": new_wh_qty,
                    })

                # 3. Calculate Financials (Discount, Tax, Shipping)
                discount_amount = round(subtotal_amount * 0.10, 2) if req.promo_code else 0.00
                taxable_amount = max(0.0, subtotal_amount - discount_amount)
                tax_amount = round(taxable_amount * 0.0825, 2) # 8.25% standard sales tax
                shipping_fee = 0.00 if subtotal_amount >= 100.0 else 9.99
                total_amount = round(subtotal_amount - discount_amount + tax_amount + shipping_fee, 2)

                # Compute estimated delivery due date and time (standard: +2 hours, express: +45 mins)
                shipping_method = (req.shipping_address.shipping_method or "STANDARD").upper()
                delivery_interval = "45 minutes" if "EXPRESS" in shipping_method else "2 hours"
                due_delta = datetime.timedelta(minutes=45 if "EXPRESS" in shipping_method else 120)
                estimated_delivery_dt = datetime.datetime.now(datetime.timezone.utc) + due_delta

                # 4. Insert into orders.orders (Header)
                cur.execute(
                    """
                    INSERT INTO orders.orders (
                        order_id, customer_id, status, subtotal_amount, tax_amount,
                        shipping_fee, discount_amount, total_amount, currency,
                        promo_code, idempotency_key, estimated_delivery_at, created_at, updated_at
                    )
                    VALUES (%s, %s, 'PAID', %s, %s, %s, %s, %s, 'USD', %s, %s, NOW() + INTERVAL '""" + delivery_interval + """', NOW(), NOW());
                    """,
                    (
                        order_id,
                        req.customer_id,
                        round(subtotal_amount, 2),
                        round(tax_amount, 2),
                        round(shipping_fee, 2),
                        round(discount_amount, 2),
                        round(total_amount, 2),
                        req.promo_code,
                        req.idempotency_key,
                    ),
                )

                # 5. Insert Line Items into orders.order_items
                for item in reserved_items:
                    cur.execute(
                        """
                        INSERT INTO orders.order_items (
                            order_id, product_id, product_name, quantity,
                            unit_price, discount_amount, line_total, attributes
                        )
                        VALUES (%s, %s, %s, %s, %s, 0.00, %s, %s);
                        """,
                        (
                            order_id,
                            item["product_id"],
                            item["product_name"],
                            item["quantity"],
                            item["unit_price"],
                            item["line_total"],
                            json.dumps(item["attributes"]),
                        ),
                    )

                # 6. Insert into orders.order_shipping_details
                cur.execute(
                    """
                    INSERT INTO orders.order_shipping_details (
                        order_id, recipient_name, phone, address_line1, address_line2,
                        city, state, postal_code, country_code, latitude, longitude, shipping_method
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        order_id,
                        req.shipping_address.recipient_name,
                        req.shipping_address.phone,
                        req.shipping_address.address_line1,
                        req.shipping_address.address_line2,
                        req.shipping_address.city,
                        req.shipping_address.state,
                        req.shipping_address.postal_code,
                        req.shipping_address.country_code,
                        req.shipping_address.latitude,
                        req.shipping_address.longitude,
                        req.shipping_address.shipping_method,
                    ),
                )

                # 7. Insert Payment record into orders.order_payments
                payment_id = f"PAY-{uuid.uuid4().hex[:10].upper()}"
                cur.execute(
                    """
                    INSERT INTO orders.order_payments (
                        payment_id, order_id, payment_method, payment_status,
                        amount, transaction_reference, paid_at
                    )
                    VALUES (%s, %s, %s, 'CAPTURED', %s, %s, NOW());
                    """,
                    (
                        payment_id,
                        order_id,
                        req.payment.payment_method,
                        round(total_amount, 2),
                        f"txn_{uuid.uuid4().hex[:12]}",
                    ),
                )

                # 8. Record initial status history in orders.order_status_history
                cur.execute(
                    """
                    INSERT INTO orders.order_status_history (
                        order_id, previous_status, new_status, changed_by, notes
                    )
                    VALUES (%s, NULL, 'PAID', 'COMMERCE_API', 'Order checkout and payment capture authorized.');
                    """,
                    (order_id,),
                )

                # 9. Register order in delivery module as unassigned (ready for fleet route assignment)
                cur.execute(
                    """
                    INSERT INTO delivery.orders (order_id, customer_id, stop_id, order_total, status, estimated_delivery_at, created_at)
                    VALUES (%s, %s, NULL, %s, 'PROCESSING', %s, NOW())
                    ON CONFLICT (order_id) DO NOTHING;
                    """,
                    (order_id, req.customer_id, round(total_amount, 2), estimated_delivery_dt),
                )

        # Update Redis stock cache post-commit
        r = get_redis_client()
        for res_item in reserved_items:
            stock_key = f"stock:{res_item['product_id']}"
            r.hset(stock_key, res_item["warehouse_id"], res_item["remaining_warehouse_stock"])
            wh_stocks = r.hgetall(stock_key)
            tot = sum(int(v) for v in wh_stocks.values() if v.isdigit())
            r.set(f"stock_total:{res_item['product_id']}", tot)

        # Emit BigQuery application log and telemetry metric
        emit_log(
            "commerce-api",
            "INFO",
            f"Order {order_id} placed for customer {req.customer_id} (Total: ${round(total_amount, 2)})",
            trace_id=order_id[:8],
            metadata={
                "order_id": order_id,
                "customer_id": req.customer_id,
                "items_count": len(reserved_items),
                "total_amount": round(total_amount, 2)
            }
        )
        emit_metric(
            "order-checkout",
            "order_placed_amount",
            round(total_amount, 2),
            {"customer_id": req.customer_id, "currency": "USD"}
        )

        full_addr = f"{req.shipping_address.address_line1}, {req.shipping_address.city}, {req.shipping_address.state}"

        # Calculate initial ML delivery delay risk prediction
        try:
            try:
                from ml.delay_risk_classifier import delay_classifier
            except ImportError:
                from commerce_api.ml.delay_risk_classifier import delay_classifier
            risk_eval = delay_classifier.predict_risk(
                stops_away=3,
                order_status="PROCESSING",
                current_stop_number=1,
                destination_stop_number=4,
            )
        except Exception as e:
            logging.warning(f"Initial checkout delay risk classification failed: {e}")
            risk_eval = {
                "risk_level": "LOW",
                "delay_probability": 0.12,
                "on_time_probability": 0.88,
                "confidence": 0.88,
                "estimated_status": "On schedule - Queued for route assignment",
            }

        return {
            "status": "CONFIRMED",
            "order_id": order_id,
            "customer_id": req.customer_id,
            "estimated_delivery_at": estimated_delivery_dt.isoformat(),
            "delivery_delay": risk_eval,
            "financials": {
                "subtotal_amount": round(subtotal_amount, 2),
                "discount_amount": round(discount_amount, 2),
                "tax_amount": round(tax_amount, 2),
                "shipping_fee": round(shipping_fee, 2),
                "total_amount": round(total_amount, 2),
                "currency": "USD",
            },
            "items_count": len(reserved_items),
            "items": reserved_items,
            "shipping": {
                "recipient_name": req.shipping_address.recipient_name,
                "address": full_addr,
                "assigned_route_id": None,
                "route_stop_sequence": None,
                "status": "QUEUED_FOR_DISPATCH",
                "shipping_method": shipping_method,
                "estimated_delivery_at": estimated_delivery_dt.isoformat(),
            },
            "payment": {
                "payment_id": payment_id,
                "status": "CAPTURED",
                "payment_method": req.payment.payment_method,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Checkout error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/orders/{order_id}", summary="Retrieve complete e-commerce order details by ID")
def get_order_details(order_id: str):
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Fetch Order Header
            cur.execute("SELECT * FROM orders.orders WHERE order_id = %s;", (order_id,))
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

            # 2. Fetch Line Items
            cur.execute("SELECT * FROM orders.order_items WHERE order_id = %s ORDER BY item_id ASC;", (order_id,))
            items = cur.fetchall()

            # 3. Fetch Shipping Details
            cur.execute("SELECT * FROM orders.order_shipping_details WHERE order_id = %s;", (order_id,))
            shipping = cur.fetchone()

            # 4. Fetch Payments
            cur.execute("SELECT * FROM orders.order_payments WHERE order_id = %s;", (order_id,))
            payments = cur.fetchall()

            # 5. Fetch Status History
            cur.execute("SELECT * FROM orders.order_status_history WHERE order_id = %s ORDER BY changed_at ASC;", (order_id,))
            history = cur.fetchall()

        return {
            "order": order,
            "items": items,
            "shipping": shipping,
            "payments": payments,
            "status_history": history,
        }
    finally:
        conn.close()


# ============================================================================
# 4. ORDER TRACKING API (Telemetry, Route ETA & Driver Location)
# ============================================================================
@router.get("/tracking/{order_id}", summary="Live real-time order tracking and vehicle location")
def track_order(order_id: str):
    query = """
        SELECT 
            o.order_id,
            o.status AS order_status,
            o.order_total,
            o.delivered_at,
            c.customer_id,
            c.name AS customer_name,
            rs.address AS delivery_address,
            rs.stop_sequence AS destination_stop_number,
            r.route_id,
            r.status AS route_status,
            vls.vehicle_id,
            vls.last_latitude AS vehicle_latitude,
            vls.last_longitude AS vehicle_longitude,
            vls.current_stop_sequence AS driver_current_stop_number,
            vls.last_ping_time AS vehicle_last_ping,
            CASE 
                WHEN o.status = 'DELIVERED' THEN 0
                WHEN vls.current_stop_sequence IS NULL THEN NULL
                ELSE GREATEST(0, rs.stop_sequence - vls.current_stop_sequence)
            END AS stops_away
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN route_stops rs ON o.stop_id = rs.stop_id
        LEFT JOIN routes r ON rs.route_id = r.route_id
        LEFT JOIN vehicle_latest_status vls ON vls.current_route_id = r.route_id
        WHERE o.order_id = %s;
    """
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (order_id,))
            result = cur.fetchone()

    if not result:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    stops_away = result.get("stops_away")
    eta_minutes = (stops_away * 12) if stops_away is not None else 30

    # Calculate real-time ML delay risk classification (EasyVisa / ReneWind Learning)
    try:
        try:
            from ml.delay_risk_classifier import delay_classifier
        except ImportError:
            from commerce_api.ml.delay_risk_classifier import delay_classifier
        risk_eval = delay_classifier.predict_risk(
            stops_away=stops_away,
            order_status=result.get("order_status", "UNKNOWN"),
            current_stop_number=result.get("driver_current_stop_number", 1),
            destination_stop_number=result.get("destination_stop_number", 5),
        )
    except Exception as e:
        logging.warning(f"Delay risk classification failed: {e}")
        risk_eval = {
            "risk_level": "LOW",
            "delay_probability": 0.15,
            "on_time_probability": 0.85,
            "estimated_status": "On schedule",
        }

    return {
        "order": result,
        "estimated_arrival_minutes": eta_minutes,
        "is_out_for_delivery": result["order_status"] in ("OUT_FOR_DELIVERY", "IN_PROGRESS"),
        "delivery_risk": risk_eval,
    }



STORAGE_EMULATOR_HOST = os.getenv("STORAGE_EMULATOR_HOST")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "commerce-lakehouse-bronze")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-standalone:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")


def get_gcs_storage_client():
    from google.cloud import storage
    if STORAGE_EMULATOR_HOST:
        os.environ["STORAGE_EMULATOR_HOST"] = STORAGE_EMULATOR_HOST
        return storage.Client.create_anonymous_client()
    return storage.Client(project=GCP_PROJECT_ID)


@router.get("/lakehouse/files", summary="List files and metadata in Bronze Lakehouse storage")
def list_lakehouse_files():
    """Lists all uploaded files in the bronze bucket from GCS."""
    try:
        gcs = get_gcs_storage_client()
        bucket = gcs.bucket(BRONZE_BUCKET)
        blobs = list(bucket.list_blobs(max_results=100))
        files = []
        for b in blobs:
            files.append({
                "name": b.name,
                "size_bytes": b.size or 0,
                "content_type": b.content_type or "application/octet-stream",
                "updated_at": b.updated.isoformat() if b.updated else None,
                "download_url": b.media_link or f"https://storage.googleapis.com/{BRONZE_BUCKET}/{b.name}",
            })
        return {"bucket": BRONZE_BUCKET, "count": len(files), "files": files}
    except Exception as e:
        logging.error(f"Error listing lakehouse files: {e}")
        return {"bucket": BRONZE_BUCKET, "count": 0, "files": [], "error": str(e)}


@router.post("/lakehouse/upload", summary="Upload CSV file directly to Bronze storage and trigger DAG 1")
async def upload_lakehouse_csv(
    file: UploadFile = File(...),
    entity_type: str = Form("products"), # 'products' or 'customers'
):
    """Uploads customer or product CSV into Bronze GCS and triggers dag_dimension_ingestion."""
    import io
    import csv
    import requests

    if entity_type not in ("products", "customers"):
        raise HTTPException(status_code=400, detail="entity_type must be 'products' or 'customers'")

    content = await file.read()
    text = content.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    headers = next(reader, None)
    if not headers:
        raise HTTPException(status_code=400, detail="Uploaded CSV file is empty")

    headers_lower = [h.strip().lower() for h in headers]
    if entity_type == "products" and not any("product_id" in h for h in headers_lower):
        raise HTTPException(status_code=400, detail="Products CSV must contain 'product_id' header")
    if entity_type == "customers" and not any("customer_id" in h for h in headers_lower):
        raise HTTPException(status_code=400, detail="Customers CSV must contain 'customer_id' header")

    # Upload to GCS
    blob_name = f"{entity_type}/{entity_type}_latest.csv"
    gcs = get_gcs_storage_client()
    bucket = gcs.bucket(BRONZE_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content, content_type="text/csv")
    logging.info(f"Uploaded {len(content)} bytes to gs://{BRONZE_BUCKET}/{blob_name}")

    # Trigger Airflow DAG 1
    airflow_triggered = False
    run_id = None
    try:
        dag_id = "dag_dimension_ingestion"
        trigger_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
        payload = {"conf": {"entity_type": entity_type, "filename": file.filename}}
        auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
        trig_res = requests.post(trigger_url, json=payload, auth=auth, timeout=5)
        if trig_res.status_code in (200, 201):
            airflow_triggered = True
            run_id = trig_res.json().get("dag_run_id")
    except Exception as e:
        logging.warning(f"Could not trigger Airflow DAG: {e}")

    return {
        "status": "success",
        "entity_type": entity_type,
        "filename": file.filename,
        "gcs_path": f"gs://{BRONZE_BUCKET}/{blob_name}",
        "size_bytes": len(content),
        "airflow_triggered": airflow_triggered,
        "dag_run_id": run_id,
    }


@router.get("/lakehouse/pipelines", summary="Get status of Airflow DAG pipelines")
def get_lakehouse_pipelines():
    """Fetches recent execution state and status for lakehouse DAGs from Airflow REST API."""
    import requests
    dags_info = [
        {"dag_id": "dag_dimension_ingestion", "name": "Dimension Ingestion Pipeline (Event-Driven)"},
        {"dag_id": "dag_periodic_orders_export", "name": "Order Lakehouse Sync Pipeline (Scheduled)"},
        {"dag_id": "dag_ml_demand_forecasting", "name": "Commerce Demand Forecaster Retraining (MLOps)"},
    ]
    results = []
    auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)

    for d in dags_info:
        dag_id = d["dag_id"]
        status_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}"
        runs_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit=5&order_by=-execution_date"
        dag_meta = {"dag_id": dag_id, "name": d["name"], "is_paused": False, "schedule_interval": "None", "recent_runs": []}
        try:
            m_res = requests.get(status_url, auth=auth, timeout=3)
            if m_res.status_code == 200:
                m_data = m_res.json()
                dag_meta["is_paused"] = m_data.get("is_paused", False)
                dag_meta["schedule_interval"] = str(m_data.get("schedule_interval", "None"))

            r_res = requests.get(runs_url, auth=auth, timeout=3)
            if r_res.status_code == 200:
                dag_meta["recent_runs"] = r_res.json().get("dag_runs", [])
        except Exception as e:
            dag_meta["error"] = str(e)

        results.append(dag_meta)

    return {"pipelines": results}


@router.post("/lakehouse/pipelines/{dag_id}/trigger", summary="Manually trigger an Airflow DAG")
def trigger_pipeline(dag_id: str):
    """Triggers an immediate execution run of the specified Airflow DAG."""
    import requests
    if dag_id not in ("dag_dimension_ingestion", "dag_periodic_orders_export", "dag_ml_demand_forecasting"):
        raise HTTPException(status_code=400, detail="Invalid dag_id")

    trigger_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
    try:
        res = requests.post(trigger_url, json={"conf": {"triggered_by": "commerce-app-ui"}}, auth=auth, timeout=5)
        if res.status_code not in (200, 201):
            raise HTTPException(status_code=res.status_code, detail=res.text)
        return {"status": "triggered", "dag_id": dag_id, "run": res.json()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# 7. BIGQUERY ANALYTICS & OBSERVABILITY LOGGING
# ============================================================================

def get_bigquery_client():
    from google.cloud import bigquery
    return bigquery.Client(project=GCP_PROJECT_ID)


def emit_log(service_name: str, log_level: str, message: str, trace_id: str = "", span_id: str = "", metadata: Optional[Dict[str, Any]] = None):
    """Streams structured application log to BigQuery application_logs asynchronously or directly."""
    try:
        client = get_bigquery_client()
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.application_logs"
        row = {
            "service_name": service_name,
            "log_level": log_level.upper(),
            "message": message,
            "trace_id": trace_id or "",
            "span_id": span_id or "",
            "metadata": json.dumps(metadata or {}),
        }
        client.insert_rows_json(table_id, [row])
    except Exception as e:
        logging.warning(f"Failed to stream log to BigQuery: {e}")


def emit_metric(pipeline_name: str, metric_name: str, metric_value: float, labels: Optional[Dict[str, Any]] = None):
    """Streams pipeline metrics to BigQuery pipeline_metrics."""
    try:
        client = get_bigquery_client()
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.pipeline_metrics"
        row = {
            "pipeline_name": pipeline_name,
            "metric_name": metric_name,
            "metric_value": float(metric_value),
            "labels": json.dumps(labels or {}),
        }
        client.insert_rows_json(table_id, [row])
    except Exception as e:
        logging.warning(f"Failed to record metric to BigQuery: {e}")


@router.get("/analytics/gold-mart", summary="Query BigQuery Gold Mart & Serving Analytics")
def get_gold_mart_analytics():
    """Queries live gold_product_sales metrics directly from Google BigQuery."""
    try:
        client = get_bigquery_client()
        query = f"""
            SELECT 
                product_id,
                product_name,
                category,
                CAST(unit_price AS FLOAT64) as unit_price,
                CAST(units_sold AS INT64) as units_sold,
                CAST(gross_revenue AS FLOAT64) as gross_revenue,
                CAST(orders_count AS INT64) as orders_count,
                CAST(avg_order_value AS FLOAT64) as avg_order_value,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', last_order_timestamp) as last_order_timestamp,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', synced_at) as synced_at
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.gold_product_sales`
            ORDER BY units_sold DESC, gross_revenue DESC
            LIMIT 50;
        """
        rows = [dict(r) for r in client.query(query).result()]
        total_revenue = sum(r["gross_revenue"] or 0.0 for r in rows)
        total_units = sum(r["units_sold"] or 0 for r in rows)
        total_orders = sum(r["orders_count"] or 0 for r in rows)

        emit_log("commerce-api", "INFO", "Queried BigQuery gold_product_sales mart", metadata={"products_count": len(rows)})

        return {
            "status": "success",
            "count": len(rows),
            "summary": {
                "total_revenue": round(total_revenue, 2),
                "total_units_sold": total_units,
                "total_orders": total_orders,
                "dataset": f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.gold_product_sales"
            },
            "products": rows
        }
    except Exception as e:
        logging.error(f"Error querying BigQuery gold mart: {e}")
        return {"status": "error", "error": str(e), "products": [], "summary": {}}


@router.get("/analytics/routes", summary="Query BigQuery Gold Daily Route Performance and Route Stops")
def get_routes_analytics():
    """Queries route performance aggregations and stop durations directly from BigQuery gold and silver layers."""
    try:
        client = get_bigquery_client()
        
        # 1. Query daily route performance
        routes_query = f"""
            SELECT 
                route_id,
                delivery_date AS route_date,
                CAST(total_stops AS INT64) AS total_stops,
                CAST(total_orders_delivered AS INT64) AS completed_stops,
                CAST(total_transit_seconds AS INT64) AS total_transit_duration_seconds,
                CAST(total_service_seconds AS INT64) AS total_service_duration_seconds,
                CAST(total_route_duration_seconds AS INT64) AS total_route_duration_seconds,
                CAST(avg_stop_transit_seconds AS FLOAT64) AS avg_stop_transit_seconds,
                CAST(avg_stop_service_seconds AS FLOAT64) AS avg_stop_service_seconds,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', synced_at) AS synced_at
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.gold_route_performance_daily`
            ORDER BY delivery_date DESC, total_route_duration_seconds DESC
            LIMIT 50;
        """
        routes_summary = [dict(r) for r in client.query(routes_query).result()]


        # 2. Query stop-level duration metrics
        stops_query = f"""
            SELECT 
                route_id,
                CAST(stop_id AS INT64) AS stop_id,
                CAST(stop_sequence AS INT64) AS stop_sequence,
                address,
                stop_status,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', departed_previous_stop_at) AS departed_previous_stop_at,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', arrived_at) AS arrived_at,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', completed_at) AS completed_at,
                CAST(transit_duration_seconds AS INT64) AS transit_duration_seconds,
                CAST(service_duration_seconds AS INT64) AS service_duration_seconds,
                CAST(total_stop_duration_seconds AS INT64) AS total_stop_duration_seconds
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_delivery_route_stops`
            ORDER BY route_id, stop_sequence ASC
            LIMIT 100;
        """
        stops_details = [dict(r) for r in client.query(stops_query).result()]

        return {
            "status": "success",
            "routes_count": len(routes_summary),
            "stops_count": len(stops_details),
            "routes": routes_summary,
            "stops": stops_details
        }
    except Exception as e:
        logging.error(f"Error querying BigQuery route analytics: {e}")
        return {"status": "error", "error": str(e), "routes": [], "stops": []}


@router.get("/analytics/order-status-history", summary="Query BigQuery Silver Order Status Transitions")
def get_order_status_history_analytics(
    limit: int = Query(100, ge=1, le=500),
    date: Optional[str] = Query(None, description="Filter transitions by date YYYY-MM-DD")
):
    """Queries comprehensive order status transition audit trail from silver_order_status_history."""
    try:
        client = get_bigquery_client()
        where_clause = ""
        if date:
            # Filter by calendar date of changed_at
            where_clause = f"WHERE DATE(changed_at) = DATE('{date}')"

        query = f"""
            SELECT 
                CAST(history_id AS INT64) AS history_id,
                order_id,
                previous_status,
                new_status,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', changed_at) AS changed_at,
                changed_by,
                notes,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', synced_at) AS synced_at
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.silver_order_status_history`
            {where_clause}
            ORDER BY changed_at DESC
            LIMIT {limit};
        """
        rows = [dict(r) for r in client.query(query).result()]
        return {
            "status": "success",
            "count": len(rows),
            "date": date,
            "history": rows
        }
    except Exception as e:
        logging.error(f"Error querying BigQuery order status history: {e}")
        return {"status": "error", "error": str(e), "count": 0, "history": []}



@router.get("/observability/logs", summary="Fetch BigQuery application and pipeline logs")
def get_observability_logs(
    service: Optional[str] = Query(None, description="Filter by service name"),
    level: Optional[str] = Query(None, description="Filter by log level"),
    trace_id: Optional[str] = Query(None, description="Filter by trace ID"),
    q: Optional[str] = Query(None, description="Free-text search query across messages"),
    limit: int = Query(50, ge=1, le=200)
):
    """Retrieves live application and pipeline logs from BigQuery application_logs with trace and search support."""
    try:
        client = get_bigquery_client()
        where_clauses = []
        if service:
            where_clauses.append(f"service_name = '{service}'")
        if level:
            where_clauses.append(f"log_level = '{level.upper()}'")
        if trace_id:
            where_clauses.append(f"trace_id = '{trace_id}'")
        if q:
            clean_q = q.replace("'", "\\'")
            where_clauses.append(f"LOWER(message) LIKE '%{clean_q.lower()}%'")
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        query = f"""
            SELECT 
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) as timestamp,
                service_name,
                log_level,
                message,
                trace_id,
                span_id,
                TO_JSON_STRING(metadata) as metadata
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.application_logs`
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT {limit};
        """
        rows = [dict(r) for r in client.query(query).result()]
        return {"status": "success", "count": len(rows), "logs": rows}
    except Exception as e:
        logging.error(f"Error querying BigQuery logs: {e}")
        return {"status": "error", "error": str(e), "count": 0, "logs": []}


@router.get("/observability/traces/{trace_id}", summary="Fetch end-to-end distributed trace spans")
def get_trace_spans(trace_id: str):
    """Retrieves all correlated execution spans chronologically across services for a given trace_id."""
    try:
        client = get_bigquery_client()
        clean_trace = trace_id.replace("'", "\\'")
        query = f"""
            SELECT 
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) as timestamp,
                service_name,
                log_level,
                message,
                trace_id,
                span_id,
                TO_JSON_STRING(metadata) as metadata
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.application_logs`
            WHERE trace_id = '{clean_trace}' OR trace_id LIKE '%{clean_trace}%'
            ORDER BY timestamp ASC
            LIMIT 50;
        """
        rows = [dict(r) for r in client.query(query).result()]
        return {
            "status": "success",
            "trace_id": trace_id,
            "spans_count": len(rows),
            "spans": rows
        }
    except Exception as e:
        logging.error(f"Error querying trace spans for {trace_id}: {e}")
        return {"status": "error", "trace_id": trace_id, "error": str(e), "spans_count": 0, "spans": []}


@router.get("/observability/metrics", summary="Fetch BigQuery pipeline metrics")
def get_observability_metrics(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline name"),
    limit: int = Query(50, ge=1, le=200)
):
    """Retrieves telemetry pipeline execution metrics from BigQuery pipeline_metrics."""
    try:
        client = get_bigquery_client()
        where_sql = f"WHERE pipeline_name = '{pipeline}'" if pipeline else ""
        query = f"""
            SELECT 
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) as timestamp,
                pipeline_name,
                metric_name,
                metric_value,
                TO_JSON_STRING(labels) as labels
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.pipeline_metrics`
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT {limit};
        """
        rows = [dict(r) for r in client.query(query).result()]
        return {"status": "success", "count": len(rows), "metrics": rows}
    except Exception as e:
        logging.error(f"Error querying BigQuery metrics: {e}")
        return {"status": "error", "error": str(e), "count": 0, "metrics": []}


@router.post("/observability/log", summary="Emit an application log event to BigQuery")
def create_log_event(payload: Dict[str, Any]):
    """Ingests a custom log event into BigQuery application_logs."""
    service_name = payload.get("service_name", "custom-client")
    log_level = payload.get("log_level", "INFO")
    message = payload.get("message", "")
    trace_id = payload.get("trace_id", str(uuid.uuid4())[:8])
    metadata = payload.get("metadata", {})
    emit_log(service_name, log_level, message, trace_id=trace_id, metadata=metadata)
    return {"status": "logged", "service": service_name, "trace_id": trace_id}


@router.post("/observability/metric", summary="Emit a pipeline metric event to BigQuery")
def create_metric_event(payload: Dict[str, Any]):
    """Ingests a pipeline metric into BigQuery pipeline_metrics."""
    pipeline_name = payload.get("pipeline_name", "custom-pipeline")
    metric_name = payload.get("metric_name", "")
    metric_value = float(payload.get("metric_value", 1.0))
    labels = payload.get("labels", {})
    emit_metric(pipeline_name, metric_name, metric_value, labels=labels)
    return {"status": "recorded", "pipeline": pipeline_name, "metric": metric_name}


class DemandForecastRequest(BaseModel):
    product_id: str = Field(..., description="Unique product ID to forecast demand for, e.g. PROD-001")
    horizon_days: int = Field(14, ge=1, le=90, description="Forecast horizon in days (1 to 90)")


@router.post("/ml/forecast/demand", summary="Commerce Retail Sales & Unit Demand Forecaster")
def forecast_product_demand(req: DemandForecastRequest):
    """
    Executes the Commerce gradient boosting regression model to forecast future
    daily sales volume, revenue bounds, and regional demand dynamics.
    """
    try:
        try:
            from ml.demand_forecaster import demand_forecaster
        except ImportError:
            from commerce_api.ml.demand_forecaster import demand_forecaster
        forecast = demand_forecaster.predict_demand(req.product_id, horizon_days=req.horizon_days)
        return {"status": "success", "data": forecast}
    except Exception as e:
        logging.error(f"Error computing demand forecast: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ml/forecast/summary", summary="Platform-wide Retail Demand Forecast Summary")
def get_demand_forecast_summary(horizon_days: int = Query(14, ge=1, le=60)):
    """
    Returns aggregated demand forecasts across all core catalog products
    to enable intelligent replenishment and warehouse inventory staging.
    """
    try:
        try:
            from ml.demand_forecaster import CATALOG_DEFAULTS, demand_forecaster
        except ImportError:
            from commerce_api.ml.demand_forecaster import CATALOG_DEFAULTS, demand_forecaster
        # 1. Fetch live product catalog from database dynamically
        catalog_products = []
        try:
            with get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT product_id, product_name AS name, category, unit_price AS price
                        FROM catalog.product_catalog
                        ORDER BY product_id ASC;
                        """
                    )
                    rows = cur.fetchall()
                    if rows:
                        catalog_products = [
                            {
                                "product_id": r["product_id"],
                                "name": r["name"],
                                "category": r.get("category") or "General",
                                "price": float(r["price"]),
                            }
                            for r in rows
                        ]
        except Exception as db_err:
            logging.warning(f"Could not load dynamic products from catalog.product_catalog: {db_err}")

        # Fallback to CATALOG_DEFAULTS if database catalog is empty
        if not catalog_products:
            catalog_products = CATALOG_DEFAULTS

        # 2. Fetch live inventory levels across warehouses
        inventory_map = {}
        try:
            with get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT product_id, warehouse_id, quantity_available
                        FROM inventory.inventory_levels;
                        """
                    )
                    inv_rows = cur.fetchall()
                    for ir in inv_rows:
                        pid = ir["product_id"]
                        wh = ir["warehouse_id"]
                        qty = int(ir["quantity_available"])
                        if pid not in inventory_map:
                            inventory_map[pid] = {"total": 0, "warehouses": {}}
                        inventory_map[pid]["warehouses"][wh] = qty
                        inventory_map[pid]["total"] += qty
        except Exception as inv_err:
            logging.warning(f"Could not load inventory levels: {inv_err}")

        # Also check Redis for any realtime cached inventory if not found or updated in postgres
        try:
            r = get_redis_client()
            for item in catalog_products:
                pid = item["product_id"]
                if pid not in inventory_map or inventory_map[pid]["total"] == 0:
                    stock_key = f"stock:{pid}"
                    wh_stocks = r.hgetall(stock_key)
                    if wh_stocks:
                        w_breakdown = {wh: int(q) for wh, q in wh_stocks.items() if q.isdigit() or (q.startswith('-') and q[1:].isdigit())}
                        tot = sum(w_breakdown.values())
                        inventory_map[pid] = {"total": tot, "warehouses": w_breakdown}
        except Exception as redis_err:
            logging.warning(f"Could not check Redis inventory cache: {redis_err}")

        summaries = []
        total_projected_units = 0
        total_projected_revenue = 0.0

        for item in catalog_products:
            fc = demand_forecaster.predict_demand(item["product_id"], horizon_days=horizon_days)
            pid = item["product_id"]
            inv_info = inventory_map.get(pid, {"total": 0, "warehouses": {}})
            current_stock = inv_info["total"]
            wh_breakdown = inv_info["warehouses"]

            summaries.append({
                "product_id": pid,
                "product_name": item["name"],
                "category": item["category"],
                "unit_price": item["price"],
                "forecasted_units": fc["total_forecasted_units"],
                "projected_revenue": fc["total_projected_revenue"],
                "daily_average": fc["average_daily_units"],
                "current_stock": current_stock,
                "warehouse_breakdown": wh_breakdown,
            })
            total_projected_units += fc["total_forecasted_units"]
            total_projected_revenue += fc["total_projected_revenue"]

        return {
            "status": "success",
            "horizon_days": horizon_days,
            "total_catalog_products": len(summaries),
            "total_projected_units": total_projected_units,
            "total_projected_revenue": round(total_projected_revenue, 2),
            "products": summaries,
        }
    except Exception as e:
        logging.error(f"Error computing demand forecast summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml/forecast/retrain", summary="Trigger Commerce Demand Forecaster retraining")
def trigger_demand_forecaster_retrain():
    """Triggers retraining of the Commerce Demand Forecaster and persists updated weights."""
    try:
        from ml.demand_forecaster import demand_forecaster
        metrics = demand_forecaster.train()
        return {"status": "success", "message": "Commerce demand forecaster retrained successfully", "metrics": metrics}
    except Exception as e:
        logging.error(f"Error retraining demand forecaster: {e}")
        raise HTTPException(status_code=500, detail=str(e))


