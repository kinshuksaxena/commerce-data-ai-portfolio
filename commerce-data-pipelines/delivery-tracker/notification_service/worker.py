import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
TOPIC_ID = os.getenv("NOTIFICATION_TOPIC", "delivery_notification")
SUBSCRIPTION_ID = os.getenv("NOTIFICATION_SUBSCRIPTION", "delivery_notification_sub")

os.environ["PUBSUB_EMULATOR_HOST"] = EMULATOR_HOST


COMMERCE_API_GATEWAY_URL = os.getenv("COMMERCE_API_URL", "http://commerce-api-gateway:8002/api")

def emit_observability_log(service_name: str, log_level: str, message: str, trace_id: str = "", metadata: Optional[Dict[str, Any]] = None):
    """Sends structured log event to commerce-api gateway for streaming into BigQuery application_logs."""
    try:
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


def dispatch_webhook(webhook_url: str, payload: Dict[str, Any]):
    """Dispatches delivery event to customer/merchant webhook endpoint."""
    try:
        logging.info(f"🌐 [WEBHOOK] Dispatching payload to: {webhook_url}")
        resp = requests.post(webhook_url, json=payload, timeout=5)
        logging.info(f"🌐 [WEBHOOK] Status {resp.status_code} from {webhook_url}")
    except Exception as e:
        logging.warning(f"⚠️ [WEBHOOK] Failed to dispatch to {webhook_url}: {e}")


def dispatch_email(email: str, payload: Dict[str, Any]):
    """Sends delivery confirmation email to customer."""
    order_id = payload.get("order_id")
    cust_name = payload.get("customer_name")
    address = payload.get("delivery_address")
    total = payload.get("order_total")
    del_time = payload.get("delivered_at")

    logging.info("=" * 60)
    logging.info(f"✉️  [CUSTOMER EMAIL DISPATCHED]")
    logging.info(f"    To: {email} ({cust_name})")
    logging.info(f"    Subject: Your Order #{order_id} has been delivered!")
    logging.info(f"    Body: Hi {cust_name}, your package for Order #{order_id}")
    logging.info(f"          (${total:.2f}) was successfully delivered to {address}")
    logging.info(f"          at {del_time}. Thank you for shopping with us!")
    logging.info("=" * 60)


DATABASE_URL = os.getenv("DATABASE_URL")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "commercedb")
PG_SSLMODE = os.getenv("POSTGRES_SSLMODE")


def get_pg_connection():
    import psycopg2
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL)
    kwargs = {
        "host": PG_HOST,
        "port": PG_PORT,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "dbname": PG_DB,
    }
    if PG_SSLMODE:
        kwargs["sslmode"] = PG_SSLMODE
    return psycopg2.connect(**kwargs)


def record_webhook_event(payload: Dict[str, Any]):
    """Persists notification events into PostgreSQL for real-time audit & UI streaming."""
    try:
        conn = get_pg_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO webhook_events (event_type, vehicle_id, route_id, order_id, customer_name, message, payload, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
                """,
                (
                    payload.get("event_type", "DELIVERY_EVENT"),
                    payload.get("vehicle_id"),
                    payload.get("route_id"),
                    payload.get("order_id"),
                    payload.get("customer_name"),
                    payload.get("message", f"Event for order {payload.get('order_id')}"),
                    json.dumps(payload),
                ),
            )
        conn.close()
    except Exception as e:
        logging.warning(f"Could not record webhook event to Postgres: {e}")


def callback(message: pubsub_v1.subscriber.message.Message):
    try:
        data_str = message.data.decode("utf-8")
        payload = json.loads(data_str)
        event_type = payload.get("event_type", "ORDER_DELIVERED")
        logging.info(f"📥 Received [{event_type}] for Order: {payload.get('order_id')}")

        # 1. Trigger Webhook
        webhook_url = payload.get("webhook_url", "https://httpbin.org/post")
        dispatch_webhook(webhook_url, payload)

        # 2. Trigger Customer Email
        customer_email = payload.get("customer_email", "customer@example.com")
        dispatch_email(customer_email, payload)

        # 3. Record in Webhook Events table for UI real-time polling
        record_webhook_event(payload)

        # 4. Stream telemetry event to BigQuery application_logs & pipeline_metrics
        emit_observability_log(
            service_name="delivery-notification-worker",
            log_level="INFO",
            message=f"Dispatched delivery notification for order {payload.get('order_id')} to {customer_email}",
            trace_id=f"notif_{payload.get('order_id')}",
            metadata={"order_id": payload.get("order_id"), "customer_email": customer_email, "event_type": event_type}
        )
        emit_observability_metric(
            pipeline_name="delivery-notification-worker",
            metric_name="notifications_dispatched",
            metric_value=1.0,
            labels={"event_type": event_type, "order_id": payload.get("order_id")}
        )

        message.ack()
    except Exception as exc:
        logging.error(f"Error processing notification: {exc}")
        message.nack()



def run():
    logging.info(
        f"Starting Notification Service on Pub/Sub: {EMULATOR_HOST} (project={PROJECT_ID}, topic={TOPIC_ID})"
    )

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    # Ensure topic exists
    try:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Created topic: {topic_path}")
    except AlreadyExists:
        pass
    except Exception as e:
        logging.info(f"Topic check: {e}")

    # Ensure subscription exists
    try:
        subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        logging.info(f"Created subscription: {subscription_path}")
    except AlreadyExists:
        pass
    except Exception as e:
        logging.info(f"Subscription check: {e}")

    logging.info(f"🎧 Listening for delivery notifications on {subscription_path}...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    run()
