import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import trigger, window
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import bigtable, pubsub_v1


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Environment configurations
PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
BIGTABLE_EMULATOR_HOST = os.getenv("BIGTABLE_EMULATOR_HOST", "bigtable-emulator:8086")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "stream-topic")
NOTIFICATION_TOPIC = os.getenv("NOTIFICATION_TOPIC", "delivery_notification")
BIGTABLE_INSTANCE = os.getenv("BIGTABLE_INSTANCE_ID", "test-instance")
BIGTABLE_TABLE = os.getenv("BIGTABLE_TABLE_ID", "vehicle_locations")
COLUMN_FAMILY = "telemetry"


# Set client library emulator hosts in the runtime environment
os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST
os.environ["BIGTABLE_EMULATOR_HOST"] = BIGTABLE_EMULATOR_HOST


def init_bigtable_schema():
    """
    Ensures Bigtable table and column family exist prior to streaming writes.
    Note: The Bigtable emulator does NOT implement instance admin APIs (get_instance),
    instances exist implicitly, so we work directly with table admin.
    """
    logging.info(
        f"Connecting to Bigtable emulator at {BIGTABLE_EMULATOR_HOST} (project={PROJECT_ID}, instance={BIGTABLE_INSTANCE})..."
    )
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)

    table = instance.table(BIGTABLE_TABLE)
    if not table.exists():
        logging.info(f"Creating Bigtable table: {BIGTABLE_TABLE}")
        table.create()
    
    # Ensure column family exists
    try:
        cf = table.column_family(COLUMN_FAMILY)
        cf.create()
        logging.info(f"Created column family '{COLUMN_FAMILY}' in table '{BIGTABLE_TABLE}'.")
    except (AlreadyExists, Exception) as e:
        logging.info(f"Column family check: {e}")



# Define Tag for DLQ Side Output
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


class ParseAndTimestampDoFn(beam.DoFn):
    """
    Step 1: Parses raw JSON byte messages from Pub/Sub and attaches event timestamps.
    Valid records are yielded as TimestampedValue to the main output.
    Corrupted or malformed records are tagged and emitted as a Side Output to DLQ.
    """

    def process(self, element: bytes):
        raw_str = ""
        try:
            raw_str = element.decode("utf-8")
            payload = json.loads(raw_str)

            if not isinstance(payload, dict):
                raise ValueError("Payload must be a JSON dictionary")

            # Validate required fields
            if "vehicle_id" not in payload:
                raise ValueError("Missing required field: 'vehicle_id'")

            # Validate coordinates if provided
            if "latitude" in payload and payload["latitude"] not in ("", None):
                float(payload["latitude"])
            if "longitude" in payload and payload["longitude"] not in ("", None):
                float(payload["longitude"])

            # Expect timestamp in payload (epoch seconds or float), fallback to current time
            event_time = float(payload.get("timestamp", time.time()))
            vehicle_id = str(payload.get("vehicle_id"))
            route_id = str(payload.get("route_id", "unassigned"))
            lat = str(payload.get("latitude", "0.0"))
            lng = str(payload.get("longitude", "0.0"))

            record = {
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "latitude": lat,
                "longitude": lng,
                "timestamp": event_time,
            }

            logging.info(f"Parsed incoming telemetry: vehicle={vehicle_id}, route={route_id}, lat={lat}, lng={lng}")

            # Main Output: valid TimestampedValue
            yield window.TimestampedValue(record, event_time)
        except Exception as e:
            err_msg = str(e)
            logging.warning(f"⚠️ Corrupted/Invalid message routed to DLQ side output: {err_msg}")
            # Side Output: TaggedOutput goes to DEAD_LETTER_TAG
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                {
                    "raw_payload": raw_str or str(element),
                    "error_message": err_msg,
                },
            )



class VehicleTelemetryAggregator(beam.CombineFn):
    """
    Step 2: Accumulates vehicle telemetry over the window.
    Calculates ping count and keeps the latest known position.
    """

    def create_accumulator(self):
        # (count, latest_event)
        return (0, None)

    def add_input(self, accumulator, input_element):
        count, latest_event = accumulator
        count += 1
        if latest_event is None or input_element["timestamp"] >= latest_event["timestamp"]:
            latest_event = input_element
        return (count, latest_event)

    def merge_accumulators(self, accumulators):
        total_count = 0
        latest_event = None
        for count, event in accumulators:
            total_count += count
            if event is not None:
                if latest_event is None or event["timestamp"] >= latest_event["timestamp"]:
                    latest_event = event
        return (total_count, latest_event)

    def extract_output(self, accumulator):
        count, latest_event = accumulator
        return {
            "ping_count": count,
            "latest": latest_event,
        }


class WriteToBigtableDoFn(beam.DoFn):
    """
    Step 3: Writes window-aggregated telemetry to the Bigtable emulator.
    Receives (vehicle_id, aggregated_result) along with the Window object using beam.DoFn.WindowParam.
    """

    def __init__(self, project_id: str, instance_id: str, table_id: str, column_family: str):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family = column_family
        self.table = None

    def setup(self):
        # setup() is called once per worker thread/process
        client = bigtable.Client(project=self.project_id, admin=True)
        instance = client.instance(self.instance_id)
        self.table = instance.table(self.table_id)

    def process(self, element: Tuple[str, Dict[str, Any]], win=beam.DoFn.WindowParam):
        vehicle_id, stats = element
        latest = stats.get("latest") or {}
        ping_count = stats.get("ping_count", 0)

        # Extract window start and end (UTC ISO format)
        win_start = datetime.datetime.utcfromtimestamp(float(win.start)).strftime("%Y%m%d%H%M%S")
        win_end = datetime.datetime.utcfromtimestamp(float(win.end)).strftime("%Y%m%d%H%M%S")
        route_id = latest.get("route_id", "unassigned")

        # Row Key Format: vehicle#<vehicle_id>#route#<route_id>#<win_start>_<win_end>
        row_key = f"vehicle#{vehicle_id}#route#{route_id}#{win_start}_{win_end}".encode("utf-8")

        row = self.table.direct_row(row_key)
        cf = self.column_family
        now = datetime.datetime.utcnow()

        row.set_cell(cf, b"vehicle_id", vehicle_id.encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"route_id", route_id.encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"latitude", str(latest.get("latitude", "")).encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"longitude", str(latest.get("longitude", "")).encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"last_event_ts", str(latest.get("timestamp", "")).encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"ping_count", str(ping_count).encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"window_start", win_start.encode("utf-8"), timestamp=now)
        row.set_cell(cf, b"window_end", win_end.encode("utf-8"), timestamp=now)

        row.commit()
        logging.info(
            f"✅ Written to Bigtable: RowKey={row_key.decode('utf-8')} "
            f"pings={ping_count} lat={latest.get('latitude')} lng={latest.get('longitude')}"
        )


class WriteToPostgresDoFn(beam.DoFn):
    """
    Step 4: Updates PostgreSQL real-time vehicle status table.
    Performs an UPSERT (ON CONFLICT DO UPDATE) with the latest coordinates and ping count.
    """

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
        import psycopg2
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
        self.conn.autocommit = True

    def teardown(self):
        if self.conn:
            self.conn.close()

    def process(self, element: Tuple[str, Dict[str, Any]]):
        vehicle_id, stats = element
        latest = stats.get("latest") or {}
        ping_count = stats.get("ping_count", 0)

        route_id = latest.get("route_id")
        lat = latest.get("latitude")
        lng = latest.get("longitude")
        ts = latest.get("timestamp")

        # Convert epoch timestamp to ISO string
        ping_time_iso = (
            datetime.datetime.utcfromtimestamp(float(ts)).isoformat()
            if ts
            else datetime.datetime.utcnow().isoformat()
        )

        query = """
            INSERT INTO vehicle_latest_status (
                vehicle_id, current_route_id, last_latitude, last_longitude,
                last_ping_time, ping_count_last_window, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET
                current_route_id = EXCLUDED.current_route_id,
                last_latitude = EXCLUDED.last_latitude,
                last_longitude = EXCLUDED.last_longitude,
                last_ping_time = EXCLUDED.last_ping_time,
                ping_count_last_window = EXCLUDED.ping_count_last_window,
                updated_at = NOW();
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (vehicle_id, route_id, lat, lng, ping_time_iso, ping_count))
            logging.info(
                f"✅ Updated Postgres vehicle_latest_status: vehicle={vehicle_id} lat={lat} lng={lng} pings={ping_count}"
            )
            emit_observability_log(
                service_name="delivery-beam-pipeline",
                log_level="INFO",
                message=f"Processed telemetry window for {vehicle_id} on {route_id}: {ping_count} pings, loc=({lat}, {lng})",
                trace_id=f"win_{vehicle_id}_{route_id}",
                metadata={"vehicle_id": vehicle_id, "route_id": route_id, "lat": lat, "lng": lng, "pings": ping_count}
            )
            emit_observability_metric(
                pipeline_name="delivery-beam-pipeline",
                metric_name="telemetry_window_processed",
                metric_value=float(ping_count),
                labels={"vehicle_id": vehicle_id, "route_id": str(route_id)}
            )
            # Check if this telemetry indicates arrival at an active stop
            self.check_and_notify_delivery(vehicle_id, route_id, lat, lng)
        except Exception as exc:
            logging.error(f"Postgres write error for vehicle {vehicle_id}: {exc}")
            # Reconnect if connection was dropped
            try:
                self.setup()
            except Exception:
                pass

    def check_and_notify_delivery(self, vehicle_id: str, route_id: str, lat_str: str, lng_str: str):
        """
        Geofence Proximity Check:
        If the vehicle is within ~0.003 degrees (~300m) of an active stop (or matches coordinates),
        marks the stop COMPLETED, marks the orders DELIVERED, compiles customer details,
        and publishes a notification to the delivery_notification Pub/Sub topic.
        """
        if not lat_str or not lng_str or not route_id:
            return

        try:
            cur_lat = float(lat_str)
            cur_lng = float(lng_str)
        except ValueError:
            return

        select_active_stop = """
            SELECT rs.stop_id, rs.stop_sequence, rs.latitude, rs.longitude, rs.address
            FROM route_stops rs
            WHERE rs.route_id = %s AND rs.status IN ('EN_ROUTE', 'PENDING')
            ORDER BY rs.stop_sequence ASC
            LIMIT 1;
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(select_active_stop, (route_id,))
                stop = cur.fetchone()
                if not stop:
                    return

                stop_id, stop_seq, stop_lat, stop_lng, address = stop
                stop_lat = float(stop_lat)
                stop_lng = float(stop_lng)

                # Distance check in degrees (~0.01 deg is approx 1.1km, ~0.005 deg is approx 550 meters)
                dist = ((cur_lat - stop_lat) ** 2 + (cur_lng - stop_lng) ** 2) ** 0.5
                
                # Proximity Notification: Vehicle is getting closer (< 2.8km)
                if 0.005 < dist <= 0.025:
                    logging.info(
                        f"🚚 Vehicle {vehicle_id} is approaching Stop #{stop_seq} (ID: {stop_id}, dist={dist:.5f})"
                    )
                    order_query = """
                        SELECT o.order_id, o.customer_id, o.order_total, c.name, c.email, c.webhook_url, c.address
                        FROM orders o
                        JOIN customers c ON o.customer_id = c.customer_id
                        WHERE o.stop_id = %s AND o.status != 'DELIVERED';
                    """
                    cur.execute(order_query, (stop_id,))
                    approaching_orders = cur.fetchall()
                    for ord_id, cust_id, total, cust_name, email, webhook_url, cust_addr in approaching_orders:
                        notification_payload = {
                            "event_type": "DELIVERY_APPROACHING",
                            "order_id": ord_id,
                            "customer_id": cust_id,
                            "customer_name": cust_name,
                            "customer_email": email or f"{cust_id}@example.com",
                            "webhook_url": webhook_url or "https://httpbin.org/post",
                            "delivery_address": address or cust_addr,
                            "order_total": float(total) if total else 0.0,
                            "timestamp": datetime.datetime.utcnow().isoformat(),
                            "vehicle_id": vehicle_id,
                            "route_id": route_id,
                            "stop_sequence": stop_seq,
                            "distance_estimate_meters": max(50, int(dist * 111000)),
                            "message": f"Truck {vehicle_id} is within {max(50, int(dist * 111000))}m of {address or cust_addr}!"
                        }
                        self.publish_delivery_notification(notification_payload)

                elif dist <= 0.005:  # Vehicle has arrived at stop
                    logging.info(
                        f"🎯 Vehicle {vehicle_id} arrived at Stop #{stop_seq} (ID: {stop_id}, dist={dist:.5f})!"
                    )

                    # 1. Update stop status to COMPLETED with duration calculations
                    update_stop = """
                        UPDATE route_stops
                        SET status = 'COMPLETED',
                            arrived_at = COALESCE(arrived_at, NOW()),
                            completed_at = NOW(),
                            transit_duration_seconds = COALESCE(
                                EXTRACT(EPOCH FROM (COALESCE(arrived_at, NOW()) - departed_previous_stop_at))::INT,
                                600
                            ),
                            service_duration_seconds = COALESCE(
                                EXTRACT(EPOCH FROM (NOW() - COALESCE(arrived_at, NOW())))::INT,
                                180
                            )
                        WHERE stop_id = %s
                        RETURNING departed_previous_stop_at, arrived_at, completed_at, transit_duration_seconds, service_duration_seconds;
                    """
                    cur.execute(update_stop, (stop_id,))
                    dur_row = cur.fetchone()

                    # 1b. Archive completed stop to delivery.route_stops_history
                    cur.execute("""
                        INSERT INTO delivery.route_stops_history (
                            route_id, stop_id, stop_sequence, address, latitude, longitude,
                            stop_status, departed_previous_stop_at, arrived_at, completed_at,
                            transit_duration_seconds, service_duration_seconds, total_stop_duration_seconds
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, 'COMPLETED', %s, %s, %s, %s, %s, %s);
                    """, (
                        route_id, stop_id, stop_seq, address, stop_lat, stop_lng,
                        dur_row[0] if dur_row else None,
                        dur_row[1] if dur_row else None,
                        dur_row[2] if dur_row else None,
                        dur_row[3] if dur_row else 600,
                        dur_row[4] if dur_row else 180,
                        ((dur_row[3] or 600) + (dur_row[4] or 180)) if dur_row else 780,
                    ))

                    # 2. Advance next stop to EN_ROUTE and its orders to OUT_FOR_DELIVERY
                    cur.execute(
                        """
                        UPDATE route_stops
                        SET status = 'EN_ROUTE', departed_previous_stop_at = NOW()
                        WHERE route_id = %s AND stop_sequence = %s AND status = 'PENDING'
                        RETURNING stop_id;
                        """,
                        (route_id, stop_seq + 1),
                    )
                    next_stop_row = cur.fetchone()
                    if next_stop_row:
                        next_stop_id = next_stop_row[0]
                        # Fetch orders on next stop
                        cur.execute("SELECT order_id, status FROM delivery.orders WHERE stop_id = %s;", (next_stop_id,))
                        next_orders = cur.fetchall()
                        if next_orders:
                            cur.execute("UPDATE delivery.orders SET status = 'OUT_FOR_DELIVERY' WHERE stop_id = %s;", (next_stop_id,))
                            cur.execute(
                                "UPDATE orders.orders SET status = 'OUT_FOR_DELIVERY', updated_at = NOW() WHERE order_id IN (SELECT order_id FROM delivery.orders WHERE stop_id = %s);",
                                (next_stop_id,),
                            )
                            for n_ord in next_orders:
                                n_id = n_ord[0]
                                prev_st = n_ord[1] or "SHIPPED"
                                cur.execute(
                                    """
                                    INSERT INTO orders.order_status_history (order_id, previous_status, new_status, changed_by, notes)
                                    VALUES (%s, %s, 'OUT_FOR_DELIVERY', 'BEAM_FLEET_PIPELINE', %s);
                                    """,
                                    (n_id, prev_st, f"Next stop #{stop_seq + 1} marked EN_ROUTE upon completing Stop #{stop_seq}."),
                                )
                    else:
                        # Check if all stops on this route are completed
                        cur.execute(
                            "SELECT COUNT(*) AS remaining FROM route_stops WHERE route_id = %s AND status IN ('PENDING', 'EN_ROUTE');",
                            (route_id,)
                        )
                        rem = cur.fetchone()
                        if rem and rem[0] == 0:
                            cur.execute("""
                                UPDATE delivery.routes
                                SET status = 'COMPLETED', completed_at = NOW()
                                WHERE route_id = %s;
                            """, (route_id,))
                            cur.execute("""
                                UPDATE delivery.vehicles
                                SET status = 'IDLE'
                                WHERE vehicle_id = %s;
                            """, (vehicle_id,))
                            logging.info(f"🏁 Route {route_id} completed all stops (returned to warehouse depot)! Marked COMPLETED.")

                    # 3. Advance vehicle current_stop_sequence
                    cur.execute(
                        "UPDATE vehicle_latest_status SET current_stop_sequence = %s WHERE vehicle_id = %s;",
                        (stop_seq + 1, vehicle_id),
                    )

                    # 3. Update orders to DELIVERED and compile notification data
                    order_query = """
                        UPDATE delivery.orders
                        SET status = 'DELIVERED', delivered_at = NOW()
                        WHERE stop_id = %s
                        RETURNING order_id, customer_id, order_total;
                    """
                    cur.execute(order_query, (stop_id,))
                    delivered_orders = cur.fetchall()

                    # Also synchronize orders.orders and orders.order_status_history
                    for d_ord in delivered_orders:
                        d_id = d_ord[0]
                        cur.execute(
                            "UPDATE orders.orders SET status = 'DELIVERED', updated_at = NOW() WHERE order_id = %s;",
                            (d_id,),
                        )
                        cur.execute(
                            """
                            INSERT INTO orders.order_status_history (order_id, previous_status, new_status, changed_by, notes)
                            VALUES (%s, 'OUT_FOR_DELIVERY', 'DELIVERED', 'BEAM_FLEET_PIPELINE', 'Vehicle arrival at stop sequence confirmed.');
                            """,
                            (d_id,),
                        )

                    for ord_id, cust_id, total in delivered_orders:
                        # Fetch customer details (email, webhook, name)
                        cur.execute(
                            "SELECT name, email, webhook_url, address FROM customers WHERE customer_id = %s;",
                            (cust_id,),
                        )
                        cust_info = cur.fetchone()
                        if cust_info:
                            cust_name, email, webhook_url, cust_addr = cust_info
                            notification_payload = {
                                "event_type": "ORDER_DELIVERED",
                                "order_id": ord_id,
                                "customer_id": cust_id,
                                "customer_name": cust_name,
                                "customer_email": email or f"{cust_id}@example.com",
                                "webhook_url": webhook_url or "https://httpbin.org/post",
                                "delivery_address": address or cust_addr,
                                "order_total": float(total) if total else 0.0,
                                "delivered_at": datetime.datetime.utcnow().isoformat(),
                                "vehicle_id": vehicle_id,
                                "route_id": route_id,
                                "stop_sequence": stop_seq,
                                "message": f"Order {ord_id} delivered successfully at {address or cust_addr}!"
                            }
                            self.publish_delivery_notification(notification_payload)
        except Exception as e:
            logging.error(f"Error checking stop arrival: {e}")


    def publish_delivery_notification(self, payload: Dict[str, Any]):
        """Publish delivery event payload to the delivery_notification Pub/Sub topic."""
        try:
            pub = pubsub_v1.PublisherClient()
            topic_path = pub.topic_path(PROJECT_ID, NOTIFICATION_TOPIC)
            # Ensure topic exists
            try:
                pub.create_topic(request={"name": topic_path})
            except AlreadyExists:
                pass
            except Exception:
                pass

            data = json.dumps(payload).encode("utf-8")
            future = pub.publish(topic_path, data=data)
            msg_id = future.result(timeout=10)
            logging.info(
                f"📬 Published DELIVERY_NOTIFICATION to {NOTIFICATION_TOPIC} for order {payload.get('order_id')} (MsgID: {msg_id})"
            )
            emit_observability_log(
                service_name="delivery-beam-pipeline",
                log_level="INFO",
                message=f"Published DELIVERY_NOTIFICATION for order {payload.get('order_id')} (MsgID: {msg_id})",
                trace_id=f"deliv_{payload.get('order_id')}",
                metadata={"topic": NOTIFICATION_TOPIC, "order_id": payload.get("order_id"), "vehicle_id": payload.get("vehicle_id")}
            )
            emit_observability_metric(
                pipeline_name="delivery-beam-pipeline",
                metric_name="delivery_notifications_emitted",
                metric_value=1.0,
                labels={"topic": NOTIFICATION_TOPIC, "order_id": payload.get("order_id")}
            )
        except Exception as err:
            logging.error(f"Failed to publish delivery notification: {err}")



class WriteToPostgresDLQDoFn(beam.DoFn):
    """
    Writes corrupt or rejected raw messages from the DLQ side output into Postgres dead_letter_messages.
    """

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
        import psycopg2
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
        self.conn.autocommit = True

    def teardown(self):
        if self.conn:
            self.conn.close()

    def process(self, element: Dict[str, Any]):
        raw_payload = str(element.get("raw_payload", ""))
        err_msg = str(element.get("error_message", "Unknown error"))
        query = """
            INSERT INTO dead_letter_messages (raw_payload, error_message, received_at)
            VALUES (%s, %s, NOW());
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (raw_payload, err_msg))
            logging.info(f"🚨 Recorded corrupt message into Postgres DLQ table: {err_msg}")
        except Exception as exc:
            logging.error(f"Failed to write to DLQ table: {exc}")
            try:
                self.setup()
            except Exception:
                pass


def run():
    # 1. Initialize Bigtable Table
    init_bigtable_schema()

    # PostgreSQL configuration
    database_url = os.getenv("DATABASE_URL")
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    pg_db = os.getenv("POSTGRES_DB", "commercedb")
    pg_sslmode = os.getenv("POSTGRES_SSLMODE")

    # 2. Pipeline Configuration
    pipeline_options = PipelineOptions(
        streaming=True,
        runner="DirectRunner",  # Local in-process streaming runner
    )
    pipeline_options.view_as(StandardOptions).streaming = True

    topic_path = f"projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC}"
    logging.info(f"Starting Beam DirectRunner streaming from {topic_path}...")

    # 3. Pipeline Graph Definition
    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read raw Pub/Sub bytes and parse with side output for dead letters
        parsed_results = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(topic=topic_path)
            | "ParseAndValidate"
            >> beam.ParDo(ParseAndTimestampDoFn()).with_outputs(
                DEAD_LETTER_TAG,
                main="valid_events",
            )
        )

        valid_events = parsed_results["valid_events"]
        dead_letters = parsed_results[DEAD_LETTER_TAG]

        # DLQ Sink: Write corrupt/failed messages directly to PostgreSQL dead_letter_messages
        _ = (
            dead_letters
            | "WriteDLQToPostgres"
            >> beam.ParDo(
                WriteToPostgresDLQDoFn(
                    host=pg_host,
                    port=pg_port,
                    user=pg_user,
                    password=pg_password,
                    db=pg_db,
                    database_url=database_url,
                    sslmode=pg_sslmode,
                )
            )
        )

        # Windowed Stream for valid events
        # Fixed Window: 10-second duration, with watermarks, 1 hour allowed lateness, and late firings
        aggregated_stream = (
            valid_events
            | "FixedWindow10sWithWatermark"
            >> beam.WindowInto(
                window.FixedWindows(10),
                trigger=trigger.AfterWatermark(
                    late=trigger.AfterCount(1)  # Fire immediately on any late data arriving within allowed_lateness
                ),
                allowed_lateness=3600,  # 1 hour allowed lateness (in seconds)
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            # Key elements by vehicle_id for aggregation: (vehicle_id, record)
            | "KeyByVehicle" >> beam.Map(lambda record: (record["vehicle_id"], record))
            # Combine all records for each vehicle within each 30s window
            | "AggregateTelemetry" >> beam.CombinePerKey(VehicleTelemetryAggregator())
        )


        # Sink 1: Bigtable (Time-series window breadcrumbs)
        _ = (
            aggregated_stream
            | "WriteToBigtable"
            >> beam.ParDo(
                WriteToBigtableDoFn(
                    project_id=PROJECT_ID,
                    instance_id=BIGTABLE_INSTANCE,
                    table_id=BIGTABLE_TABLE,
                    column_family=COLUMN_FAMILY,
                )
            )
        )

        # Sink 2: PostgreSQL (Relational operational state for real-time customer APIs)
        _ = (
            aggregated_stream
            | "WriteToPostgres"
            >> beam.ParDo(
                WriteToPostgresDoFn(
                    host=pg_host,
                    port=pg_port,
                    user=pg_user,
                    password=pg_password,
                    db=pg_db,
                    database_url=database_url,
                    sslmode=pg_sslmode,
                )
            )
        )



if __name__ == "__main__":
    run()
