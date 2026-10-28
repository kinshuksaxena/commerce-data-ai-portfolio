# Fleet Stream: How to Run and Test Guide

This guide provides instructions for launching the environment with Docker, publishing messages, testing the Apache Beam streaming pipeline, and verifying results in Bigtable and PostgreSQL.

---

## 1. Quick Start (Run the Stack)

Navigate to the project root and launch all services in detached mode:

```bash
cd /Users/ks/forsch-ai/beam-stream
docker compose up -d --build
```

### Verified Service Ports
| Service | Container Name | Port | Description |
| :--- | :--- | :--- | :--- |
| **Publisher & API** | `pubsub-http-publisher` | `http://localhost:8000` | Ingestion & Tracking API + Swagger UI |
| **Pub/Sub Emulator** | `pubsub-emulator` | `localhost:8085` | Google Cloud Pub/Sub Emulator |
| **Bigtable Emulator**| `bigtable-emulator` | `localhost:8086` | Google Cloud Bigtable gRPC Emulator |
| **PostgreSQL** | `fleet-postgres` | `localhost:5432` | Relational Fleet & DLQ Database |
| **Beam Pipeline** | `beam-streaming-pipeline` | N/A | Streaming DirectRunner Worker |
| **Notification Worker** | `delivery-notification-worker` | N/A | Webhook & Email Delivery Subscriber |


Check status of all containers:
```bash
docker compose ps
```

---

## 2. Interactive Single Page Application (SPA) Dashboard

Open the live interactive dashboard in your browser:
👉 **[http://localhost:8000/](http://localhost:8000/)**

The SPA provides:
- **Interactive Map (Leaflet Dark Theme)**: Displays live delivery vehicle markers, route paths, and delivery stops with animated pulses for the active stop.
- **Route Dispatcher**: 1-click generation of new delivery routes (`route_sf_express`), vehicles, and 3 customer delivery stops in San Francisco.
- **Simulation Controls**:
  - `Advance Step`: Progresses the truck along intermediate waypoints towards the active stop.
  - `Auto-Drive`: Continuously moves the vehicle with automatic GPS telemetry publishing every 4 seconds.
  - `Send Corrupt (DLQ)`: Emits invalid telemetry to demonstrate Apache Beam Dead-Letter Queue side outputs.
- **Live Stop & Order Status**: Dynamically tracks `PENDING`, `EN_ROUTE`, and `COMPLETED` stops with real-time customer and order value cards.
- **Real-Time Webhook & Notification Drawer**: Live sliding feed displaying `DELIVERY_APPROACHING` alerts (when within ~500m-1.5km of a customer) and `ORDER_DELIVERED` webhook dispatches with email notifications.

---

## 3. Interactive Browser API (Swagger UI)

Open the built-in Swagger UI in your browser:
👉 **[http://localhost:8000/docs](http://localhost:8000/docs)**

From here you can execute:
- `POST /publish` - Publish vehicle GPS events or corrupt test payloads
- `POST /api/routes/initiate` - Create a new route, driver, stops, and customer orders
- `GET /api/fleet/summary` - Real-time active fleet locations, stops, and order states
- `GET /api/webhooks/recent` - Live log of triggered webhook events
- `GET /orders/{order_id}/status` - Live tracking and "stops away" for an order
- `GET /customers/{customer_id}/orders` - Customer active/delivered orders
- `GET /dlq/messages` - Inspect Dead Letter Queue errors and payloads

---

## 3. End-to-End Testing Workflows

### Test 1: Publish a Valid Vehicle Location Event

Send a GPS ping for `truck_42`:

```bash
curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "vehicle_id": "truck_42",
      "route_id": "route_bay_area",
      "latitude": 37.7852,
      "longitude": -122.4055,
      "timestamp": '$(date +%s)'
    }
  }'
```

**Expected Response:**
```json
{"status": "success", "message_id": "...", "topic": "stream-topic"}
```

---

### Test 2: Verify Real-Time Customer Order Tracking

Check the status of order `ord_103` (assigned to customer `Charlie Brown`, stop #3):

```bash
curl -s http://localhost:8000/orders/ord_103/status | jq
```

**Expected Output:**
```json
{
  "order_id": "ord_103",
  "order_status": "OUT_FOR_DELIVERY",
  "order_total": 120.0,
  "customer_name": "Charlie Brown",
  "destination_stop_number": 3,
  "route_id": "route_bay_area",
  "vehicle_id": "truck_42",
  "vehicle_latitude": 37.7852,
  "vehicle_longitude": -122.4055,
  "driver_current_stop_number": 2,
  "stops_away": 1
}
```

Check all orders for customer `cust_102`:
```bash
curl -s http://localhost:8000/customers/cust_102/orders | jq
```

---

### Test 3: Test the Dead Letter Queue (DLQ)

Publish a malformed payload (missing the mandatory `vehicle_id` key):

```bash
curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "route_id": "route_bay_area",
      "latitude": "invalid_coordinates"
    }
  }'
```

Publish an invalid non-JSON string:
```bash
curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "CORRUPT_NON_JSON_DATA"}'
```

Now query the DLQ endpoint to verify both bad records were quarantined:
```bash
curl -s http://localhost:8000/dlq/messages | jq
```

**Expected Output:**
The messages are recorded in `dead_letter_messages` table with full timestamps and error messages (`Missing required field: 'vehicle_id'` and JSON decode errors).

---

### Test 4: Verify Bigtable Fixed Window Breadcrumbs (Watermarks & Allowed Lateness)

Inspect the 30-second fixed (tumbling) window aggregations written to Bigtable when watermarks advance:


```bash
uv run --with google-cloud-bigtable python -c '
import os
from google.cloud import bigtable

os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
client = bigtable.Client(project="test-project", admin=True)
table = client.instance("test-instance").table("vehicle_locations")

for row in table.read_rows():
    print(f"\nRowKey: {row.row_key.decode(\"utf-8\")}")
    for cf, cols in row.cells.items():
        for col, cells in cols.items():
            for cell in cells:
                print(f"  [{cf}:{col.decode(\"utf-8\")}] = {cell.value.decode(\"utf-8\")}")
'
```

---

### Test 5: Analytical Route Queries in PostgreSQL

Run the analytical stop-to-stop duration query directly in the database:

```bash
docker exec -it fleet-postgres psql -U postgres -d fleetdb -c "
SELECT 
    r.route_id,
    d.name AS driver,
    rs.stop_sequence,
    rs.address,
    rs.transit_duration_seconds / 60.0 AS transit_minutes,
    rs.service_duration_seconds / 60.0 AS dwell_minutes,
    rs.status AS stop_status
FROM route_stops rs
JOIN routes r ON rs.route_id = r.route_id
JOIN drivers d ON r.driver_id = d.driver_id
ORDER BY rs.stop_sequence;
"
```

---

### Test 6: Trigger Stop Arrival & Delivery Notifications (Webhook + Email)

Publish a GPS ping matching Stop #3 (`lat: 37.7800, lng: -122.4100`, customer `Charlie Brown`, order `ord_103`):

```bash
curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "vehicle_id": "truck_42",
      "route_id": "route_bay_area",
      "latitude": 37.7800,
      "longitude": -122.4100,
      "timestamp": '$(date +%s)'
    }
  }'
```

Now inspect the logs of `delivery-notification-worker`:
```bash
docker logs delivery-notification-worker --tail 25
```

**What you will see:**
1. **Beam**: Detects arrival within the geofence at Stop #3, marks `route_stops` as `COMPLETED`, marks `orders` as `DELIVERED`, and publishes to `delivery_notification`.
2. **Notification Service**:
   - Dispatches a webhook HTTP POST request to `https://httpbin.org/post` (status 200).
   - Generates and logs customer receipt email to `charlie@example.com` with order total and delivery timestamp.
3. **API Status**:
   - Calling `GET /orders/ord_103/status` now immediately reflects `DELIVERED`!

---

## 4. Useful Operations & Commands

- **Stream Beam pipeline logs live:**
  ```bash
  docker logs -f beam-streaming-pipeline
  ```

- **Stream Notification Worker logs live:**
  ```bash
  docker logs -f delivery-notification-worker
  ```

- **Stream Publisher API logs live:**
  ```bash
  docker logs -f pubsub-http-publisher
  ```

- **Open PostgreSQL shell:**
  ```bash
  docker exec -it fleet-postgres psql -U postgres -d fleetdb
  ```

- **Stop and remove all containers:**
  ```bash
  docker compose down
  ```

