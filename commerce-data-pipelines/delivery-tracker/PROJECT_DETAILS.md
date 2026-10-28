# Real-Time Fleet Streaming & Route Logistics Architecture

A real-time streaming data platform demonstrating event-driven telemetry processing, windowed state aggregation, dual-sink storage (Bigtable + PostgreSQL), and Dead Letter Queue (DLQ) pattern using **Apache Beam**, **Google Cloud Pub/Sub**, **Cloud Bigtable**, and **PostgreSQL**.

---

## 1. System Architecture

```text
                               ┌────────────────────────────────────────────────────────┐
                               │             FastAPI Gateway + SPA Frontend             │
                               │                (pubsub-http-publisher)                 │
                               │  - GET  / (Single Page Application Dashboard)          │
                               │  - POST /api/routes/initiate (DB Seed & Route Setup)   │
                               │  - GET  /api/fleet/summary (Live Vehicles & Stops)     │
                               │  - GET  /api/webhooks/recent (Recent Webhook Events)   │
                               │  - POST /publish (Publishes to Pub/Sub)                │
                               │  - GET  /orders/{order_id}/status (Postgres Real-time) │
                               │  - GET  /customers/{customer_id}/orders                │
                               │  - GET  /dlq/messages (Dead Letter Queue API)          │
                               └──────────────────────────┬─────────────────────────────┘
                                                          │
                                         (GPS Telemetry JSON)
                                                          │
                                                          ▼
                                            ┌───────────────────────────┐
                                            │      Pub/Sub Emulator     │
                                            │   Topic: `stream-topic`   │
                                            └─────────────┬─────────────┘
                                                          │
                                            (Unbounded Streaming Ingestion)
                                                          │
                                                          ▼
                                            ┌───────────────────────────┐
                                            │   Apache Beam Pipeline    │
                                            │  (DirectRunner Streaming) │
                                            └─────────────┬─────────────┘
                                                          │
                         ┌────────────────────────────────┴────────────────────────────────┐
                         │                                                                 │
                  (Valid Telemetry)                                                (Corrupt / Malformed)
                         │                                                                 │
                         ▼                                                                 ▼
           ┌───────────────────────────┐                                      ┌───────────────────────────┐
           │      Fixed Window 30s     │                                      │     Tagged Side Output    │
           │  (Watermark + 1hr Lateness│                                      │       DEAD_LETTER_TAG     │
           └─────────────┬─────────────┘                                      └─────────────┬─────────────┘

                         │                                                                  │
                         ├─────────────────────────────────┐                                │
                         │                                 │                                │
                         ▼                                 ▼                                ▼
           ┌───────────────────────────┐     ┌───────────────────────────┐    ┌───────────────────────────┐
           │     Bigtable Emulator     │     │     PostgreSQL Database   │    │    PostgreSQL Database    │
           │  Table: vehicle_locations │     │Table:vehicle_latest_status│    │Table:dead_letter_messages │
           │ (Time-series Breadcrumbs) │     │  (Operational Real-time)  │    │     (DLQ Quarantine)      │
           └───────────────────────────┘     └─────────────┬─────────────┘    └───────────────────────────┘
                                                           │
                                                  (Arrival at Stop)
                                                           │
                                                           ▼
                                             ┌───────────────────────────┐
                                             │      Pub/Sub Emulator     │
                                             │Topic:delivery_notification│
                                             └─────────────┬─────────────┘
                                                           │
                                             ┌─────────────┴─────────────┐
                                             │                           │
                                             ▼                           ▼
                                    ┌──────────────────┐        ┌──────────────────┐
                                    │ Webhook Dispatch │        │  Customer Email  │
                                    │(Merchant Webhook)│        │  (HTML Receipt)  │
                                    └──────────────────┘        └──────────────────┘
```


---

## 2. Core Components

### A. Ingestion Gateway (`pubsub-http-publisher`)
- Built with **FastAPI** and **Pydantic**.
- Publishes incoming GPS telemetry payloads to the local Pub/Sub emulator topic `stream-topic`.
- Serves interactive documentation and test clients (**Swagger UI** at `/docs` and **ReDoc** at `/redoc`) with CORS enabled for browser clients.
- Directly reads the PostgreSQL operational database to serve order tracking, customer history, and DLQ inspection.

### B. Streaming Processing (`beam-pipeline`)
- Implemented with **Apache Beam (Python SDK)** running with `DirectRunner` in `streaming=True` mode.
- **Timestamp Assignment**: Elements receive event timestamps from the payload's `timestamp` field via `beam.window.TimestampedValue`.
- **Fixed Windows (Tumbling Windows) with Watermarks & Allowed Lateness**:
  - **Window Size**: 30 seconds (`window.FixedWindows(30)`).
  - **Watermark & Triggering**: Emits on the watermark (`trigger.AfterWatermark()`), with speculative late firings triggered immediately on late element arrival (`late=trigger.AfterCount(1)`).
  - **Allowed Lateness**: 1 hour (`allowed_lateness=3600`). Elements arriving up to 1 hour after the window's watermark has passed are still processed and merged into the window. Data arriving older than 1 hour is discarded.
- **Side Outputs & Dead Letter Queue (DLQ)**:
  - Malformed JSON, non-dictionary payloads, or missing mandatory keys (`vehicle_id`) are tagged as `DEAD_LETTER_TAG` via `beam.pvalue.TaggedOutput`.

### C. Notification Consumer Service (`notification-service`)
- Listens as an event-driven worker on the `delivery_notification_sub` subscription of the `delivery_notification` Pub/Sub topic.
- Automatically dispatches:
  1. **Merchant/Customer Webhook**: Sends HTTP POST requests containing delivery payloads to registered endpoints (e.g. `https://httpbin.org/post`).
  2. **Customer Email Confirmation**: Generates transactional delivery receipts with order total, delivery timestamp, and recipient address.

### D. Storage Strategy: Why Bigtable + PostgreSQL?

| Feature | Cloud Bigtable (`:8086`) | PostgreSQL (`:5432`) |

| :--- | :--- | :--- |
| **Primary Use Case** | Raw high-velocity telemetry & historical playback | Relational business entities & operational transactions |
| **Data Stored** | Windowed GPS coordinates, ping counts, time boundaries | Drivers, vehicles, routes, route stops, orders, customers |
| **Query Pattern** | Fast row-key range scans (`vehicle#id#route#id#time`) | Relational joins, foreign keys, window progress calculations |
| **Write Model** | Appends / Direct Row Mutations | `UPSERT` on latest status, transactional updates on orders |

---

## 3. Data Models

### Bigtable Schema
- **Table**: `vehicle_locations`
- **Column Family**: `telemetry`
- **Row Key Design**: `vehicle#<vehicle_id>#route#<route_id>#<window_start>_<window_end>`
- **Columns**:
  - `telemetry:vehicle_id`
  - `telemetry:route_id`
  - `telemetry:latitude`
  - `telemetry:longitude`
  - `telemetry:last_event_ts`
  - `telemetry:ping_count`
  - `telemetry:window_start`
  - `telemetry:window_end`

### PostgreSQL Relational Schema
1. **`vehicles`**: Physical vehicles in fleet (`vehicle_id`, `model`, `license_plate`, `status`).
2. **`drivers`**: Personnel operating vehicles (`driver_id`, `name`, `phone`).
3. **`customers`**: Delivery recipients (`customer_id`, `name`, `address`).
4. **`routes`**: Scheduled or active journeys (`route_id`, `driver_id`, `vehicle_id`, `status`, `started_at`).
5. **`route_stops`**: Ordered sequence of customer drops (`stop_id`, `route_id`, `stop_sequence`, `latitude`, `longitude`, `address`, `status`, `departed_previous_stop_at`, `arrived_at`, `completed_at`, `transit_duration_seconds`, `service_duration_seconds`).
6. **`orders`**: Customer packages tied to stops (`order_id`, `customer_id`, `stop_id`, `status`, `order_total`, `delivered_at`).
7. **`vehicle_latest_status`**: Real-time snapshot updated by Beam (`vehicle_id`, `current_route_id`, `last_latitude`, `last_longitude`, `last_ping_time`, `ping_count_last_window`, `current_stop_sequence`).
8. **`dead_letter_messages`**: DLQ quarantined records (`id`, `raw_payload`, `error_message`, `received_at`).

---

## 4. Key Business & Analytical Queries

### 1. Stop-to-Stop Transit & Service Dwell Times
```sql
SELECT 
    r.route_id,
    d.name AS driver,
    rs.stop_sequence,
    rs.address,
    rs.transit_duration_seconds / 60.0 AS transit_minutes_to_stop,
    rs.service_duration_seconds / 60.0 AS dwell_minutes_at_stop,
    rs.status AS stop_status
FROM route_stops rs
JOIN routes r ON rs.route_id = r.route_id
JOIN drivers d ON r.driver_id = d.driver_id
WHERE r.route_id = 'route_bay_area'
ORDER BY rs.stop_sequence;
```

### 2. Real-Time Customer Tracking ("Stops Away")
```sql
SELECT 
    o.order_id,
    c.name AS customer_name,
    o.status AS order_status,
    rs.stop_sequence AS destination_stop,
    vls.current_stop_sequence AS truck_current_stop,
    GREATEST(0, rs.stop_sequence - vls.current_stop_sequence) AS stops_away
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN route_stops rs ON o.stop_id = rs.stop_id
LEFT JOIN vehicle_latest_status vls ON vls.current_route_id = rs.route_id
WHERE o.order_id = 'ord_103';
```
