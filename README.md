# Data Engineering & Applied AI/ML Commerce Platform

> **Interactive Living Documentation:** Explore the full interactive architectural blueprints, streaming topology, Medallion lakehouse specs, and ML models in [`docs/index.html`](https://kinshuksaxena.github.io/commerce-data-ai-portfolio/) (hosted via GitHub Pages).

This repository showcases a production-grade, end-to-end **Data Engineering, Real-Time Streaming, Medallion Lakehouse, and Applied Machine Learning** platform built for modern autonomous commerce. It captures practical enterprise patterns across event-driven streaming, strict relational schema isolation, closed-loop Reverse-ETL, and embedded predictive ML.

---

## Core Architectural Pillars

| Pillar | Status | Key Technologies & Patterns |
|---|---|---|
| **1. Fleet Telematics Streaming** | **Complete** | Cloud Pub/Sub $\rightarrow$ Apache Beam (`10s` Fixed Windows, `550m` Geofencing, `30s` Stop Dwell, Side-Output DLQ) $\rightarrow$ Cloud Bigtable + PostgreSQL (`delivery.*`) |
| **2. High-Throughput Inventory Mesh** | **Complete** | Cloud Pub/Sub $\rightarrow$ Apache Beam Pull Worker $\rightarrow$ Idempotency Guard + Pessimistic Row Locking (`SELECT ... FOR UPDATE`) + Redis 7 Sub-ms L1 Cache |
| **3. Medallion Lakehouse & Reverse-ETL** | **Complete** | Google Cloud Storage Bronze (`Avro` / PyIceberg & `CSV`) $\rightarrow$ Google BigQuery Silver & Gold Marts (`dbt`) $\rightarrow$ Apache Airflow Reverse-ETL back to PostgreSQL |
| **4. Unified Commerce API & Portal** | **Complete** | FastAPI Gateway (`:8002`) + Centralized Glassmorphic Command Center (`:3000`) covering Data Streams, Catalog, Live Leaflet Fleet Tracking, Analytics, and Observability |
| **5. Applied AI/ML Suite** | **Complete** | Demand Forecaster (`GradientBoostingRegressor` + DoI Runway) & Calibrated Delivery Delay Risk Classifier (`0.9811 ROC-AUC`) with interactive Jupyter notebooks |
| **6. Glass-Box Observability** | **Complete** | Distributed `trace_id` & `span_id` waterfall correlation across HTTP, Pub/Sub, and Apache Beam into BigQuery `application_logs` & `pipeline_metrics` |
| **7. Conversational Agentic AI** | **Phase 2 Roadmap** | Multi-agent Browse, Shop & Track orchestration with Hybrid TF-IDF/BM25 RAG (in active development) |

---

## Infrastructure & Technology Stack

| Component | Service / Tool | Responsibility |
|---|---|---|
| **Message Bus** | Google Cloud Pub/Sub Emulator (`:8085`) | High-velocity event streaming for vehicle GPS pings, inventory movements, and delivery webhooks |
| **Time-Series NoSQL** | Google Cloud Bigtable Emulator (`:8086`) | Low-latency storage for high-frequency vehicle telematics (`vehicle_locations`) |
| **Operational OLTP** | PostgreSQL (Neon Serverless / Pooled) | Strict 5-schema isolation: `catalog`, `orders`, `delivery`, `customers`, and `inventory` |
| **Data Lake Storage** | Google Cloud Storage (GCS) | Immutable Bronze staging for transactional Avro (`orders_latest.avro`) and dimensional CSVs |
| **Enterprise Data Warehouse** | Google BigQuery (`commerce_analytics`) | Partitioned/clustered Silver tables, Gold analytical marts, ML feature store, and observability store |
| **Pipeline Orchestration** | Apache Airflow Standalone (`:8080`) | Orchestrates `dag_dimension_ingestion`, `dag_periodic_orders_export`, and `dag_ml_demand_forecasting` |
| **In-Memory Cache** | Redis 7 (`:6379`) | Sub-millisecond L1 stock lookups (`stock:{product_id}`) synchronized via Apache Beam |
| **API & Experience** | FastAPI (`:8000`, `:8002`, `:3000`) | REST APIs, OpenAPI `/docs`, and 5-workbench Centralized Commerce Portal |

---

## Repository Structure

```text
├── commerce-data-pipelines/
│   ├── delivery-tracker/        # GPS Telematics API (:8000), Apache Beam worker, and Webhook Notification service
│   ├── inventory-pipeline/      # Warehouse Simulator Publisher & Apache Beam ACID + Redis streaming pipeline
│   └── lakehouse/               # Airflow DAGs, GCS/Iceberg/BigQuery ingestion scripts, and dbt models
├── commerce-api/                # Unified Commerce FastAPI Gateway (:8002) & Pytest ML verification suite
├── commerce-app/                # Centralized Commerce Portal (:3000) — 5 interactive operational workbenches
├── commerce-ml/                 # Trained Scikit-Learn models (.pkl), inference pipelines, datasets & notebooks
├── deploy/sql/                  # Multi-schema PostgreSQL initialization DDL (init_postgres_schemas.sql)
└── docs/                        # Interactive HTML/CSS/SVG Living Documentation site (GitHub Pages)
```

---

## Quickstart & Local Execution

### 1. Prerequisites
- **Docker & Docker Compose**
- **Python 3.11+** with [`uv`](https://github.com/astral-sh/uv) package manager
- **PostgreSQL instance** (local container or Neon Serverless PostgreSQL) initialized with [`deploy/sql/init_postgres_schemas.sql`](deploy/sql/init_postgres_schemas.sql)
- **Google Cloud Project** with BigQuery dataset (`commerce_analytics`), GCS bucket, and Application Default Credentials (`gcloud auth application-default login`)

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your POSTGRES_*, GCP_PROJECT_ID, and BRONZE_BUCKET settings
```

### 3. Launch the Stack
```bash
docker compose up --build -d
```

### 4. Access Service Endpoints
- **Centralized Commerce Portal**: `http://localhost:3000`
- **Unified Commerce API (Swagger UI)**: `http://localhost:8002/docs`
- **Delivery Tracker API (Swagger UI)**: `http://localhost:8000/docs`
- **Apache Airflow Orchestrator**: `http://localhost:8080` (`admin` / `admin`)

### 5. Run Automated ML & API Verification Suite
```bash
uv run pytest commerce-api/tests/test_ml_suite.py -v
```

