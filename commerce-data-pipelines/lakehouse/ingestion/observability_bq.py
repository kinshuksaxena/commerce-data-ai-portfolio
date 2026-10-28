import os
import json
import logging
from typing import Optional, Dict, Any
from google.cloud import bigquery

logger = logging.getLogger("observability_logger")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BIGQUERY_DATASET", "commerce_analytics")

_bq_client: Optional[bigquery.Client] = None

def get_bq_client() -> Optional[bigquery.Client]:
    global _bq_client
    if _bq_client is None:
        try:
            _bq_client = bigquery.Client(project=PROJECT_ID)
        except Exception as e:
            logger.warning(f"Could not initialize BigQuery client for observability: {e}")
            return None
    return _bq_client


def log_event(service_name: str, log_level: str, message: str, trace_id: str = "", span_id: str = "", metadata: Optional[Dict[str, Any]] = None):
    """Asynchronously or synchronously streams application log events to BigQuery application_logs."""
    client = get_bq_client()
    if not client:
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.application_logs"
    row = {
        "service_name": service_name,
        "log_level": log_level.upper(),
        "message": message,
        "trace_id": trace_id or "",
        "span_id": span_id or "",
        "metadata": json.dumps(metadata or {})
    }

    try:
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            logger.warning(f"Failed to stream log to BigQuery: {errors}")
    except Exception as e:
        logger.warning(f"Exception streaming log to BigQuery: {e}")


def record_pipeline_metric(pipeline_name: str, metric_name: str, metric_value: float, labels: Optional[Dict[str, Any]] = None):
    """Streams a pipeline execution metric to BigQuery pipeline_metrics."""
    client = get_bq_client()
    if not client:
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.pipeline_metrics"
    row = {
        "pipeline_name": pipeline_name,
        "metric_name": metric_name,
        "metric_value": float(metric_value),
        "labels": json.dumps(labels or {})
    }

    try:
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            logger.warning(f"Failed to record pipeline metric to BigQuery: {errors}")
    except Exception as e:
        logger.warning(f"Exception recording metric to BigQuery: {e}")
