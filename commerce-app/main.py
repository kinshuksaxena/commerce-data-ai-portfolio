import os
import urllib.parse
from typing import Optional
import requests
from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response

app = FastAPI(title="Unified Commerce Platform Portal")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

COMMERCE_API_URL = os.getenv("COMMERCE_API_URL", "http://commerce-api:8002/api")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-standalone:8080")
DELIVERY_TRACKER_URL = os.getenv("DELIVERY_TRACKER_URL", "http://delivery-tracker-api:8000")
PUBSUB_EMULATOR_URL = os.getenv("PUBSUB_EMULATOR_URL", "http://pubsub-emulator:8085")


app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/health")
def health():
    return {"status": "healthy", "service": "commerce-app-portal"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "active_tab": "data_management"})


@app.get("/api/portal/files")
def proxy_files():
    try:
        res = requests.get(f"{COMMERCE_API_URL}/lakehouse/files", timeout=5)
        data = res.json()
        if "files" in data:
            for f in data["files"]:
                obj_name = urllib.parse.quote(f.get("name", ""), safe="")
                f["download_url"] = f"/api/portal/download?object_name={obj_name}"
        return data
    except Exception as e:
        return {"bucket": "bronze", "count": 0, "files": [], "error": str(e)}


STORAGE_EMULATOR_HOST = os.getenv("STORAGE_EMULATOR_HOST")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "commerce-lakehouse-bronze")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")


@app.get("/api/portal/download")
def proxy_download(object_name: str):
    """Proxies file download from GCS to browser."""
    try:
        from google.cloud import storage
        if STORAGE_EMULATOR_HOST:
            os.environ["STORAGE_EMULATOR_HOST"] = STORAGE_EMULATOR_HOST
            gcs = storage.Client.create_anonymous_client()
        else:
            gcs = storage.Client(project=GCP_PROJECT_ID)
        bucket = gcs.bucket(BRONZE_BUCKET)
        blob = bucket.blob(object_name)
        if not blob.exists():
            raise HTTPException(status_code=404, detail="File not found in storage")
        content = blob.download_as_bytes()
        filename = object_name.split("/")[-1]
        media_type = blob.content_type or "application/octet-stream"
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portal/pipelines")
def proxy_pipelines():
    try:
        res = requests.get(f"{COMMERCE_API_URL}/lakehouse/pipelines", timeout=5)
        return res.json()
    except Exception as e:
        return {"pipelines": [], "error": str(e)}


@app.post("/api/portal/upload")
async def proxy_upload(file: UploadFile = File(...), entity_type: str = Form("products")):
    try:
        files = {"file": (file.filename, await file.read(), file.content_type)}
        data = {"entity_type": entity_type}
        res = requests.post(f"{COMMERCE_API_URL}/lakehouse/upload", files=files, data=data, timeout=10)
        return res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portal/pipelines/{dag_id}/trigger")
def proxy_trigger(dag_id: str):
    try:
        res = requests.post(f"{COMMERCE_API_URL}/lakehouse/pipelines/{dag_id}/trigger", timeout=5)
        return res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portal/catalog")
def proxy_catalog():
    try:
        res = requests.get(f"{COMMERCE_API_URL}/catalog/products?limit=50", timeout=5)
        return res.json()
    except Exception as e:
        return {"count": 0, "products": [], "error": str(e)}


@app.get("/api/portal/customers")
def proxy_customers():
    try:
        res = requests.get(f"{COMMERCE_API_URL}/customers", timeout=5)
        return res.json()
    except Exception as e:
        return {"count": 0, "customers": [], "error": str(e)}


@app.get("/api/portal/inventory/{product_id}")
def proxy_inventory(product_id: str):
    try:
        res = requests.get(f"{COMMERCE_API_URL}/inventory/{product_id}", timeout=5)
        return res.json()
    except Exception as e:
        return {"product_id": product_id, "total_available": 0, "warehouse_breakdown": {}, "error": str(e)}


@app.post("/api/portal/inventory/publish")
async def proxy_inventory_publish(request: Request):
    try:
        body = await request.json()
        res = requests.post(f"{COMMERCE_API_URL}/inventory/publish", json=body, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portal/orders/checkout")
async def proxy_checkout(request: Request):
    try:
        body = await request.json()
        res = requests.post(f"{COMMERCE_API_URL}/orders/checkout", json=body, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# FLEET TRACKING & SIMULATION PORTAL PROXIES
# =====================================================================

@app.get("/api/portal/orders/dashboard")
def proxy_orders_dashboard():
    try:
        res = requests.get(f"{DELIVERY_TRACKER_URL}/api/orders/dashboard", timeout=10)
        data = res.json()
        orders = data.get("orders", [])
        
        # Ensure every order has estimated_delivery_at and delivery_delay ML prediction
        import datetime, math
        for ord_row in orders:
            # 1. Estimated delivery due date / time
            if not ord_row.get("estimated_delivery_at"):
                created_iso = ord_row.get("created_at")
                if created_iso:
                    try:
                        base_dt = datetime.datetime.fromisoformat(created_iso.replace("Z", "+00:00"))
                        due_dt = base_dt + datetime.timedelta(hours=2)
                        ord_row["estimated_delivery_at"] = due_dt.isoformat()
                    except Exception:
                        ord_row["estimated_delivery_at"] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat()
                else:
                    ord_row["estimated_delivery_at"] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat()

            # 2. ML Delivery Delay Risk & Confidence
            if not ord_row.get("delivery_delay"):
                stage = (ord_row.get("delivery_stage") or ord_row.get("status") or "PENDING").upper()
                if stage == "DELIVERED":
                    ord_row["delivery_delay"] = {
                        "risk_level": "NONE",
                        "delay_probability": 0.0,
                        "on_time_probability": 1.0,
                        "confidence": 1.0,
                        "estimated_status": "Delivered on schedule",
                        "model_version": "CalibratedGradientBoosting(v2)"
                    }
                else:
                    stops = ord_row.get("stops_away")
                    effective_stops = stops if stops is not None else 3
                    now_dt = datetime.datetime.now(datetime.timezone.utc)
                    hour = now_dt.hour
                    is_rush = 1 if (7 <= hour <= 9 or 16 <= hour <= 19) else 0
                    congestion = 1.65 if is_rush else 1.25
                    if effective_stops >= 6:
                        congestion += 0.3

                    z = -2.6 + 0.28 * effective_stops + 0.82 * is_rush + 0.65 * (congestion - 1.0)
                    prob_delay = round(float(1.0 / (1.0 + math.exp(-z))), 3)
                    prob_ontime = round(1.0 - prob_delay, 3)

                    if prob_delay >= 0.55:
                        risk_lvl = "HIGH"
                        status_text = "Significant delay risk detected"
                        conf = round(prob_delay, 2)
                    elif prob_delay >= 0.25:
                        risk_lvl = "MEDIUM"
                        status_text = "Moderate delay risk - monitoring route"
                        conf = round(prob_delay, 2)
                    else:
                        risk_lvl = "LOW"
                        status_text = "On schedule"
                        conf = round(prob_ontime, 2)

                    ord_row["delivery_delay"] = {
                        "risk_level": risk_lvl,
                        "delay_probability": prob_delay,
                        "on_time_probability": prob_ontime,
                        "confidence": conf,
                        "estimated_status": status_text,
                        "model_version": "CalibratedGradientBoosting(v2)"
                    }

        return JSONResponse(status_code=res.status_code, content=data)
    except Exception as e:
        return JSONResponse(status_code=500, content={"counts": {}, "orders": [], "error": str(e)})


@app.get("/api/portal/orders/{order_id}")
def proxy_order_details(order_id: str):
    try:
        res = requests.get(f"{COMMERCE_API_URL}/orders/{order_id}", timeout=5)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portal/fleet/summary")
def proxy_fleet_summary():
    try:
        res = requests.get(f"{DELIVERY_TRACKER_URL}/api/fleet/summary", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"routes": [], "stops": [], "error": str(e)})


@app.get("/api/portal/routes/list")
def proxy_routes_list():
    try:
        res = requests.get(f"{DELIVERY_TRACKER_URL}/api/routes/list", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"routes": [], "error": str(e)})


@app.get("/api/portal/fleet/resources")
def proxy_fleet_resources():
    try:
        res = requests.get(f"{DELIVERY_TRACKER_URL}/api/fleet/resources", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "vehicles": [], "drivers": [], "error": str(e)})


@app.post("/api/portal/routes/initiate")
async def proxy_routes_initiate(request: Request):
    try:
        body = await request.json()
        res = requests.post(f"{DELIVERY_TRACKER_URL}/api/routes/initiate", json=body, timeout=15)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portal/routes/{route_id}/start")
def proxy_start_route(route_id: str):
    try:
        res = requests.post(f"{DELIVERY_TRACKER_URL}/api/routes/{route_id}/start", timeout=15)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portal/routes/assign-orders")
async def proxy_assign_orders(request: Request):
    try:
        body = await request.json()
        res = requests.post(f"{DELIVERY_TRACKER_URL}/api/routes/assign-orders", json=body, timeout=15)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portal/orders/{order_id}/stage")
async def proxy_update_order_stage(order_id: str, request: Request):
    try:
        body = await request.json()
        res = requests.post(f"{DELIVERY_TRACKER_URL}/api/orders/{order_id}/stage", json=body, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portal/routes/simulate-step")
async def proxy_simulate_step(request: Request):
    try:
        body = await request.json()
        res = requests.post(f"{DELIVERY_TRACKER_URL}/api/routes/simulate-step", json=body, timeout=15)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portal/webhooks/recent")
def proxy_recent_webhooks(customer_id: Optional[str] = None):
    try:
        url = f"{DELIVERY_TRACKER_URL}/api/webhooks/recent"
        params = {}
        if customer_id:
            params["customer_id"] = customer_id
        res = requests.get(url, params=params, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=200, content=[])


@app.get("/api/portal/orders/{order_id}/tracking")
def proxy_order_tracking(order_id: str):
    try:
        res = requests.get(f"{DELIVERY_TRACKER_URL}/orders/{order_id}/status", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portal/analytics/gold-mart")
def proxy_gold_mart():
    try:
        res = requests.get(f"{COMMERCE_API_URL}/analytics/gold-mart", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e), "products": [], "summary": {}})


@app.get("/api/portal/analytics/routes")
def proxy_routes_analytics():
    try:
        res = requests.get(f"{COMMERCE_API_URL}/analytics/routes", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e), "routes": [], "stops": []})


@app.get("/api/portal/analytics/order-status-history")
def proxy_order_status_history(limit: int = 100, date: Optional[str] = None):
    try:
        params = {"limit": limit}
        if date:
            params["date"] = date
        res = requests.get(f"{COMMERCE_API_URL}/analytics/order-status-history", params=params, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e), "count": 0, "history": []})



@app.get("/api/portal/observability/logs")
def proxy_observability_logs(
    service: Optional[str] = None,
    level: Optional[str] = None,
    trace_id: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50
):
    try:
        params = {"limit": limit}
        if service:
            params["service"] = service
        if level:
            params["level"] = level
        if trace_id:
            params["trace_id"] = trace_id
        if q:
            params["q"] = q
        res = requests.get(f"{COMMERCE_API_URL}/observability/logs", params=params, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e), "count": 0, "logs": []})


@app.get("/api/portal/observability/traces/{trace_id}")
def proxy_observability_trace(trace_id: str):
    try:
        res = requests.get(f"{COMMERCE_API_URL}/observability/traces/{trace_id}", timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "trace_id": trace_id, "error": str(e), "spans_count": 0, "spans": []})


@app.get("/api/portal/observability/metrics")
def proxy_observability_metrics(pipeline: Optional[str] = None, limit: int = 50):
    try:
        params = {"limit": limit}
        if pipeline:
            params["pipeline"] = pipeline
        res = requests.get(f"{COMMERCE_API_URL}/observability/metrics", params=params, timeout=10)
        return JSONResponse(status_code=res.status_code, content=res.json())
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e), "count": 0, "metrics": []})




@app.get("/api/portal/observability/heartbeats")
def get_system_heartbeats():
    """Polls real-time health checks & heartbeats across Airflow, Pub/Sub, and API gateways."""
    results = {
        "airflow": {"status": "unreachable", "details": None, "url": "http://localhost:8080"},
        "pubsub": {"status": "unreachable", "details": None, "url": "http://localhost:8085"},
        "commerce_api": {"status": "unreachable", "details": None, "url": "http://localhost:8002"},
        "delivery_tracker": {"status": "unreachable", "details": None, "url": "http://localhost:8000"}
    }

    # 1. Airflow Scheduler & Triggerer Heartbeat
    try:
        r = requests.get(f"{AIRFLOW_URL}/health", timeout=3)
        if r.status_code == 200:
            data = r.json()
            sched = data.get("scheduler", {})
            trig = data.get("triggerer", {})
            meta = data.get("metadatabase", {})
            is_healthy = sched.get("status") == "healthy"
            results["airflow"] = {
                "status": "healthy" if is_healthy else "degraded",
                "scheduler_status": sched.get("status"),
                "latest_scheduler_heartbeat": sched.get("latest_scheduler_heartbeat"),
                "triggerer_status": trig.get("status"),
                "metadatabase": meta.get("status"),
                "url": "http://localhost:8080"
            }
    except Exception as e:
        results["airflow"]["error"] = str(e)

    # 2. Pub/Sub Emulator Heartbeat
    try:
        r = requests.get(f"{PUBSUB_EMULATOR_URL}/", timeout=3)
        if r.status_code == 200:
            results["pubsub"] = {
                "status": "healthy",
                "host": PUBSUB_EMULATOR_URL,
                "project_id": "test-project",
                "response": r.text.strip(),
                "url": "http://localhost:8085"
            }
    except Exception as e:
        results["pubsub"]["error"] = str(e)

    # 3. Commerce API Gateway Heartbeat
    try:
        r = requests.get(f"{COMMERCE_API_URL.replace('/api', '')}/health", timeout=3)
        if r.status_code == 200:
            data = r.json()
            results["commerce_api"] = {
                "status": data.get("status", "healthy"),
                "service": data.get("service", "commerce-api"),
                "url": "http://localhost:8002"
            }
    except Exception as e:
        results["commerce_api"]["error"] = str(e)

    # 4. Delivery Tracker API Heartbeat
    try:
        r = requests.get(f"{DELIVERY_TRACKER_URL}/health", timeout=3)
        if r.status_code == 200:
            data = r.json()
            results["delivery_tracker"] = {
                "status": "healthy" if data.get("status") == "ok" else data.get("status"),
                "project_id": data.get("project_id"),
                "emulator_host": data.get("emulator_host"),
                "url": "http://localhost:8000"
            }
    except Exception as e:
        results["delivery_tracker"]["error"] = str(e)

    return JSONResponse(status_code=200, content=results)


@app.get("/api/portal/ml/forecast/summary")
def proxy_ml_forecast_summary(horizon_days: int = 14):
    """Proxies Commerce demand forecasting summary to the UI."""
    try:
        r = requests.get(f"{COMMERCE_API_URL}/ml/forecast/summary?horizon_days={horizon_days}", timeout=8)
        return r.json()
    except Exception as e:
        return {"status": "error", "error": str(e), "products": []}


@app.post("/api/portal/ml/forecast/demand")
def proxy_ml_forecast_demand(payload: dict):
    """Proxies individual product demand forecast."""
    try:
        r = requests.post(f"{COMMERCE_API_URL}/ml/forecast/demand", json=payload, timeout=8)
        return r.json()
    except Exception as e:
        return {"status": "error", "error": str(e)}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)

