from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.api import router as commerce_router

app = FastAPI(
    title="Commerce & Logistics API Gateway",
    description="Unified API Gateway serving Product Catalog (Browse), Real-time Inventory, Order Checkout, and Real-time Order Tracking",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(commerce_router)


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "commerce-api"}


@app.get("/")
def root():
    return {
        "service": "Commerce & Logistics API Gateway",
        "docs_url": "/docs",
        "endpoints": [
            "/api/catalog/products",
            "/api/catalog/categories",
            "/api/catalog/products/{product_id}",
            "/api/inventory/{product_id}",
            "/api/orders/checkout",
            "/api/tracking/{order_id}",
        ],
    }
