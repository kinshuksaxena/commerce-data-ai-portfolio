-- =========================================================================
-- Unified Commerce Platform — PostgreSQL Multi-Schema Initialization DDL
-- Enforces Domain Isolation across: customers, delivery, inventory, catalog, orders
-- =========================================================================

-- 1. Create Dedicated Domain Schemas
CREATE SCHEMA IF NOT EXISTS customers;
CREATE SCHEMA IF NOT EXISTS delivery;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE SCHEMA IF NOT EXISTS orders;
CREATE SCHEMA IF NOT EXISTS airflow;

-- =========================================================================
-- SCHEMA: customers (Customer Dimensions, Loyalty Tiers & Geolocation)
-- =========================================================================
CREATE TABLE IF NOT EXISTS customers.customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128),
    phone VARCHAR(32),
    loyalty_tier VARCHAR(32) DEFAULT 'STANDARD',
    webhook_url TEXT,
    address TEXT,
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =========================================================================
-- SCHEMA: delivery (Delivery Tracking & Fleet Route Logistics)
-- =========================================================================
CREATE TABLE IF NOT EXISTS delivery.vehicles (
    vehicle_id VARCHAR(64) PRIMARY KEY,
    model VARCHAR(64),
    license_plate VARCHAR(32),
    status VARCHAR(32) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.drivers (
    driver_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    phone VARCHAR(32),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.routes (
    route_id VARCHAR(64) PRIMARY KEY,
    driver_id VARCHAR(64) REFERENCES delivery.drivers(driver_id),
    vehicle_id VARCHAR(64) REFERENCES delivery.vehicles(vehicle_id),
    status VARCHAR(32) DEFAULT 'IN_PROGRESS', -- PLANNED, IN_PROGRESS, COMPLETED
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.route_stops (
    stop_id SERIAL PRIMARY KEY,
    route_id VARCHAR(64) REFERENCES delivery.routes(route_id),
    stop_sequence INT NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    address TEXT,
    status VARCHAR(32) DEFAULT 'PENDING', -- PENDING, EN_ROUTE, ARRIVED, COMPLETED
    departed_previous_stop_at TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    transit_duration_seconds INT,
    service_duration_seconds INT,
    UNIQUE (route_id, stop_sequence)
);

CREATE TABLE IF NOT EXISTS delivery.route_stops_history (
    history_id BIGSERIAL PRIMARY KEY,
    stop_id INT,
    route_id VARCHAR(64),
    stop_sequence INT,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    address TEXT,
    stop_status VARCHAR(32),
    departed_previous_stop_at TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    transit_duration_seconds INT,
    service_duration_seconds INT,
    total_stop_duration_seconds INT,
    archived_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    stop_id INT REFERENCES delivery.route_stops(stop_id),
    status VARCHAR(32) DEFAULT 'OUT_FOR_DELIVERY', -- PENDING, OUT_FOR_DELIVERY, DELIVERED
    order_total DECIMAL(10, 2),
    estimated_delivery_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.vehicle_latest_status (
    vehicle_id VARCHAR(64) PRIMARY KEY REFERENCES delivery.vehicles(vehicle_id),
    current_route_id VARCHAR(64) REFERENCES delivery.routes(route_id),
    last_latitude DECIMAL(9, 6),
    last_longitude DECIMAL(9, 6),
    last_ping_time TIMESTAMPTZ,
    ping_count_last_window INT DEFAULT 0,
    current_stop_sequence INT DEFAULT 1,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.dead_letter_messages (
    id SERIAL PRIMARY KEY,
    raw_payload TEXT,
    error_message TEXT,
    received_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery.webhook_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(64),
    vehicle_id VARCHAR(64),
    route_id VARCHAR(64),
    order_id VARCHAR(64),
    customer_name VARCHAR(128),
    message TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =========================================================================
-- SCHEMA: inventory (Warehouse Stock Levels, Audit Trail & Idempotency)
-- =========================================================================
CREATE TABLE IF NOT EXISTS inventory.inventory_levels (
    product_id VARCHAR(64) NOT NULL,
    warehouse_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(128),
    category VARCHAR(64),
    quantity_available INT NOT NULL DEFAULT 0,
    reserved_quantity INT NOT NULL DEFAULT 0,
    safety_stock INT NOT NULL DEFAULT 10,
    unit_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS inventory.inventory_audit_log (
    audit_id SERIAL PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL,
    warehouse_id VARCHAR(64) NOT NULL,
    change_type VARCHAR(32) NOT NULL,
    quantity_change INT NOT NULL,
    quantity_after INT NOT NULL,
    reason TEXT,
    event_timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory.processed_inventory_events (
    event_id VARCHAR(128) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL,
    warehouse_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- =========================================================================
-- SCHEMA: catalog (Product Master & Reverse-ETL Enriched Sales Metrics)
-- =========================================================================
CREATE TABLE IF NOT EXISTS catalog.product_catalog (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(128) NOT NULL,
    category VARCHAR(64),
    unit_price NUMERIC(10, 2),
    total_units_sold BIGINT DEFAULT 0,
    total_gross_revenue NUMERIC(12, 2) DEFAULT 0.00,
    last_synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- =========================================================================
-- SCHEMA: orders (E-Commerce Order Capture, Line Items, Shipping & Audit)
-- =========================================================================
CREATE TABLE IF NOT EXISTS orders.orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    subtotal_amount NUMERIC(10, 2) NOT NULL CHECK (subtotal_amount >= 0),
    tax_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00 CHECK (tax_amount >= 0),
    shipping_fee NUMERIC(10, 2) NOT NULL DEFAULT 0.00 CHECK (shipping_fee >= 0),
    discount_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00 CHECK (discount_amount >= 0),
    total_amount NUMERIC(10, 2) NOT NULL CHECK (total_amount >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    promo_code VARCHAR(32),
    idempotency_key VARCHAR(128) UNIQUE,
    estimated_delivery_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders.order_items (
    item_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders.orders(order_id) ON DELETE CASCADE,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(128) NOT NULL,
    sku VARCHAR(64),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount_amount NUMERIC(10, 2) NOT NULL DEFAULT 0.00 CHECK (discount_amount >= 0),
    line_total NUMERIC(10, 2) NOT NULL CHECK (line_total >= 0),
    attributes JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON orders.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON orders.order_items(product_id);

CREATE TABLE IF NOT EXISTS orders.order_shipping_details (
    order_id VARCHAR(64) PRIMARY KEY REFERENCES orders.orders(order_id) ON DELETE CASCADE,
    recipient_name VARCHAR(128) NOT NULL,
    phone VARCHAR(32),
    address_line1 TEXT NOT NULL,
    address_line2 TEXT,
    city VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    country_code VARCHAR(2) NOT NULL DEFAULT 'US',
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    shipping_method VARCHAR(32) NOT NULL DEFAULT 'STANDARD',
    tracking_number VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders.order_payments (
    payment_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders.orders(order_id) ON DELETE CASCADE,
    payment_method VARCHAR(32) NOT NULL,
    payment_status VARCHAR(32) NOT NULL DEFAULT 'AUTHORIZED',
    amount NUMERIC(10, 2) NOT NULL,
    transaction_reference VARCHAR(128),
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders.order_status_history (
    history_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders.orders(order_id) ON DELETE CASCADE,
    previous_status VARCHAR(32),
    new_status VARCHAR(32) NOT NULL,
    changed_by VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    notes TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_history_order_id ON orders.order_status_history(order_id);

-- =========================================================================
-- COMPATIBILITY VIEWS
-- =========================================================================
CREATE OR REPLACE VIEW public.vehicles AS SELECT * FROM delivery.vehicles;
CREATE OR REPLACE VIEW public.drivers AS SELECT * FROM delivery.drivers;
CREATE OR REPLACE VIEW public.customers AS SELECT * FROM customers.customers;
CREATE OR REPLACE VIEW delivery.customers AS SELECT * FROM customers.customers;
CREATE OR REPLACE VIEW public.routes AS SELECT * FROM delivery.routes;
CREATE OR REPLACE VIEW public.route_stops AS SELECT * FROM delivery.route_stops;
CREATE OR REPLACE VIEW public.orders AS SELECT * FROM delivery.orders;
CREATE OR REPLACE VIEW public.vehicle_latest_status AS SELECT * FROM delivery.vehicle_latest_status;
CREATE OR REPLACE VIEW public.dead_letter_messages AS SELECT * FROM delivery.dead_letter_messages;
CREATE OR REPLACE VIEW public.webhook_events AS SELECT * FROM delivery.webhook_events;
CREATE OR REPLACE VIEW public.product_catalog AS SELECT * FROM catalog.product_catalog;
CREATE OR REPLACE VIEW public.inventory_levels AS SELECT * FROM inventory.inventory_levels;
CREATE OR REPLACE VIEW public.inventory_audit_log AS SELECT * FROM inventory.inventory_audit_log;
CREATE OR REPLACE VIEW public.ecommerce_orders AS SELECT * FROM orders.orders;
CREATE OR REPLACE VIEW public.order_items AS SELECT * FROM orders.order_items;

