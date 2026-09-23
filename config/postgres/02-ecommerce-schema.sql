-- ==============================================================================
-- LakeFlow Production E-Commerce Schema & Seed Data
-- ==============================================================================

\c platform_db;

CREATE SCHEMA IF NOT EXISTS platform;

-- 1. Customers Table
CREATE TABLE IF NOT EXISTS platform.customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name VARCHAR(64) NOT NULL,
    last_name VARCHAR(64) NOT NULL,
    email VARCHAR(128) UNIQUE NOT NULL,
    phone VARCHAR(32),
    country VARCHAR(64) DEFAULT 'USA',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. Products Table
CREATE TABLE IF NOT EXISTS platform.products (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. Orders Table
CREATE TABLE IF NOT EXISTS platform.orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID REFERENCES platform.customers(customer_id) ON DELETE CASCADE,
    order_number VARCHAR(32) UNIQUE NOT NULL,
    order_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    shipping_address TEXT,
    order_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4. Order Items Table
CREATE TABLE IF NOT EXISTS platform.order_items (
    item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID REFERENCES platform.orders(order_id) ON DELETE CASCADE,
    product_id UUID REFERENCES platform.products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ==============================================================================
-- Ensure REPLICA IDENTITY FULL for all CDC tables to capture old & new state
-- ==============================================================================
ALTER TABLE platform.customers REPLICA IDENTITY FULL;
ALTER TABLE platform.products REPLICA IDENTITY FULL;
ALTER TABLE platform.orders REPLICA IDENTITY FULL;
ALTER TABLE platform.order_items REPLICA IDENTITY FULL;

-- Add all tables to the publication
ALTER PUBLICATION platform_cdc_publication ADD TABLE 
    platform.customers,
    platform.products,
    platform.orders,
    platform.order_items;

-- ==============================================================================
-- Initial Seed Data
-- ==============================================================================
INSERT INTO platform.customers (customer_id, first_name, last_name, email, phone, country)
VALUES
    ('c1000000-0000-0000-0000-000000000001', 'Alice', 'Chen', 'alice.chen@enterprise.io', '+1-415-555-0101', 'USA'),
    ('c1000000-0000-0000-0000-000000000002', 'Bob', 'Smith', 'bob.smith@cloudtech.com', '+1-206-555-0102', 'USA'),
    ('c1000000-0000-0000-0000-000000000003', 'Carlos', 'Garcia', 'carlos.garcia@globalfin.es', '+34-91-555-0103', 'Spain'),
    ('c1000000-0000-0000-0000-000000000004', 'Deepa', 'Patel', 'deepa.patel@datastack.in', '+91-80-555-0104', 'India'),
    ('c1000000-0000-0000-0000-000000000005', 'Erik', 'Lindqvist', 'erik.lindqvist@nordicdev.se', '+46-8-555-0105', 'Sweden')
ON CONFLICT (email) DO NOTHING;

INSERT INTO platform.products (product_id, sku, name, category, price, stock_quantity)
VALUES
    ('p2000000-0000-0000-0000-000000000001', 'COMP-001', 'Data Engineering Workstation', 'Hardware', 2499.00, 50),
    ('p2000000-0000-0000-0000-000000000002', 'COMP-002', 'Ultra-Wide 38in Monitor', 'Monitors', 899.99, 120),
    ('p2000000-0000-0000-0000-000000000003', 'NET-001', '10GbE SFP+ Managed Switch', 'Networking', 450.00, 35),
    ('p2000000-0000-0000-0000-000000000004', 'PERI-001', 'Low-Profile Mechanical Keyboard', 'Peripherals', 149.50, 300),
    ('p2000000-0000-0000-0000-000000000005', 'STOR-001', '4TB NVMe PCIe 4.0 SSD', 'Storage', 320.00, 200)
ON CONFLICT (sku) DO NOTHING;

INSERT INTO platform.orders (order_id, customer_id, order_number, order_status, total_amount, shipping_address)
VALUES
    ('o3000000-0000-0000-0000-000000000001', 'c1000000-0000-0000-0000-000000000001', 'ORD-2026-0001', 'COMPLETED', 3398.99, '500 Howard St, San Francisco, CA'),
    ('o3000000-0000-0000-0000-000000000002', 'c1000000-0000-0000-0000-000000000002', 'ORD-2026-0002', 'PROCESSING', 469.50, '1200 Westlake Ave N, Seattle, WA'),
    ('o3000000-0000-0000-0000-000000000003', 'c1000000-0000-0000-0000-000000000004', 'ORD-2026-0003', 'SHIPPED', 640.00, '45 Outer Ring Rd, Bengaluru, KA')
ON CONFLICT (order_number) DO NOTHING;

INSERT INTO platform.order_items (item_id, order_id, product_id, quantity, unit_price)
VALUES
    ('i4000000-0000-0000-0000-000000000001', 'o3000000-0000-0000-0000-000000000001', 'p2000000-0000-0000-0000-000000000001', 1, 2499.00),
    ('i4000000-0000-0000-0000-000000000002', 'o3000000-0000-0000-0000-000000000001', 'p2000000-0000-0000-0000-000000000002', 1, 899.99),
    ('i4000000-0000-0000-0000-000000000003', 'o3000000-0000-0000-0000-000000000002', 'p2000000-0000-0000-0000-000000000004', 1, 149.50),
    ('i4000000-0000-0000-0000-000000000004', 'o3000000-0000-0000-0000-000000000002', 'p2000000-0000-0000-0000-000000000005', 1, 320.00),
    ('i4000000-0000-0000-0000-000000000005', 'o3000000-0000-0000-0000-000000000003', 'p2000000-0000-0000-0000-000000000005', 2, 320.00)
ON CONFLICT (item_id) DO NOTHING;
