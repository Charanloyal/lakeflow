#!/usr/bin/env bash
# Source schema, CDC publication, heartbeat/signal tables and deterministic seed data (valid UUIDs via md5()).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname lakeflow <<'SQL'
SET ROLE lakeflow_app;

CREATE SCHEMA shop;
CREATE SCHEMA lakeflow_ops;

CREATE TABLE shop.customers (
    customer_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email       text NOT NULL UNIQUE,
    full_name   text NOT NULL CHECK (length(full_name) BETWEEN 1 AND 200),
    country     char(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- The source deliberately accepts JPY while contract v2 does not: this is the contract-drift/DLQ-replay scenario.
CREATE TABLE shop.orders (
    order_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id uuid NOT NULL REFERENCES shop.customers (customer_id),
    status      text NOT NULL CHECK (status IN ('PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    amount      numeric(12, 2) NOT NULL CHECK (amount >= 0),
    currency    char(3) NOT NULL CHECK (currency IN ('USD', 'EUR', 'GBP', 'INR', 'JPY')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_customer_idx ON shop.orders (customer_id);
CREATE INDEX orders_updated_idx ON shop.orders (updated_at);

CREATE FUNCTION shop.touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;
CREATE TRIGGER customers_touch BEFORE UPDATE ON shop.customers FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER orders_touch BEFORE UPDATE ON shop.orders FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();

-- Full before-images make update/delete events self-describing (trade-off: more WAL per change).
ALTER TABLE shop.customers REPLICA IDENTITY FULL;
ALTER TABLE shop.orders REPLICA IDENTITY FULL;

-- Heartbeat target: Debezium updates this row every heartbeat.interval.ms so the replication slot keeps
-- advancing (and WAL can be recycled) even when the captured shop tables are idle.
CREATE TABLE lakeflow_ops.debezium_heartbeat (id int PRIMARY KEY, ts timestamptz NOT NULL);
INSERT INTO lakeflow_ops.debezium_heartbeat VALUES (1, now());

-- Debezium source signalling channel (incremental snapshots / backfill).
CREATE TABLE lakeflow_ops.debezium_signal (id varchar(64) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048));

CREATE PUBLICATION lakeflow_cdc FOR TABLE
    shop.customers, shop.orders, lakeflow_ops.debezium_heartbeat, lakeflow_ops.debezium_signal;

-- Least privilege: Debezium reads (snapshots), updates the heartbeat row, and streams via its REPLICATION role.
GRANT USAGE ON SCHEMA shop, lakeflow_ops TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA shop, lakeflow_ops TO debezium;
GRANT UPDATE ON lakeflow_ops.debezium_heartbeat TO debezium;
-- Incremental snapshots write open/close watermark rows into the signal table.
GRANT INSERT ON lakeflow_ops.debezium_signal TO debezium;
-- Trino reads the source for reconciliation only.
GRANT USAGE ON SCHEMA shop TO trino_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO trino_reader;

INSERT INTO shop.customers (customer_id, email, full_name, country, created_at, updated_at)
SELECT md5('lakeflow-customer-' || i)::uuid,
       'customer' || i || '@example.com',
       (ARRAY['Ada', 'Grace', 'Alan', 'Edsger', 'Barbara', 'Donald', 'Margaret', 'Ken'])[1 + i % 8] || ' Customer ' || i,
       (ARRAY['US', 'GB', 'DE', 'IN', 'FR', 'JP'])[1 + i % 6],
       timestamptz '2026-09-01 00:00:00+00' + i * interval '1 hour',
       timestamptz '2026-09-01 00:00:00+00' + i * interval '1 hour'
FROM generate_series(1, 50) AS i;

INSERT INTO shop.orders (order_id, customer_id, status, amount, currency, created_at, updated_at)
SELECT md5('lakeflow-order-' || i)::uuid,
       md5('lakeflow-customer-' || (1 + i % 50))::uuid,
       (ARRAY['PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED'])[1 + i % 5],
       round(((i * 3779) % 50000) / 100.0 + 5, 2),
       (ARRAY['USD', 'EUR', 'GBP', 'INR'])[1 + i % 4],
       timestamptz '2026-09-10 00:00:00+00' + i * interval '7 minutes',
       timestamptz '2026-09-10 00:00:00+00' + i * interval '7 minutes'
FROM generate_series(1, 200) AS i;
SQL
