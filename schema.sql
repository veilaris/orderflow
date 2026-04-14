-- Run this in Supabase SQL Editor before first sync.
-- Table: orders
create table if not exists orders (
    id               bigint primary key generated always as identity,
    retailcrm_id     integer      not null unique,   -- RetailCRM internal order id
    retailcrm_number text,                           -- Human-readable order number (e.g. "63A")
    status           text,
    order_method     text,
    first_name       text,
    last_name        text,
    phone            text,
    email            text,
    delivery_city    text,
    delivery_address text,
    utm_source       text,
    total_sum        numeric(12, 2),
    created_at       timestamptz,                    -- order creation time in RetailCRM
    updated_at       timestamptz,                    -- last update time in RetailCRM
    synced_at        timestamptz default now()       -- last time this row was written by sync
);

-- Table: order_items
create table if not exists order_items (
    id                bigint primary key generated always as identity,
    order_id          bigint not null references orders (id) on delete cascade,
    retailcrm_number  text,      -- Human-readable order number, e.g. "113А"
    product_name      text,
    quantity          numeric(10, 3),
    price             numeric(12, 2)
);

-- Index for fast lookup when upserting items by order
create index if not exists order_items_order_id_idx on order_items (order_id);
