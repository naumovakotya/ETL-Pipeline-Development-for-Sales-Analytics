-- Подключение к базе данных DWH и создание таблиц
\c dwh;

-- Создание схемы для хранения логов и high water mark
CREATE SCHEMA IF NOT EXISTS dwh_metadata;

CREATE TABLE IF NOT EXISTS dwh_dim_customers (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,        -- ID клиента из STG 
    customer_name TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh_dim_products (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    total_sales INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dwh_fact_sales (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    customer_id INT NOT NULL REFERENCES dwh_dim_customers(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES dwh_dim_products(id) ON DELETE CASCADE,
    qty INT NOT NULL CHECK (qty > 0),
    sale_date TIMESTAMP NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

-- High Water Mark
CREATE TABLE IF NOT EXISTS dwh_metadata.high_water_mark (
    table_name TEXT PRIMARY KEY, 
    last_update TIMESTAMP DEFAULT '2000-01-01 00:00:00'
);

-- Система логированиия
CREATE TABLE dwh_metadata.logs (
    id SERIAL PRIMARY KEY,
    process_name TEXT,
    log_time TIMESTAMP DEFAULT NOW(),
    status TEXT,      
    message TEXT
);