-- Подключение к базе данных STG и создание таблиц
\c stg;

CREATE TABLE IF NOT EXISTS stg_dim_customers (
    id SERIAL PRIMARY KEY,         -- Новый ID для STG
    source_id INT NOT NULL,        -- Оригинальный ID из MRR
    customer_name TEXT NOT NULL,
    country TEXT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_dim_products (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_fact_sales (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL,       
    mrr_id INT NOT NULL REFERENCES mrr.mrr_fact_sales(id) ON DELETE CASCADE,
    customer_id INT NOT NULL REFERENCES stg_dim_customers(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES stg_dim_products(id) ON DELETE CASCADE,
    qty INT NOT NULL CHECK (qty > 0),
    sale_date TIMESTAMP NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);