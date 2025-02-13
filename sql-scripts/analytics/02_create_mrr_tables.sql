-- Подключение к базе данных MRR и создание таблиц
\c mrr;

CREATE TABLE IF NOT EXISTS mrr_dim_customers (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mrr_fact_sales (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mrr_dim_products (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);