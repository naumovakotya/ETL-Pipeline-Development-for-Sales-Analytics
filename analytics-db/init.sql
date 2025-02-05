-- Создание схем
CREATE SCHEMA IF NOT EXISTS mrr;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS dwh_metadata;

-- Таблицы для STG 
CREATE TABLE IF NOT EXISTS stg.stg_dim_customers (
    id SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    country TEXT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.stg_dim_products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.stg_fact_sales (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES stg.stg_dim_customers(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES stg.stg_dim_products(id) ON DELETE CASCADE,
    qty INT NOT NULL CHECK (qty > 0),
    sale_date TIMESTAMP NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Таблицы для MRR
CREATE TABLE IF NOT EXISTS mrr.mrr_dim_customers (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mrr.mrr_fact_sales (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mrr.mrr_dim_products (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Таблицы для DWH
CREATE TABLE IF NOT EXISTS dwh.dwh_dim_customers (
    id SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dwh_dim_products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    total_sales INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dwh.dwh_fact_sales (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES dwh.dwh_dim_customers(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES dwh.dwh_dim_products(id) ON DELETE CASCADE,
    qty INT NOT NULL CHECK (qty > 0),
    sale_date TIMESTAMP NOT NULL
);

-- High Water Mark
CREATE TABLE IF NOT EXISTS dwh_metadata.high_water_mark (
    table_name TEXT PRIMARY KEY, 
    last_update TIMESTAMP DEFAULT '2000-01-01 00:00:00'
);

INSERT INTO dwh_metadata.high_water_mark (table_name, last_update)
VALUES ('mrr_fact_sales', '2000-01-01 00:00:00')
ON CONFLICT (table_name) DO UPDATE SET last_update = EXCLUDED.last_update;

-- Система логированиия
CREATE TABLE dwh_metadata.etl_logs (
    id SERIAL PRIMARY KEY,
    package_name TEXT,
    log_time TIMESTAMP DEFAULT NOW(),
    status TEXT,      
    message TEXT
);

-- Предположим, что в таблице dwh_dim_products есть столбец total_sales для хранения агрегированных продаж.
CREATE OR REPLACE PROCEDURE dwh.process_sales_data()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
BEGIN
    -- Перебираем все записи из таблицы фактов
    FOR rec IN SELECT * FROM dwh.dwh_fact_sales LOOP
        BEGIN
            -- Пример обновления агрегата в таблице измерений для продукта
            UPDATE dwh.dwh_dim_products
            SET total_sales = total_sales + rec.qty
            WHERE id = rec.product_id;
        EXCEPTION WHEN OTHERS THEN
            -- В случае ошибки записываем лог в таблицу etl_logs
            INSERT INTO dwh_metadata.etl_logs (package_name, log_time, status, message)
            VALUES ('process_sales_data', NOW(), 'ERROR', SQLERRM);
        END;
    END LOOP;
    
    -- Если всё прошло успешно, записываем успешный лог
    INSERT INTO dwh_metadata.etl_logs (package_name, log_time, status, message)
    VALUES ('process_sales_data', NOW(), 'SUCCESS', 'Агрегирование продаж завершено.');
END;
$$;

CREATE OR REPLACE FUNCTION dwh.get_product_total_sales(p_product_id INT)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    total INT;
BEGIN
    SELECT total_sales INTO total
    FROM dwh.dwh_dim_products
    WHERE id = p_product_id;
    
    RETURN total;
EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN 0;
END;
$$;
