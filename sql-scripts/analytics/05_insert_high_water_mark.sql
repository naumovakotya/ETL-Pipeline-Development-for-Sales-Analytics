\c dwh;

-- Наполнение таблицы с дефолтным значением high_water_mark
INSERT INTO dwh_metadata.high_water_mark (table_name, last_update)
VALUES ('mrr_fact_sales', '2000-01-01 00:00:00')
ON CONFLICT (table_name) DO UPDATE SET last_update = EXCLUDED.last_update;