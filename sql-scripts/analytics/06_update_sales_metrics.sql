CREATE OR REPLACE FUNCTION update_total_sales()
RETURNS VOID AS $$
DECLARE
    last_update TIMESTAMP;
    record_row RECORD;
BEGIN
    -- Получение последнего water_mark
    SELECT MAX(log_time) INTO last_update
    FROM dwh_metadata.logs
    WHERE process_name = 'update_total_sales' AND status = 'SUCCESS';

    FOR record_row IN
        SELECT id
        FROM dwh_dim_products
        WHERE id IN (
            SELECT DISTINCT product_id
            FROM dwh_fact_sales
            WHERE sale_date > COALESCE(last_update, '2000-01-01')
        )
    LOOP
        BEGIN
            UPDATE dwh_dim_products
            SET total_sales = (
                SELECT COALESCE(SUM(qty), 0)
                FROM dwh_fact_sales
                WHERE product_id = record_row.id
            )
            WHERE id = record_row.id;
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO dwh_metadata.logs (process_name, log_time, status, message)
            VALUES (
                'update_total_sales',
                CURRENT_TIMESTAMP,
                'ERROR',
                CONCAT('Error updating product ID ', record_row.id, ': ', SQLERRM)
            );
        END;
    END LOOP;


    INSERT INTO dwh_metadata.logs (process_name, log_time, status, message)
    VALUES (
        'update_total_sales',
        CURRENT_TIMESTAMP,
        'SUCCESS',
        'Обновление выполнено для изменённых продуктов.'
    );
END;
$$ LANGUAGE plpgsql;


SELECT update_total_sales();
