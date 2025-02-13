-- Сортировка новосозданной таблицы sales по времени 
CREATE TEMP TABLE sales_temp AS
SELECT *
FROM sales
ORDER BY sale_date;

TRUNCATE TABLE sales RESTART IDENTITY;

INSERT INTO sales (customer_id, product_id, qty, sale_date)
SELECT customer_id, product_id, qty, sale_date
FROM sales_temp;

DROP TABLE sales_temp;