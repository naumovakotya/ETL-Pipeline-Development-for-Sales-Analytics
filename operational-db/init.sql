DROP TABLE IF EXISTS sales, customers, products CASCADE;

-- Корректировка создания и наполнения таблиц
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL DEFAULT 'Unknown'
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    groupname VARCHAR(100) NOT NULL DEFAULT 'Unknown'
);

CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    qty INT NOT NULL CHECK (qty > 0),
    sale_date TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (customer_id) REFERENCES customers (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
);

-- Наполнение таблиц данными
INSERT INTO customers (name, country)
SELECT 
    CONCAT('Customer ', i), 
    COALESCE((ARRAY['USA', 'Canada', 'Germany', 'France', 'Italy', 'Spain', 'UK', 'Australia', 'Japan', 'Brazil'])[FLOOR(RANDOM() * 10)], 'Unknown')
FROM generate_series(1, 50) AS s(i);

INSERT INTO products (name, groupname)
SELECT 
    CONCAT('Product ', i),
    COALESCE((ARRAY['Electronics', 'Furniture', 'Clothing', 'Books', 'Food', 'Toys', 'Appliances', 'Tools', 'Automotive', 'Sports'])[FLOOR(RANDOM() * 10)], 'Unknown')
FROM generate_series(1, 10) AS s(i);


-- Наполнение таблицы продаж
INSERT INTO sales (customer_id, product_id, qty, sale_date)
SELECT 
    c.id AS customer_id,
    p.id AS product_id,
    FLOOR(RANDOM() * 5 + 1)::INT AS qty, -- Количество от 1 до 5
    NOW() - (INTERVAL '1 day' * FLOOR(RANDOM() * 60)::INT) AS sale_date -- Дата в пределах 60 дней
FROM 
    (SELECT id FROM customers ORDER BY RANDOM() LIMIT 100) c, -- Случайные клиенты
    (SELECT id FROM products ORDER BY RANDOM() LIMIT 100) p -- Случайные продукты
LIMIT 100;

-- Сортировка новосозданной таблицы sales по времени 
-- Создать временную таблицу для хранения отсортированных данных
CREATE TEMP TABLE sales_temp AS
SELECT *
FROM sales
ORDER BY sale_date;

-- Очистить таблицу sales
TRUNCATE TABLE sales RESTART IDENTITY;

-- Вставить отсортированные данные обратно в таблицу sales
INSERT INTO sales (customer_id, product_id, qty, sale_date)
SELECT customer_id, product_id, qty, sale_date
FROM sales_temp;

-- Удалить временную таблицу
DROP TABLE sales_temp;
