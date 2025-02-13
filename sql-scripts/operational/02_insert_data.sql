-- Наполнение таблиц данными
INSERT INTO customers (name, country)
SELECT 
    FORMAT('Customer %s', i), 
    (ARRAY[
        'USA', 'Canada', 'Germany', 'France', 'Italy', 'Spain', 
        'UK', 'Australia', 'Japan', 'Brazil', 'Israel', 'Georgia'
    ])[FLOOR(RANDOM() * 12 + 1)] -- Случайная страна из массива (индексы 1-12)
FROM generate_series(1, 50) AS s(i);

INSERT INTO products (name, groupname)
SELECT 
    CONCAT('Product ', i), 
    category
FROM generate_series(1, 10) AS s(i),
     UNNEST(ARRAY[
        'Electronics', 'Furniture', 'Clothing', 'Books', 
        'Food', 'Toys', 'Appliances', 'Tools', 'Automotive', 'Sports
        ']) AS category
LIMIT 10;

INSERT INTO sales (customer_id, product_id, qty, sale_date)
SELECT 
    c.id AS customer_id,
    p.id AS product_id,
    FLOOR(RANDOM() * 5 + 1)::INT AS qty, 
    NOW() - (INTERVAL '1 day' * FLOOR(RANDOM() * 60)::INT) AS sale_date -- Дата в пределах 60 дней
FROM 
    (SELECT id FROM customers ORDER BY RANDOM() LIMIT 1000) c, -- Случайные клиенты 
    (SELECT id FROM products ORDER BY RANDOM() LIMIT 1000) p -- Случайные продукты 
LIMIT 1000;
