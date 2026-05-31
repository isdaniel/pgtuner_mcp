INSERT INTO users (email)
SELECT 'user_' || g || '@example.com' FROM generate_series(1, 5000) g;

INSERT INTO products (sku, price)
SELECT 'SKU-' || g, (random()*100)::numeric(10,2) FROM generate_series(1, 500) g;

INSERT INTO orders (user_id, product_id, amount)
SELECT (random()*4999+1)::bigint, (random()*499+1)::bigint, (random()*100)::numeric(10,2)
FROM generate_series(1, 20000);

INSERT INTO docs (body)
SELECT repeat('lorem ipsum dolor sit amet ', 2000) FROM generate_series(1, 100);

INSERT INTO audit (event, payload)
SELECT 'evt_' || g, repeat('x', 200) FROM generate_series(1, 10000);
DELETE FROM audit WHERE id % 2 = 0;

ANALYZE;
