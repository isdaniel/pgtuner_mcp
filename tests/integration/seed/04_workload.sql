SELECT pg_stat_statements_reset();

SELECT count(*) FROM orders WHERE amount > 50;
SELECT count(*) FROM orders WHERE amount > 50;
SELECT count(*) FROM orders WHERE amount > 50;

SELECT u.email, count(o.id) FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.email LIMIT 10;
SELECT u.email, count(o.id) FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.email LIMIT 10;

SELECT * FROM users WHERE id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5;
SELECT * FROM users WHERE id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5;
SELECT * FROM users WHERE id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5;

SELECT count(*) FROM products;
SELECT count(*) FROM products;
SELECT count(*) FROM products;
