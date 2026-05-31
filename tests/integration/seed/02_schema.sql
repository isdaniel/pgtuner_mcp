DROP TABLE IF EXISTS orders, users, products, docs, audit CASCADE;

CREATE TABLE users (
  id bigserial PRIMARY KEY,
  email text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  active boolean NOT NULL DEFAULT true
);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_email_dup ON users(email);  -- duplicate for find_unused_indexes
CREATE INDEX idx_users_never_used ON users(active) WHERE active = false;

CREATE TABLE products (
  id bigserial PRIMARY KEY,
  sku text NOT NULL,
  price numeric(10,2) NOT NULL
);

CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  user_id bigint NOT NULL REFERENCES users(id),
  product_id bigint NOT NULL REFERENCES products(id),
  amount numeric(10,2) NOT NULL,
  placed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE docs (
  id bigserial PRIMARY KEY,
  body text
);

CREATE TABLE audit (
  id bigserial PRIMARY KEY,
  event text,
  payload text
);
