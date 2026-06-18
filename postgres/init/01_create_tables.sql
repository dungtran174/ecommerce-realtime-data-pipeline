-- =============================================================================
-- E-Commerce OLTP Database Schema
-- PostgreSQL 15
-- =============================================================================

-- =============================================
-- 1. User & Role Management
-- =============================================

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    username    VARCHAR(100) NOT NULL UNIQUE,
    password    VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL UNIQUE,
    mobile      VARCHAR(20) UNIQUE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS roles (
    id          SERIAL PRIMARY KEY,
    role_name   VARCHAR(5) NOT NULL UNIQUE,
    role_title  VARCHAR(100) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS role_user (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    role_id     INTEGER NOT NULL REFERENCES roles(id),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================
-- 2. Geography (Regions, Provinces, Addresses)
-- =============================================

CREATE TABLE IF NOT EXISTS regions (
    id          SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS provinces (
    id              SERIAL PRIMARY KEY,
    province_name   VARCHAR(100) NOT NULL UNIQUE,
    region_id       INTEGER NOT NULL REFERENCES regions(id),
    latitude        NUMERIC,
    longitude       NUMERIC,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS addresses (
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(100),
    user_id         INTEGER NOT NULL REFERENCES users(id),
    province_id     INTEGER NOT NULL REFERENCES provinces(id),
    region_id       INTEGER NOT NULL REFERENCES regions(id),
    full_address    VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================
-- 3. Product Catalog
-- =============================================

-- Self-referencing: category_id points to parent category (NULL = root)
CREATE TABLE IF NOT EXISTS categories (
    id              SERIAL PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL,
    category_id     INTEGER REFERENCES categories(id),
    slug            VARCHAR(100) UNIQUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS brands (
    id          SERIAL PRIMARY KEY,
    brand_name  VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tags (
    id          SERIAL PRIMARY KEY,
    tag_name    VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id                  SERIAL PRIMARY KEY,
    product_name        VARCHAR(255) NOT NULL,
    category_id         INTEGER REFERENCES categories(id),
    brand_id            INTEGER REFERENCES brands(id),
    product_price       NUMERIC(15, 2) NOT NULL DEFAULT 0,
    unit_cost           NUMERIC(15, 2) NOT NULL DEFAULT 0,
    product_quantity    INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_tag (
    id          SERIAL PRIMARY KEY,
    product_id  INTEGER NOT NULL REFERENCES products(id),
    tag_id      INTEGER NOT NULL REFERENCES tags(id),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================
-- 4. Order Status & Payment/Shipping Methods
-- =============================================

CREATE TABLE IF NOT EXISTS order_status (
    id                  SERIAL PRIMARY KEY,
    order_status_name   VARCHAR(100) NOT NULL UNIQUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_status (
    id                      SERIAL PRIMARY KEY,
    payment_status_name     VARCHAR(100) NOT NULL UNIQUE,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shipping_status (
    id                      SERIAL PRIMARY KEY,
    shipping_status_name    VARCHAR(100) NOT NULL UNIQUE,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_methods (
    id                      SERIAL PRIMARY KEY,
    payment_method_name     VARCHAR(100) NOT NULL UNIQUE,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shipping_methods (
    id                      SERIAL PRIMARY KEY,
    shipping_method_name    VARCHAR(100) NOT NULL UNIQUE,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================
-- 5. Marketing & Discounts
-- =============================================

CREATE TABLE IF NOT EXISTS ads_campaigns (
    id              SERIAL PRIMARY KEY,
    campaign_title  VARCHAR(100) NOT NULL,
    started_at      TIMESTAMP,
    expired_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS discounts (
    id              SERIAL PRIMARY KEY,
    adscampaign_id  INTEGER REFERENCES ads_campaigns(id),
    type            VARCHAR(20) NOT NULL,         -- 'percent' or 'amount'
    value           NUMERIC(15, 2) NOT NULL,
    code            VARCHAR(100),
    started_at      TIMESTAMP,
    expired_at      TIMESTAMP
);

-- =============================================
-- 6. Orders & Transactions
-- =============================================

CREATE TABLE IF NOT EXISTS orders (
    id                  SERIAL PRIMARY KEY,
    user_id             INTEGER NOT NULL REFERENCES users(id),
    staff_id            INTEGER REFERENCES users(id),
    address_id          INTEGER REFERENCES addresses(id),
    order_amount        NUMERIC(15, 2) NOT NULL DEFAULT 0,
    discount_amount     NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_amount        NUMERIC(15, 2) NOT NULL DEFAULT 0,
    discount_id         INTEGER REFERENCES discounts(id),
    payment_method_id   INTEGER REFERENCES payment_methods(id),
    payment_status_id   INTEGER REFERENCES payment_status(id),
    order_status_id     INTEGER REFERENCES order_status(id),
    shipping_method_id  INTEGER REFERENCES shipping_methods(id),
    shipping_status_id  INTEGER REFERENCES shipping_status(id),
    shipped_at          TIMESTAMP,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orderdetails (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(id),
    product_id      INTEGER NOT NULL REFERENCES products(id),
    quantity        INTEGER NOT NULL DEFAULT 1,
    product_price   NUMERIC(15, 2) NOT NULL DEFAULT 0,
    product_tax     NUMERIC(15, 2) NOT NULL DEFAULT 0,
    subtotal_amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_status_history (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(id),
    order_status_id INTEGER NOT NULL REFERENCES order_status(id),
    staff_id        INTEGER REFERENCES users(id),
    comments        VARCHAR(500),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TYPE transaction_type AS ENUM ('payment', 'refund', 'adjustment', 'other');

CREATE TABLE IF NOT EXISTS transactions (
    id                  SERIAL PRIMARY KEY,
    order_id            INTEGER NOT NULL REFERENCES orders(id),
    transaction_type    transaction_type NOT NULL,
    status              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    description         TEXT
);

-- =============================================
-- Indexes for performance
-- =============================================

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_status ON orders(order_status_id);
CREATE INDEX idx_orderdetails_order_id ON orderdetails(order_id);
CREATE INDEX idx_orderdetails_product_id ON orderdetails(product_id);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_brand_id ON products(brand_id);
CREATE INDEX idx_addresses_user_id ON addresses(user_id);
CREATE INDEX idx_addresses_province_id ON addresses(province_id);
CREATE INDEX idx_transactions_order_id ON transactions(order_id);
