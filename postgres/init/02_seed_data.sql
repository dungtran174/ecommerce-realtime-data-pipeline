-- =============================================================================
-- Seed Data for Lookup Tables
-- These are static/semi-static reference tables used across the system
-- =============================================================================

-- =============================================
-- Roles
-- =============================================
INSERT INTO roles (role_name, role_title) VALUES
    ('admin', 'Administrator'),
    ('staff', 'Staff'),
    ('user',  'Customer')
ON CONFLICT (role_name) DO NOTHING;

-- =============================================
-- Order Status (5 statuses reflecting the order lifecycle)
-- =============================================
INSERT INTO order_status (order_status_name) VALUES
    ('pending'),
    ('confirmed'),
    ('shipping'),
    ('delivered'),
    ('cancelled')
ON CONFLICT (order_status_name) DO NOTHING;

-- =============================================
-- Payment Status
-- =============================================
INSERT INTO payment_status (payment_status_name) VALUES
    ('pending'),
    ('completed'),
    ('failed'),
    ('refunded')
ON CONFLICT (payment_status_name) DO NOTHING;

-- =============================================
-- Shipping Status
-- =============================================
INSERT INTO shipping_status (shipping_status_name) VALUES
    ('pending'),
    ('picked_up'),
    ('in_transit'),
    ('delivered'),
    ('returned')
ON CONFLICT (shipping_status_name) DO NOTHING;

-- =============================================
-- Payment Methods
-- =============================================
INSERT INTO payment_methods (payment_method_name) VALUES
    ('COD'),
    ('Credit Card'),
    ('Momo'),
    ('ZaloPay'),
    ('Bank Transfer')
ON CONFLICT (payment_method_name) DO NOTHING;

-- =============================================
-- Shipping Methods
-- =============================================
INSERT INTO shipping_methods (shipping_method_name) VALUES
    ('Standard'),
    ('Express'),
    ('Same Day')
ON CONFLICT (shipping_method_name) DO NOTHING;
