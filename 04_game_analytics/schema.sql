-- ============================================================================
-- Q1: Data Model Design - Star Schema for Mobile Game Analytics
-- ============================================================================
-- Game: Crystal Quest
-- Business Scenario: Analytics for play sessions and in-app purchases
-- 
-- Key Design Decisions:
-- 1. Separate fact tables for sessions and purchases (different grain)
-- 2. SCD Type 2 for dim_user to track device changes over time
-- 3. Snowflake schema elements (dim_device) to reduce redundancy
-- 4. All amounts normalized to USD for global reporting
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension: Games
CREATE TABLE dim_game (
    game_sk BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(100) UNIQUE NOT NULL,
    game_name VARCHAR(255) NOT NULL,
    genre VARCHAR(50),
    developer VARCHAR(255),
    release_date DATE,
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dimension: Items (IAP catalog)
CREATE TABLE dim_item (
    item_sk BIGSERIAL PRIMARY KEY,
    item_id VARCHAR(255) UNIQUE NOT NULL,
    item_name VARCHAR(255) NOT NULL,
    item_type VARCHAR(50) NOT NULL, -- virtual_currency, subscription, bundle, power_up
    base_price_usd DECIMAL(10, 2),
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dimension: Device (Snowflake schema element)
CREATE TABLE dim_device (
    device_sk BIGSERIAL PRIMARY KEY,
    device_model VARCHAR(100) NOT NULL,
    os VARCHAR(20) NOT NULL, -- ios, android
    os_version VARCHAR(20) NOT NULL,
    form_factor VARCHAR(20), -- phone, tablet, unknown
    UNIQUE(device_model, os, os_version)
);

-- Dimension: User (SCD Type 2)
-- Critical: Tracks user profile changes over time (e.g., device upgrades)
CREATE TABLE dim_user (
    user_sk BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL, -- Natural key
    device_sk BIGINT REFERENCES dim_device(device_sk),
    country_code VARCHAR(3),
    first_seen_date DATE NOT NULL,
    
    -- SCD Type 2 fields
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP, -- NULL = current record
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(user_id, effective_start_date)
);

-- Index for fast lookups of current user records
CREATE INDEX idx_dim_user_current ON dim_user(user_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_user_effective_dates ON dim_user(effective_start_date, effective_end_date);

-- Dimension: Date (for time-based analysis)
CREATE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_num INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Fact Table: Sessions
-- Grain: One row per unique session
-- Purpose: Analyze user engagement, session duration, retention
CREATE TABLE fact_session (
    session_sk BIGSERIAL PRIMARY KEY,
    
    -- Foreign Keys (Star Schema)
    user_sk BIGINT NOT NULL REFERENCES dim_user(user_sk),
    game_sk BIGINT NOT NULL REFERENCES dim_game(game_sk),
    device_sk BIGINT NOT NULL REFERENCES dim_device(device_sk),
    start_date_sk INTEGER NOT NULL REFERENCES dim_date(date_sk),
    
    -- Degenerate Dimension (no separate table needed)
    session_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Measures (Additive)
    duration_seconds INTEGER,
    levels_started INTEGER DEFAULT 0,
    levels_completed INTEGER DEFAULT 0,
    levels_won INTEGER DEFAULT 0,
    levels_lost INTEGER DEFAULT 0,
    max_level_reached INTEGER,
    ads_watched INTEGER DEFAULT 0,
    ad_revenue_usd DECIMAL(10, 4) DEFAULT 0.00, -- Estimated from ad impressions
    
    -- Timestamps (for session windowing)
    session_start_ts TIMESTAMP NOT NULL,
    session_end_ts TIMESTAMP,
    
    -- Semi-Additive (snapshot at session time)
    country_code VARCHAR(3), -- Snapshot: user could travel
    network_type VARCHAR(10), -- wifi, 4g, 5g
    app_version VARCHAR(20),
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_fact_session_user ON fact_session(user_sk);
CREATE INDEX idx_fact_session_game ON fact_session(game_sk);
CREATE INDEX idx_fact_session_start_date ON fact_session(start_date_sk);
CREATE INDEX idx_fact_session_timestamps ON fact_session(session_start_ts, session_end_ts);

-- Fact Table: Purchases
-- Grain: One row per transaction (including refunds as separate rows)
-- Purpose: Revenue analysis, conversion rates, refund tracking
CREATE TABLE fact_purchase (
    purchase_sk BIGSERIAL PRIMARY KEY,
    
    -- Foreign Keys
    user_sk BIGINT NOT NULL REFERENCES dim_user(user_sk),
    game_sk BIGINT NOT NULL REFERENCES dim_game(game_sk),
    item_sk BIGINT NOT NULL REFERENCES dim_item(item_sk),
    device_sk BIGINT NOT NULL REFERENCES dim_device(device_sk),
    purchase_date_sk INTEGER NOT NULL REFERENCES dim_date(date_sk),
    
    -- Optional: Link to session (if purchase happened during gameplay)
    session_sk BIGINT REFERENCES fact_session(session_sk),
    
    -- Degenerate Dimensions
    purchase_id VARCHAR(100) NOT NULL, -- NOT UNIQUE! Refunds reuse same purchase_id
    transaction_ts TIMESTAMP NOT NULL,
    
    -- Measures (Additive for aggregation, but watch for refunds!)
    quantity INTEGER NOT NULL DEFAULT 1,
    amount_local DECIMAL(10, 2) NOT NULL, -- Original currency
    amount_usd DECIMAL(10, 2) NOT NULL, -- Normalized to USD for global reporting
    original_price_local DECIMAL(10, 2), -- For discount analysis
    original_price_usd DECIMAL(10, 2),
    
    -- Flags (Semi-Additive - must be handled in queries)
    is_refund BOOLEAN NOT NULL DEFAULT FALSE,
    is_sandbox BOOLEAN NOT NULL DEFAULT FALSE, -- Test purchases
    
    -- Attributes
    currency_code VARCHAR(3) NOT NULL,
    platform VARCHAR(50) NOT NULL, -- apple_app_store, google_play
    country_code VARCHAR(3), -- Snapshot
    refund_reason VARCHAR(100), -- Only for refunds
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_fact_purchase_user ON fact_purchase(user_sk);
CREATE INDEX idx_fact_purchase_item ON fact_purchase(item_sk);
CREATE INDEX idx_fact_purchase_date ON fact_purchase(purchase_date_sk);
CREATE INDEX idx_fact_purchase_transaction_ts ON fact_purchase(transaction_ts);
CREATE INDEX idx_fact_purchase_session ON fact_purchase(session_sk) WHERE session_sk IS NOT NULL;
CREATE INDEX idx_fact_purchase_purchase_id ON fact_purchase(purchase_id); -- For refund lookups

-- Composite index for revenue queries (excludes refunds and sandbox)
CREATE INDEX idx_fact_purchase_revenue ON fact_purchase(is_refund, is_sandbox, purchase_date_sk) 
    WHERE is_refund = FALSE AND is_sandbox = FALSE;

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Current User Profile (latest SCD Type 2 record)
CREATE VIEW v_user_current AS
SELECT 
    user_sk,
    user_id,
    device_sk,
    country_code,
    first_seen_date,
    effective_start_date
FROM dim_user
WHERE is_current = TRUE;

-- View: Net Revenue (purchases minus refunds)
CREATE VIEW v_net_revenue AS
SELECT 
    purchase_date_sk,
    user_sk,
    item_sk,
    SUM(CASE WHEN is_refund = FALSE THEN amount_usd ELSE 0 END) AS gross_revenue_usd,
    SUM(CASE WHEN is_refund = TRUE THEN amount_usd ELSE 0 END) AS refund_amount_usd,
    SUM(CASE WHEN is_refund = FALSE THEN amount_usd ELSE -amount_usd END) AS net_revenue_usd,
    COUNT(CASE WHEN is_refund = FALSE THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN is_refund = TRUE THEN 1 END) AS refund_count
FROM fact_purchase
WHERE is_sandbox = FALSE
GROUP BY purchase_date_sk, user_sk, item_sk;

-- View: Session Summary with Revenue
-- Purpose: Join sessions with their purchases for "revenue per session" analysis
CREATE VIEW v_session_with_revenue AS
SELECT 
    s.session_sk,
    s.session_id,
    s.user_sk,
    s.session_start_ts,
    s.duration_seconds,
    s.levels_completed,
    COALESCE(SUM(CASE WHEN p.is_refund = FALSE THEN p.amount_usd ELSE 0 END), 0) AS session_revenue_usd,
    COUNT(CASE WHEN p.is_refund = FALSE THEN 1 END) AS purchases_in_session
FROM fact_session s
LEFT JOIN fact_purchase p ON p.session_sk = s.session_sk
GROUP BY s.session_sk, s.session_id, s.user_sk, s.session_start_ts, s.duration_seconds, s.levels_completed;

-- ============================================================================
-- SAMPLE QUERIES (Q2 Preview)
-- ============================================================================

-- Q2.1: Daily Revenue by Country (excluding refunds)
-- Expected output: Which countries generate most revenue?
/*
SELECT 
    d.date_actual,
    fp.country_code,
    COUNT(DISTINCT fp.user_sk) AS paying_users,
    COUNT(*) AS transactions,
    SUM(fp.amount_usd) AS gross_revenue_usd,
    SUM(CASE WHEN fp.is_refund THEN fp.amount_usd ELSE 0 END) AS refund_amount_usd,
    SUM(CASE WHEN NOT fp.is_refund THEN fp.amount_usd ELSE -fp.amount_usd END) AS net_revenue_usd
FROM fact_purchase fp
JOIN dim_date d ON fp.purchase_date_sk = d.date_sk
WHERE fp.is_sandbox = FALSE
GROUP BY d.date_actual, fp.country_code
ORDER BY d.date_actual DESC, net_revenue_usd DESC;
*/

-- Q2.2: User Retention (sessions per user over time)
/*
WITH user_sessions AS (
    SELECT 
        user_sk,
        COUNT(*) AS total_sessions,
        MIN(session_start_ts) AS first_session,
        MAX(session_start_ts) AS last_session,
        DATE(MAX(session_start_ts)) - DATE(MIN(session_start_ts)) AS days_active
    FROM fact_session
    GROUP BY user_sk
)
SELECT 
    days_active,
    COUNT(*) AS users,
    AVG(total_sessions) AS avg_sessions_per_user
FROM user_sessions
GROUP BY days_active
ORDER BY days_active;
*/

-- Q2.3: Conversion Rate (% of sessions with purchases)
/*
SELECT 
    d.date_actual,
    COUNT(DISTINCT s.session_sk) AS total_sessions,
    COUNT(DISTINCT s.session_sk) FILTER (WHERE p.purchase_sk IS NOT NULL) AS sessions_with_purchase,
    ROUND(100.0 * COUNT(DISTINCT s.session_sk) FILTER (WHERE p.purchase_sk IS NOT NULL) / 
          NULLIF(COUNT(DISTINCT s.session_sk), 0), 2) AS conversion_rate_pct
FROM fact_session s
JOIN dim_date d ON s.start_date_sk = d.date_sk
LEFT JOIN fact_purchase p ON p.session_sk = s.session_sk AND p.is_refund = FALSE
GROUP BY d.date_actual
ORDER BY d.date_actual DESC;
*/

-- ============================================================================
-- SEED DATA (for testing)
-- ============================================================================

-- Insert game
INSERT INTO dim_game (game_id, game_name, genre, developer, release_date)
VALUES ('crystal_quest', 'Crystal Quest', 'Puzzle', 'MindGames Studios', '2024-01-15');

-- Insert sample items
INSERT INTO dim_item (item_id, item_name, item_type, base_price_usd) VALUES
('com.crystal_quest.coin_pack_small', 'Small Coin Pack', 'virtual_currency', 0.99),
('com.crystal_quest.coin_pack_medium', 'Medium Coin Pack', 'virtual_currency', 4.99),
('com.crystal_quest.coin_pack_large', 'Large Coin Pack', 'virtual_currency', 9.99),
('com.crystal_quest.coin_pack_mega', 'Mega Coin Pack', 'virtual_currency', 19.99),
('com.crystal_quest.premium_monthly', 'Premium Monthly', 'subscription', 9.99),
('com.crystal_quest.premium_yearly', 'Premium Yearly', 'subscription', 99.99),
('com.crystal_quest.starter_pack', 'Starter Pack', 'bundle', 4.99),
('com.crystal_quest.ultimate_bundle', 'Ultimate Bundle', 'bundle', 29.99),
('com.crystal_quest.hint_3pack', 'Hint 3-Pack', 'power_up', 1.99),
('com.crystal_quest.timer_boost_5', 'Timer Boost x5', 'power_up', 2.99);

-- Insert sample devices
INSERT INTO dim_device (device_model, os, os_version, form_factor) VALUES
('iPhone15,2', 'ios', '17.4', 'phone'),
('iPhone14,3', 'ios', '17.2', 'phone'),
('iPhone13,4', 'ios', '16.5', 'phone'),
('SM-G998B', 'android', '14', 'phone'),
('Pixel 8 Pro', 'android', '14', 'phone'),
('OnePlus 11', 'android', '13', 'phone');

COMMENT ON TABLE dim_user IS 'User dimension with SCD Type 2 to track profile changes (e.g., device upgrades)';
COMMENT ON TABLE fact_session IS 'Grain: One row per gameplay session. Tracks engagement metrics.';
COMMENT ON TABLE fact_purchase IS 'Grain: One row per transaction. Refunds are separate rows with same purchase_id.';
COMMENT ON COLUMN fact_purchase.is_refund IS 'CRITICAL: Always filter this in revenue queries. Refunds have same purchase_id as original.';
