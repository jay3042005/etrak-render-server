-- eTrakYan Database Setup Script
-- Run this on your MySQL server to set up the collection_records table and fix schema mismatches
-- Safe to run multiple times - uses IF NOT EXISTS checks

USE railway;

-- =============================================
-- COLLECTION RECORDS TABLE (Driver Routes)
-- =============================================
CREATE TABLE IF NOT EXISTS collection_records (
    id VARCHAR(36) PRIMARY KEY,
    driver_id VARCHAR(36),
    schedule_id VARCHAR(36),
    barangay VARCHAR(100),
    waste_type VARCHAR(50),
    time_slot VARCHAR(50),
    date DATE,
    status VARCHAR(20) DEFAULT 'pending',
    waste_amount_kg DECIMAL(10,2) DEFAULT 0,
    biodegradable_kg DECIMAL(10,2) DEFAULT 0,
    non_biodegradable_kg DECIMAL(10,2) DEFAULT 0,
    recyclable_kg DECIMAL(10,2) DEFAULT 0,
    special_waste_kg DECIMAL(10,2) DEFAULT 0,
    notes TEXT,
    collected_at TIMESTAMP NULL,
    skipped_at TIMESTAMP NULL,
    skip_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_driver_date (driver_id, date),
    INDEX idx_schedule_date (schedule_id, date),
    INDEX idx_status (status)
);

-- =============================================
-- FIX SCHEDULES TABLE - Add missing columns
-- =============================================
-- Check if day_of_week column exists, if not add it
-- Note: MySQL doesn't have IF NOT EXISTS for columns, so we use a workaround

-- Add day_of_week column if it doesn't exist (for compatibility)
-- This is a workaround since MySQL doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN
-- We'll create a procedure to handle this safely

DELIMITER //

DROP PROCEDURE IF EXISTS add_column_if_not_exists//

CREATE PROCEDURE add_column_if_not_exists()
BEGIN
    -- Check if schedules table exists
    IF EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules') THEN
        -- Add day_of_week column if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules' AND COLUMN_NAME = 'day_of_week') THEN
            ALTER TABLE schedules ADD COLUMN day_of_week INT COMMENT '0=Monday, 6=Sunday';
        END IF;
        
        -- Add collection_type column if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules' AND COLUMN_NAME = 'collection_type') THEN
            ALTER TABLE schedules ADD COLUMN collection_type VARCHAR(50) DEFAULT 'all' COMMENT 'biodegradable, non_biodegradable, residual, all';
        END IF;
        
        -- Add start_time column if not exists (as VARCHAR for string format)
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules' AND COLUMN_NAME = 'start_time') THEN
            ALTER TABLE schedules ADD COLUMN start_time VARCHAR(20);
        END IF;
        
        -- Add end_time column if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules' AND COLUMN_NAME = 'end_time') THEN
            ALTER TABLE schedules ADD COLUMN end_time VARCHAR(20);
        END IF;
        
        -- Add driver_id column if not exists (VARCHAR for UUID compatibility)
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules' AND COLUMN_NAME = 'driver_id') THEN
            ALTER TABLE schedules ADD COLUMN driver_id VARCHAR(36);
        END IF;
        
        -- Add is_active column if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schedules' AND COLUMN_NAME = 'is_active') THEN
            ALTER TABLE schedules ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
        END IF;
    END IF;
END//

DELIMITER ;

-- Run the procedure to add missing columns
CALL add_column_if_not_exists();

-- Clean up procedure
DROP PROCEDURE IF EXISTS add_column_if_not_exists;

-- =============================================
-- FIX USERS TABLE - Add missing columns
-- =============================================
DELIMITER //

DROP PROCEDURE IF EXISTS add_user_columns//

CREATE PROCEDURE add_user_columns()
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users') THEN
        -- Add total_points if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'total_points') THEN
            ALTER TABLE users ADD COLUMN total_points INT DEFAULT 0;
        END IF;
        
        -- Add lifetime_points if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'lifetime_points') THEN
            ALTER TABLE users ADD COLUMN lifetime_points INT DEFAULT 0;
        END IF;
        
        -- Add reports_submitted if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'reports_submitted') THEN
            ALTER TABLE users ADD COLUMN reports_submitted INT DEFAULT 0;
        END IF;
        
        -- Add is_active if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'is_active') THEN
            ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
        END IF;
        
        -- Add assigned_truck_plate if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'assigned_truck_plate') THEN
            ALTER TABLE users ADD COLUMN assigned_truck_plate VARCHAR(20);
        END IF;
        
        -- Add profile_image_url if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'profile_image_url') THEN
            ALTER TABLE users ADD COLUMN profile_image_url VARCHAR(500);
        END IF;
    END IF;
END//

DELIMITER ;

CALL add_user_columns();
DROP PROCEDURE IF EXISTS add_user_columns;

-- =============================================
-- TRUCK LOCATIONS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS truck_locations (
    id VARCHAR(36) PRIMARY KEY,
    truck_id VARCHAR(36) NOT NULL,
    driver_name VARCHAR(255),
    plate_number VARCHAR(20),
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    current_barangay VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active' COMMENT 'active, offline',
    speed DECIMAL(6, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- The driver's duty intent, separate from `status`.
    --
    -- `status` is liveness: the sweeper rewrites it to 'offline' when
    -- last_updated goes stale. It therefore cannot also record whether the
    -- driver flipped the duty toggle, or the sweeper would end a shift the
    -- driver is still on. Handlers write this column, and without it the
    -- sweeper raised (1054, "Unknown column 'is_on_duty' in 'WHERE'")
    -- every tick, filling the log and never marking trucks offline.
    is_on_duty BOOLEAN DEFAULT FALSE,
    INDEX idx_truck_id (truck_id),
    INDEX idx_status (status)
);

-- =============================================
-- REWARDS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS rewards (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    points_required INT NOT NULL,
    image_url VARCHAR(500),
    available_quantity INT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_active (is_active)
);

-- =============================================
-- REDEMPTIONS TABLE (User Rewards)
-- =============================================
CREATE TABLE IF NOT EXISTS redemptions (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    reward_id VARCHAR(36) NOT NULL,
    reward_name VARCHAR(100),
    points_spent INT NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' COMMENT 'pending, approved, claimed, rejected',
    redeemed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_reward_id (reward_id),
    INDEX idx_status (status)
);

-- =============================================
-- SURVEYS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS surveys (
    id VARCHAR(36) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    questions JSON,
    target_barangay VARCHAR(100),
    created_by VARCHAR(36),
    is_active BOOLEAN DEFAULT TRUE,
    starts_at TIMESTAMP NULL,
    ends_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_active (is_active),
    INDEX idx_barangay (target_barangay)
);

-- =============================================
-- SURVEY RESPONSES TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS survey_responses (
    id VARCHAR(36) PRIMARY KEY,
    survey_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(36) NOT NULL,
    responses JSON NOT NULL,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_survey_id (survey_id),
    INDEX idx_user_id (user_id)
);

-- =============================================
-- REPORTS TABLE - Add missing columns
-- =============================================
DELIMITER //

DROP PROCEDURE IF EXISTS fix_reports_table//

CREATE PROCEDURE fix_reports_table()
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reports') THEN
        -- Add points_awarded if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reports' AND COLUMN_NAME = 'points_awarded') THEN
            ALTER TABLE reports ADD COLUMN points_awarded INT DEFAULT 0;
        END IF;
        
        -- Add image_url if not exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reports' AND COLUMN_NAME = 'image_url') THEN
            ALTER TABLE reports ADD COLUMN image_url VARCHAR(500);
        END IF;
    END IF;
END//

DELIMITER ;

CALL fix_reports_table();
DROP PROCEDURE IF EXISTS fix_reports_table;

-- =============================================
-- NOTIFICATIONS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS notifications (
    id VARCHAR(36) PRIMARY KEY,
    -- NULL for broadcast rows (announcements / schedule reminders); the audience
    -- is resolved at read time from target_scope/target_role/target_barangay.
    user_id VARCHAR(36) NULL,
    title VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    type VARCHAR(20) DEFAULT 'system' COMMENT 'collection, news, report, reward, system',
    is_read BOOLEAN DEFAULT FALSE,
    -- Broadcast targeting. These MUST be declared here: the API creates an index
    -- on notifications(target_scope) right after setup, and on a fresh database
    -- that failed with "Key column 'target_scope' doesn't exist in table".
    target_scope VARCHAR(20) DEFAULT NULL,
    target_barangay VARCHAR(100) DEFAULT NULL,
    target_role VARCHAR(20) DEFAULT NULL,
    schedule_id VARCHAR(36) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_is_read (is_read),
    INDEX idx_target_scope (target_scope)
);

-- =============================================
-- BARANGAYS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS barangays (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    district VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    population INT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- Insert default barangays if empty
-- =============================================
INSERT IGNORE INTO barangays (id, name, district) VALUES
(UUID(), 'General Santos City', 'Central'),
(UUID(), 'Dadiangas', 'Central'),
(UUID(), 'Labangal', 'Central'),
(UUID(), 'San Jose', 'Central'),
(UUID(), 'Koronadal', 'South'),
(UUID(), 'Mabuhay', 'South'),
(UUID(), 'Buayan', 'South'),
(UUID(), 'Bula', 'North'),
(UUID(), 'Calumpang', 'North'),
(UUID(), 'Siguel', 'North'),
(UUID(), 'Apopong', 'East'),
(UUID(), 'Batomelong', 'North')
ON DUPLICATE KEY UPDATE name = name;

-- =============================================
-- Insert sample rewards if empty
-- =============================================
INSERT IGNORE INTO rewards (id, name, description, points_required) VALUES
(UUID(), 'Eco Bag', 'Reusable eco-friendly bag', 50),
(UUID(), 'Tumbler', 'Bamboo tumbler', 100),
(UUID(), 'Umbrella', 'Eco-themed umbrella', 200),
(UUID(), 'Plant Pot', 'Decorative plant pot', 150),
(UUID()->'Certificate', 'Certificate of Appreciation', 300)
ON DUPLICATE KEY UPDATE name = name;

-- =============================================
-- PERFORMANCE INDEXES (safe to run multiple times)
-- =============================================
DELIMITER //

DROP PROCEDURE IF EXISTS add_perf_indexes//

CREATE PROCEDURE add_perf_indexes()
BEGIN
    -- reports: speed up GROUP BY status + month filter + barangay
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reports' AND INDEX_NAME = 'idx_reports_status_created') THEN
        CREATE INDEX idx_reports_status_created ON reports(status, created_at);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reports' AND INDEX_NAME = 'idx_reports_barangay') THEN
        CREATE INDEX idx_reports_barangay ON reports(barangay);
    END IF;

    -- collection_records: speed up waste_by_type + collections_this_month
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'collection_records' AND INDEX_NAME = 'idx_cr_status_collected') THEN
        CREATE INDEX idx_cr_status_collected ON collection_records(status, collected_at);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'collection_records' AND INDEX_NAME = 'idx_cr_waste_type') THEN
        CREATE INDEX idx_cr_waste_type ON collection_records(waste_type);
    END IF;

    -- truck_locations: unique index on truck_id for proper upsert
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'truck_locations' AND INDEX_NAME = 'idx_truck_id_unique') THEN
        CREATE UNIQUE INDEX idx_truck_id_unique ON truck_locations(truck_id);
    END IF;

    -- notifications: speed up get_notifications sort
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notifications' AND INDEX_NAME = 'idx_notif_user_created') THEN
        CREATE INDEX idx_notif_user_created ON notifications(user_id, created_at);
    END IF;
END//

DELIMITER ;

CALL add_perf_indexes();
DROP PROCEDURE IF EXISTS add_perf_indexes;

-- =============================================
-- Verify Setup
-- =============================================
SELECT 'Database setup completed!' as status;

-- List all tables
SHOW TABLES;

