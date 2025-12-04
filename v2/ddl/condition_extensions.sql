-- Table: condition_extensions
-- Normalized table for condition extensions
-- Each condition can have multiple extensions (one row per extension)
-- Supports nested extensions and various value types
-- Updated: 2025-01-XX - v2 schema

CREATE TABLE IF NOT EXISTS public.condition_extensions (
    condition_id VARCHAR(255),
    extension_url VARCHAR(500),
    extension_type VARCHAR(50),
    value_type VARCHAR(50),
    value_string VARCHAR(MAX),
    value_datetime TIMESTAMP,
    value_reference VARCHAR(255),
    value_code VARCHAR(100),
    value_boolean BOOLEAN,
    value_decimal DECIMAL(18,6),
    value_integer INTEGER,
    parent_extension_url VARCHAR(500),
    extension_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
SORTKEY (condition_id, extension_url, extension_order);


