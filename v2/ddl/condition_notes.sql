-- Table: condition_notes
-- Normalized table for condition notes
-- Each condition can have multiple notes (one row per note)
-- Updated: 2025-01-XX - v2 schema

CREATE TABLE IF NOT EXISTS public.condition_notes (
    condition_id VARCHAR(255),
    meta_last_updated TIMESTAMP,
    note_text VARCHAR(MAX),
    note_author_reference VARCHAR(255),
    note_time TIMESTAMP
) 
SORTKEY (condition_id, note_time);


