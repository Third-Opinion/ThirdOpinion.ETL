-- Table: schema_migrations
-- Purpose: Tracks applied database migrations for version control
-- Created: 2024-01-15

CREATE TABLE IF NOT EXISTS public.schema_migrations (
    migration_id VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT GETDATE(),
    applied_by VARCHAR(255),
    execution_time_seconds INTEGER,
    status VARCHAR(50) NOT NULL DEFAULT 'success',
    error_message TEXT,
    checksum VARCHAR(64),
    migration_file_path VARCHAR(500)
) DISTKEY (migration_id) SORTKEY (applied_at);

-- Add comments
COMMENT ON TABLE public.schema_migrations IS 'Tracks all applied database migrations with execution details';
COMMENT ON COLUMN public.schema_migrations.migration_id IS 'Unique identifier: YYYYMMDD_HHMMSS_description';
COMMENT ON COLUMN public.schema_migrations.applied_at IS 'Timestamp when migration was applied';
COMMENT ON COLUMN public.schema_migrations.applied_by IS 'AWS user/role that applied the migration';
COMMENT ON COLUMN public.schema_migrations.execution_time_seconds IS 'Time taken to execute migration in seconds';
COMMENT ON COLUMN public.schema_migrations.status IS 'Migration status: success, failed, rolled_back';
COMMENT ON COLUMN public.schema_migrations.error_message IS 'Error message if migration failed';
COMMENT ON COLUMN public.schema_migrations.checksum IS 'SHA256 hash of migration file for verification';
COMMENT ON COLUMN public.schema_migrations.migration_file_path IS 'Path to migration file relative to repo root';

-- Note: Redshift doesn't support traditional indexes
-- Performance is optimized through DISTKEY (migration_id) and SORTKEY (applied_at) in the table definition above

