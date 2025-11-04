-- Managed Views Tracking Table
-- This table tracks all views managed by the redeploy_fhir_views.sh script
-- including their dependency levels and last update timestamps

CREATE TABLE IF NOT EXISTS public.managed_views (
    view_name VARCHAR(255) PRIMARY KEY,
    dependency_level INTEGER NOT NULL,
    view_type VARCHAR(50) NOT NULL, -- 'MATERIALIZED' or 'REGULAR'
    view_category VARCHAR(50), -- 'FACT', 'REPORTING', etc.
    sql_file_path VARCHAR(500),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_deployed TIMESTAMP,
    last_refreshed TIMESTAMP,
    row_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
)
DISTSTYLE ALL
SORTKEY (dependency_level, view_name);

-- Create index for faster lookups by dependency level
CREATE INDEX idx_managed_views_level ON public.managed_views(dependency_level);

-- Add comments for documentation
COMMENT ON TABLE public.managed_views IS 'Tracks all FHIR views managed by deployment scripts with metadata and deployment history';
COMMENT ON COLUMN public.managed_views.view_name IS 'Name of the view in the database';
COMMENT ON COLUMN public.managed_views.dependency_level IS 'Deployment dependency level (0-8), views at lower levels must be deployed first';
COMMENT ON COLUMN public.managed_views.view_type IS 'Type of view: MATERIALIZED or REGULAR';
COMMENT ON COLUMN public.managed_views.view_category IS 'Category: FACT, REPORTING, UNION, etc.';
COMMENT ON COLUMN public.managed_views.sql_file_path IS 'Relative path to SQL definition file';
COMMENT ON COLUMN public.managed_views.last_updated IS 'Timestamp of last record update';
COMMENT ON COLUMN public.managed_views.last_deployed IS 'Timestamp when view was last deployed/created';
COMMENT ON COLUMN public.managed_views.last_refreshed IS 'Timestamp when materialized view was last refreshed';
COMMENT ON COLUMN public.managed_views.row_count IS 'Number of rows in the view (for monitoring)';
COMMENT ON COLUMN public.managed_views.created_at IS 'Timestamp when this tracking record was created';
COMMENT ON COLUMN public.managed_views.notes IS 'Additional notes about the view';
