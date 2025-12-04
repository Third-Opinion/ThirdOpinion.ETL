-- Drop all tables in dependency order (dependent tables first, then base tables)
-- Execute this before running migrations fresh

-- Drop junction/detail tables first (depend on main tables)
DROP TABLE IF EXISTS public.observation_performers CASCADE;
DROP TABLE IF EXISTS public.observation_reference_ranges CASCADE;
DROP TABLE IF EXISTS public.observation_interpretations CASCADE;
DROP TABLE IF EXISTS public.observation_derived_from CASCADE;
DROP TABLE IF EXISTS public.observation_notes CASCADE;
DROP TABLE IF EXISTS public.observation_components CASCADE;
DROP TABLE IF EXISTS public.observation_members CASCADE;
DROP TABLE IF EXISTS public.observation_body_sites CASCADE;
DROP TABLE IF EXISTS public.observation_categories CASCADE;
DROP TABLE IF EXISTS public.observation_codes CASCADE;

DROP TABLE IF EXISTS public.condition_evidence CASCADE;
DROP TABLE IF EXISTS public.condition_extensions CASCADE;
DROP TABLE IF EXISTS public.condition_notes CASCADE;
DROP TABLE IF EXISTS public.condition_stage_summaries CASCADE;
DROP TABLE IF EXISTS public.condition_stage_types CASCADE;
DROP TABLE IF EXISTS public.condition_stages CASCADE;
DROP TABLE IF EXISTS public.condition_body_sites CASCADE;
DROP TABLE IF EXISTS public.condition_categories CASCADE;
DROP TABLE IF EXISTS public.condition_codes CASCADE;

-- Drop main tables
DROP TABLE IF EXISTS public.observations CASCADE;
DROP TABLE IF EXISTS public.conditions CASCADE;

-- Drop reference tables
DROP TABLE IF EXISTS public.interpretations CASCADE;
DROP TABLE IF EXISTS public.body_sites CASCADE;
DROP TABLE IF EXISTS public.categories CASCADE;
DROP TABLE IF EXISTS public.codes CASCADE;

-- Drop migration tracking table (will be recreated by migrations)
DROP TABLE IF EXISTS public.schema_migrations CASCADE;

