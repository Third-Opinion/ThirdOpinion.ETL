CREATE TABLE IF NOT EXISTS public.observation_codes (
    observation_id VARCHAR(255),
    code_code VARCHAR(50),
    code_system VARCHAR(255),
    code_display VARCHAR(255),
    code_text VARCHAR(500)
) SORTKEY (observation_id, code_code);