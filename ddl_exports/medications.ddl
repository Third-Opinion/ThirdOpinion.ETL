CREATE TABLE public.medications (
    medication_id character varying(255) NOT NULL,
    resource_type character varying(50),
    code_text character varying(500),
    status character varying(50),
    meta_version_id character varying(50),
    meta_last_updated timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);