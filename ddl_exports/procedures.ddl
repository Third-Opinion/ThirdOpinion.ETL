CREATE TABLE public.procedures (
    procedure_id character varying(255) NOT NULL,
    resource_type character varying(50),
    status character varying(50),
    patient_id character varying(255) NOT NULL,
    code_text character varying(500),
    performed_date_time timestamp without time zone,
    meta_version_id character varying(50),
    meta_last_updated timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);