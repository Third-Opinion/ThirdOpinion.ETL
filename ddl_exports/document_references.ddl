CREATE TABLE public.document_references (
    document_reference_id character varying(255) NOT NULL,
    patient_id character varying(255) NOT NULL,
    status character varying(50),
    type_code character varying(50),
    type_system character varying(255),
    type_display character varying(500),
    date timestamp without time zone,
    custodian_id character varying(255),
    description character varying(65535),
    context_period_start timestamp without time zone,
    context_period_end timestamp without time zone,
    meta_version_id character varying(50),
    meta_last_updated timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);