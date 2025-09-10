CREATE TABLE public.medication_requests (
    medication_request_id character varying(255) NOT NULL,
    patient_id character varying(255) NOT NULL,
    encounter_id character varying(255),
    medication_id character varying(255),
    medication_display character varying(500),
    status character varying(50),
    intent character varying(50),
    reported_boolean boolean,
    authored_on timestamp without time zone,
    meta_version_id character varying(50),
    meta_last_updated timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);