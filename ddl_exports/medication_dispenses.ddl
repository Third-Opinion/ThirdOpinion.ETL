CREATE TABLE public.medication_dispenses (
    medication_dispense_id character varying(255) NOT NULL,
    resource_type character varying(50),
    status character varying(50),
    patient_id character varying(255) NOT NULL,
    medication_id character varying(255),
    medication_display character varying(500),
    type_system character varying(255),
    type_code character varying(50),
    type_display character varying(255),
    quantity_value numeric(10,2),
    when_handed_over timestamp without time zone,
    meta_version_id character varying(50),
    meta_last_updated timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);