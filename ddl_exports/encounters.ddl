CREATE TABLE public.encounters (
    encounter_id character varying(255),
    patient_id character varying(255),
    status character varying(50),
    resourcetype character varying(50),
    class_code character varying(10),
    class_display character varying(255),
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    service_provider_id character varying(255),
    appointment_id character varying(255),
    parent_encounter_id character varying(255),
    meta_data character varying(256),
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);