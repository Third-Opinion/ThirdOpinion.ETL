CREATE TABLE public.encounters (
    encounter_id character varying(65535) ENCODE lzo,
    patient_id character varying(65535) ENCODE lzo,
    status character varying(65535) ENCODE lzo,
    resourcetype character varying(65535) ENCODE lzo,
    class_code character varying(65535) ENCODE lzo,
    class_display character varying(65535) ENCODE lzo,
    start_time timestamp without time zone ENCODE az64,
    end_time timestamp without time zone ENCODE az64,
    service_provider_id character varying(65535) ENCODE lzo,
    appointment_id character varying(65535) ENCODE lzo,
    parent_encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
