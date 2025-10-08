CREATE TABLE public.medication_requests (
    medication_request_id character varying(65535) ENCODE lzo,
    patient_id character varying(65535) ENCODE lzo,
    encounter_id character varying(65535) ENCODE lzo,
    medication_id character varying(65535) ENCODE lzo,
    medication_display character varying(65535) ENCODE lzo,
    status character varying(65535) ENCODE lzo,
    intent character varying(65535) ENCODE lzo,
    reported_boolean boolean ENCODE raw,
    authored_on timestamp without time zone ENCODE az64,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
