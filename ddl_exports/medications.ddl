CREATE TABLE public.medications (
    medication_id character varying(65535) ENCODE lzo,
    resource_type character varying(65535) ENCODE lzo,
    code_text character varying(65535) ENCODE lzo,
    code character varying(65535) ENCODE lzo,
    primary_code character varying(65535) ENCODE lzo,
    primary_system character varying(65535) ENCODE lzo,
    primary_text character varying(65535) ENCODE lzo,
    status character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
