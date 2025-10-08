CREATE TABLE public.practitioner_addresses (
    practitioner_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    line character varying(500) ENCODE lzo,
    city character varying(100) ENCODE lzo,
    state character varying(50) ENCODE raw,
    postal_code character varying(20) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( practitioner_id, state );
