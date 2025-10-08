CREATE TABLE public.practitioner_telecoms (
    practitioner_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    system character varying(50) ENCODE raw,
    value character varying(255) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( practitioner_id, system );
