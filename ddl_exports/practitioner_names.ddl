CREATE TABLE public.practitioner_names (
    practitioner_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    text character varying(500) ENCODE lzo,
    family character varying(255) ENCODE raw,
    given character varying(255) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( practitioner_id, family );
