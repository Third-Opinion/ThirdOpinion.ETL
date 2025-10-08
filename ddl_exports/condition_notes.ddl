CREATE TABLE public.condition_notes (
    condition_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    note_text character varying(65535) ENCODE lzo,
    note_author_reference character varying(65535) ENCODE lzo,
    note_time timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
