CREATE TABLE public.observation_notes (
    observation_id character varying(65535) ENCODE lzo,
    note_text character varying(65535) ENCODE lzo,
    note_author_reference character varying(65535) ENCODE lzo,
    note_time timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
