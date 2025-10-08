CREATE TABLE public.encounter_types (
    encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    type_code character varying(65535) ENCODE lzo,
    type_system character varying(65535) ENCODE lzo,
    type_display character varying(65535) ENCODE lzo,
    type_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
