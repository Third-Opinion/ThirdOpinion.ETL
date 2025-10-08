CREATE TABLE public.encounter_reasons (
    encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    reason_code character varying(65535) ENCODE lzo,
    reason_system character varying(65535) ENCODE lzo,
    reason_display character varying(65535) ENCODE lzo,
    reason_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
