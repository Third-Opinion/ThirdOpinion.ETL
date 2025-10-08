CREATE TABLE public.condition_codes (
    condition_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    code_code character varying(65535) ENCODE lzo,
    code_system character varying(65535) ENCODE lzo,
    code_display character varying(65535) ENCODE lzo,
    code_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
