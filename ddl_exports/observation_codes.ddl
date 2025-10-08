CREATE TABLE public.observation_codes (
    observation_id character varying(65535) ENCODE lzo,
    code_code character varying(65535) ENCODE lzo,
    code_system character varying(65535) ENCODE lzo,
    code_display character varying(65535) ENCODE lzo,
    code_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
