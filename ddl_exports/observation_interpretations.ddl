CREATE TABLE public.observation_interpretations (
    observation_id character varying(65535) ENCODE lzo,
    interpretation_code character varying(65535) ENCODE lzo,
    interpretation_system character varying(65535) ENCODE lzo,
    interpretation_display character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
