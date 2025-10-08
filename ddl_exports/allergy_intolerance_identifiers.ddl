CREATE TABLE public.allergy_intolerance_identifiers (
    allergy_intolerance_id character varying(65535) ENCODE lzo,
    identifier_use character varying(65535) ENCODE lzo,
    identifier_type_code character varying(65535) ENCODE lzo,
    identifier_type_display character varying(65535) ENCODE lzo,
    identifier_system character varying(65535) ENCODE lzo,
    identifier_value character varying(65535) ENCODE lzo,
    identifier_period_start date ENCODE az64,
    identifier_period_end date ENCODE az64
)
DISTSTYLE EVEN;
