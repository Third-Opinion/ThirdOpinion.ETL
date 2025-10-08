CREATE TABLE public.observation_reference_ranges (
    observation_id character varying(65535) ENCODE lzo,
    range_low_value numeric(10,2) ENCODE az64,
    range_low_unit character varying(65535) ENCODE lzo,
    range_high_value numeric(10,2) ENCODE az64,
    range_high_unit character varying(65535) ENCODE lzo,
    range_type_code character varying(65535) ENCODE lzo,
    range_type_system character varying(65535) ENCODE lzo,
    range_type_display character varying(65535) ENCODE lzo,
    range_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
