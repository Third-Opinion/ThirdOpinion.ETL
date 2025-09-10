CREATE TABLE public.observation_reference_ranges (
    observation_id character varying(65535),
    range_low_value numeric(10,2),
    range_low_unit character varying(65535),
    range_high_value numeric(10,2),
    range_high_unit character varying(65535),
    range_type_code character varying(65535),
    range_type_system character varying(65535),
    range_type_display character varying(65535),
    range_text character varying(65535)
);