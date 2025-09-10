CREATE TABLE public.condition_extensions (
    condition_id character varying(255) NOT NULL,
    extension_url character varying(500) NOT NULL,
    extension_type character varying(50) NOT NULL,
    value_type character varying(50),
    value_string character varying(65535),
    value_datetime timestamp without time zone,
    value_reference character varying(255),
    value_code character varying(100),
    value_boolean boolean,
    value_decimal numeric(18,6),
    value_integer integer,
    parent_extension_url character varying(500),
    extension_order integer,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);