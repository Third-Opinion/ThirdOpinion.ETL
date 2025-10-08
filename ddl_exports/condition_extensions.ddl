CREATE TABLE public.condition_extensions (
    condition_id character varying(65535) ENCODE lzo,
    extension_url character varying(65535) ENCODE lzo,
    extension_type character varying(65535) ENCODE lzo,
    value_type character varying(65535) ENCODE lzo,
    value_string character varying(65535) ENCODE lzo,
    value_datetime timestamp without time zone ENCODE az64,
    value_reference character varying(65535) ENCODE lzo,
    value_code character varying(65535) ENCODE lzo,
    value_boolean boolean ENCODE raw,
    value_decimal numeric(10,2) ENCODE az64,
    value_integer integer ENCODE az64,
    parent_extension_url character varying(65535) ENCODE lzo,
    extension_order integer ENCODE az64,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
