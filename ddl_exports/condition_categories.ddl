CREATE TABLE public.condition_categories (
    condition_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    category_code character varying(65535) ENCODE lzo,
    category_system character varying(65535) ENCODE lzo,
    category_display character varying(65535) ENCODE lzo,
    category_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
