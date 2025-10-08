CREATE TABLE public.document_reference_categories (
    document_reference_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    category_code character varying(50) ENCODE raw,
    category_system character varying(255) ENCODE lzo,
    category_display character varying(255) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( document_reference_id, category_code );
