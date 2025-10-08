CREATE TABLE public.document_reference_content (
    document_reference_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    attachment_content_type character varying(100) ENCODE lzo,
    attachment_url character varying(65535) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( document_reference_id );
