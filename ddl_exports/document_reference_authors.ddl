CREATE TABLE public.document_reference_authors (
    document_reference_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    author_id character varying(255) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( document_reference_id );
