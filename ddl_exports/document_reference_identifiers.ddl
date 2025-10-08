CREATE TABLE public.document_reference_identifiers (
    document_reference_id character varying(255) ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    identifier_system character varying(255) ENCODE raw,
    identifier_value character varying(255) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( document_reference_id, identifier_system );
