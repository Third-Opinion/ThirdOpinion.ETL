CREATE TABLE public.practitioners (
    practitioner_id character varying(255) NOT NULL ENCODE raw distkey,
    resource_type character varying(50) ENCODE lzo,
    active boolean ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64,
    updated_at timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64,
    PRIMARY KEY (practitioner_id)
)
DISTSTYLE AUTO
SORTKEY ( practitioner_id );
