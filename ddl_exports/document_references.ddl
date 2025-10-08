CREATE TABLE public.document_references (
    document_reference_id character varying(255) NOT NULL ENCODE lzo,
    patient_id character varying(255) NOT NULL ENCODE raw distkey,
    status character varying(50) ENCODE lzo,
    type_code character varying(50) ENCODE lzo,
    type_system character varying(255) ENCODE lzo,
    type_display character varying(500) ENCODE lzo,
    meta_type character varying(50) ENCODE lzo,
    date timestamp without time zone ENCODE raw,
    custodian_id character varying(255) ENCODE lzo,
    description character varying(65535) ENCODE lzo,
    context_period_start timestamp without time zone ENCODE az64,
    context_period_end timestamp without time zone ENCODE az64,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64,
    updated_at timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64,
    PRIMARY KEY (document_reference_id)
)
DISTSTYLE KEY
SORTKEY ( patient_id, date );
