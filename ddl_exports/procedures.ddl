CREATE TABLE public.procedures (
    procedure_id character varying(255) NOT NULL ENCODE lzo,
    resource_type character varying(50) ENCODE lzo,
    status character varying(50) ENCODE lzo,
    patient_id character varying(255) NOT NULL ENCODE raw distkey,
    code_text character varying(500) ENCODE lzo,
    performed_date_time timestamp without time zone ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64,
    updated_at timestamp without time zone DEFAULT ('now'::text)::timestamp with time zone ENCODE az64,
    PRIMARY KEY (procedure_id)
)
DISTSTYLE KEY
SORTKEY ( patient_id, performed_date_time );
