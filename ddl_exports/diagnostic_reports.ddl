CREATE TABLE public.diagnostic_reports (
    diagnostic_report_id character varying(65535) ENCODE lzo,
    resource_type character varying(65535) ENCODE lzo,
    status character varying(65535) ENCODE lzo,
    effective_datetime timestamp without time zone ENCODE az64,
    issued_datetime timestamp without time zone ENCODE az64,
    code_text character varying(65535) ENCODE lzo,
    code_primary_code character varying(65535) ENCODE lzo,
    code_primary_system character varying(65535) ENCODE lzo,
    code_primary_display character varying(65535) ENCODE lzo,
    patient_id character varying(65535) ENCODE lzo,
    encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    extensions character varying(65535) ENCODE lzo,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
