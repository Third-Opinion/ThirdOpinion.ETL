CREATE TABLE public.mv_tbl__fact_fhir_practitioners_view_v1__0 (
    practitioner_id character varying(255) ENCODE raw distkey,
    resource_type character varying(50) ENCODE lzo,
    active boolean ENCODE raw,
    meta_last_updated timestamp without time zone ENCODE az64,
    etl_created_at timestamp without time zone ENCODE az64,
    etl_updated_at timestamp without time zone ENCODE az64,
    names super,
    addresses super,
    telecoms super,
    telecom_count bigint ENCODE az64,
    primary_phone character varying(255) ENCODE lzo,
    primary_email character varying(255) ENCODE lzo,
    has_complete_contact boolean ENCODE raw,
    primary_city character varying(100) ENCODE lzo,
    primary_state character varying(50) ENCODE lzo
)
DISTSTYLE KEY
SORTKEY ( practitioner_id );
