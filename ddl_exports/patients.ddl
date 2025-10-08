CREATE TABLE public.patients (
    patient_id character varying(65535) ENCODE lzo,
    active boolean ENCODE raw,
    gender character varying(65535) ENCODE lzo,
    birth_date date ENCODE az64,
    deceased boolean ENCODE raw,
    deceased_date timestamp without time zone ENCODE az64,
    resourcetype character varying(65535) ENCODE lzo,
    marital_status_code character varying(65535) ENCODE lzo,
    marital_status_display character varying(65535) ENCODE lzo,
    marital_status_system character varying(65535) ENCODE lzo,
    multiple_birth boolean ENCODE raw,
    birth_order integer ENCODE az64,
    managing_organization_id character varying(65535) ENCODE lzo,
    photos character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    meta_source character varying(65535) ENCODE lzo,
    meta_security character varying(65535) ENCODE lzo,
    meta_tag character varying(65535) ENCODE lzo,
    extensions character varying(65535) ENCODE lzo,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
