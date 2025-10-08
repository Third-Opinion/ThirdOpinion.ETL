CREATE TABLE public.patient_links (
    patient_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    other_patient_id character varying(65535) ENCODE lzo,
    link_type_code character varying(65535) ENCODE lzo,
    link_type_system character varying(65535) ENCODE lzo,
    link_type_display character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
