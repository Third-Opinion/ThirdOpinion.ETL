CREATE TABLE public.patient_practitioners (
    patient_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    practitioner_id character varying(65535) ENCODE lzo,
    practitioner_role_id character varying(65535) ENCODE lzo,
    organization_id character varying(65535) ENCODE lzo,
    reference_type character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
