CREATE TABLE public.medication_dispense_auth_prescriptions (
    medication_dispense_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    authorizing_prescription_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
