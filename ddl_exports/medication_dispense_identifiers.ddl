CREATE TABLE public.medication_dispense_identifiers (
    medication_dispense_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    identifier_system character varying(65535) ENCODE lzo,
    identifier_value character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
