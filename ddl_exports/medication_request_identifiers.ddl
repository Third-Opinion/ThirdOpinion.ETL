CREATE TABLE public.medication_request_identifiers (
    medication_request_id character varying(65535) ENCODE lzo,
    identifier_system character varying(65535) ENCODE lzo,
    identifier_value character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
