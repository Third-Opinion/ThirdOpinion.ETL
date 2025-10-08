CREATE TABLE public.diagnostic_report_based_on (
    diagnostic_report_id character varying(65535) ENCODE lzo,
    service_request_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
