CREATE TABLE public.diagnostic_report_results (
    diagnostic_report_id character varying(65535) ENCODE lzo,
    observation_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
