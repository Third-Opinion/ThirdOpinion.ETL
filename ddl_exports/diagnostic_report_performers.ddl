CREATE TABLE public.diagnostic_report_performers (
    diagnostic_report_id character varying(65535) ENCODE lzo,
    performer_type character varying(65535) ENCODE lzo,
    performer_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
