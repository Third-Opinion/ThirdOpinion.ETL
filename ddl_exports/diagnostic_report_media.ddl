CREATE TABLE public.diagnostic_report_media (
    diagnostic_report_id character varying(65535) ENCODE lzo,
    media_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
