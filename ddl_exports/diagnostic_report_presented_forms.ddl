CREATE TABLE public.diagnostic_report_presented_forms (
    diagnostic_report_id character varying(65535) ENCODE lzo,
    content_type character varying(65535) ENCODE lzo,
    data character varying(65535) ENCODE lzo,
    title character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
