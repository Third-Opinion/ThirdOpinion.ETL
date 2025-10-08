CREATE TABLE public.procedure_code_codings (
    procedure_id character varying(255) ENCODE raw,
    code_system character varying(255) ENCODE raw,
    code_code character varying(100) ENCODE lzo,
    code_display character varying(500) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( procedure_id, code_system );
