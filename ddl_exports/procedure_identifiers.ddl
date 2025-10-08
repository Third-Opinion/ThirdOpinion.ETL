CREATE TABLE public.procedure_identifiers (
    procedure_id character varying(255) ENCODE raw,
    identifier_system character varying(255) ENCODE raw,
    identifier_value character varying(255) ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( procedure_id, identifier_system );
