CREATE TABLE public.care_plan_identifiers (
    care_plan_id character varying(65535) ENCODE lzo,
    identifier_system character varying(65535) ENCODE lzo,
    identifier_value character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
