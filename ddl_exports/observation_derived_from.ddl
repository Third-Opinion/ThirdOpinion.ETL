CREATE TABLE public.observation_derived_from (
    observation_id character varying(65535) ENCODE lzo,
    derived_from_reference character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
