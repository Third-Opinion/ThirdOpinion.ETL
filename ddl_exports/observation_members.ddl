CREATE TABLE public.observation_members (
    observation_id character varying(65535) ENCODE lzo,
    member_observation_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
