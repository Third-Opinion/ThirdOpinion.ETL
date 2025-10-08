CREATE TABLE public.care_plan_care_teams (
    care_plan_id character varying(65535) ENCODE lzo,
    care_team_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
