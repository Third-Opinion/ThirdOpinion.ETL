CREATE TABLE public.care_plan_goals (
    care_plan_id character varying(65535) ENCODE lzo,
    goal_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
