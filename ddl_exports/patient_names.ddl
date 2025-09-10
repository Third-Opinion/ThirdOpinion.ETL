CREATE TABLE public.patient_names (
    patient_id character varying(65535),
    name_use character varying(65535),
    name_text character varying(65535),
    family_name character varying(65535),
    given_names character varying(65535),
    prefix character varying(65535),
    suffix character varying(65535),
    period_start date,
    period_end date,
    dummy character varying(65535)
);