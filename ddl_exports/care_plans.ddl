CREATE TABLE public.care_plans (
    care_plan_id character varying(255) NOT NULL,
    patient_id character varying(255) NOT NULL,
    status character varying(50),
    intent character varying(50),
    title character varying(500),
    meta_version_id character varying(50),
    meta_last_updated timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);