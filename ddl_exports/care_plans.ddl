CREATE TABLE public.care_plans (
    care_plan_id character varying(65535) ENCODE lzo,
    patient_id character varying(65535) ENCODE lzo,
    status character varying(65535) ENCODE lzo,
    intent character varying(65535) ENCODE lzo,
    title character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
