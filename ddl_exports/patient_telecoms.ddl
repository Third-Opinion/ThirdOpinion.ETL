CREATE TABLE public.patient_telecoms (
    patient_id character varying(65535),
    telecom_system character varying(65535),
    telecom_value character varying(65535),
    telecom_use character varying(65535),
    telecom_rank integer,
    period_start date,
    period_end date,
    dummy character varying(65535)
);