CREATE TABLE public.patient_communications (
    patient_id character varying(65535),
    language_code character varying(65535),
    language_system character varying(65535),
    language_display character varying(65535),
    preferred boolean,
    extensions character varying(65535),
    dummy character varying(65535)
);