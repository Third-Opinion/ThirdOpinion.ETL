CREATE TABLE public.patient_addresses (
    patient_id character varying(65535),
    address_use character varying(65535),
    address_type character varying(65535),
    address_text character varying(65535),
    address_line character varying(65535),
    city character varying(65535),
    district character varying(65535),
    state character varying(65535),
    postal_code character varying(65535),
    country character varying(65535),
    period_start date,
    period_end date,
    dummy character varying(65535)
);