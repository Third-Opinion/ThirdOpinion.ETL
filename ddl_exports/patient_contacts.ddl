CREATE TABLE public.patient_contacts (
    patient_id character varying(65535),
    contact_relationship_code character varying(65535),
    contact_relationship_system character varying(65535),
    contact_relationship_display character varying(65535),
    contact_name_text character varying(65535),
    contact_name_family character varying(65535),
    contact_name_given character varying(65535),
    contact_telecom_system character varying(65535),
    contact_telecom_value character varying(65535),
    contact_telecom_use character varying(65535),
    contact_gender character varying(65535),
    contact_organization_id character varying(65535),
    period_start date,
    period_end date,
    dummy character varying(65535)
);