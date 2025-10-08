CREATE TABLE public.patient_communications (
    patient_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    language_code character varying(65535) ENCODE lzo,
    language_system character varying(65535) ENCODE lzo,
    language_display character varying(65535) ENCODE lzo,
    preferred boolean ENCODE raw,
    extensions character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
