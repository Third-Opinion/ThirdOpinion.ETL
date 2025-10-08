CREATE TABLE public.patient_names (
    patient_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    name_use character varying(65535) ENCODE lzo,
    name_text character varying(65535) ENCODE lzo,
    family_name character varying(65535) ENCODE lzo,
    given_names character varying(65535) ENCODE lzo,
    prefix character varying(65535) ENCODE lzo,
    suffix character varying(65535) ENCODE lzo,
    period_start date ENCODE az64,
    period_end date ENCODE az64
)
DISTSTYLE EVEN;
