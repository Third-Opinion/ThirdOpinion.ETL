CREATE TABLE public.patient_telecoms (
    patient_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    telecom_system character varying(65535) ENCODE lzo,
    telecom_value character varying(65535) ENCODE lzo,
    telecom_use character varying(65535) ENCODE lzo,
    telecom_rank integer ENCODE az64,
    period_start date ENCODE az64,
    period_end date ENCODE az64
)
DISTSTYLE EVEN;
