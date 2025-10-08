CREATE TABLE public.patient_addresses (
    patient_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    address_use character varying(65535) ENCODE lzo,
    address_type character varying(65535) ENCODE lzo,
    address_text character varying(65535) ENCODE lzo,
    address_line character varying(65535) ENCODE lzo,
    city character varying(65535) ENCODE lzo,
    district character varying(65535) ENCODE lzo,
    state character varying(65535) ENCODE lzo,
    postal_code character varying(65535) ENCODE lzo,
    country character varying(65535) ENCODE lzo,
    period_start date ENCODE az64,
    period_end date ENCODE az64
)
DISTSTYLE EVEN;
