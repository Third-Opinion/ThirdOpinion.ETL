CREATE TABLE public.medication_dispenses (
    medication_dispense_id character varying(65535) ENCODE lzo,
    resource_type character varying(65535) ENCODE lzo,
    status character varying(65535) ENCODE lzo,
    patient_id character varying(65535) ENCODE lzo,
    medication_id character varying(65535) ENCODE lzo,
    medication_display character varying(65535) ENCODE lzo,
    type_system character varying(65535) ENCODE lzo,
    type_code character varying(65535) ENCODE lzo,
    type_display character varying(65535) ENCODE lzo,
    quantity_value numeric(10,2) ENCODE az64,
    when_handed_over timestamp without time zone ENCODE az64,
    meta_last_updated timestamp without time zone ENCODE az64,
    created_at timestamp without time zone ENCODE az64,
    updated_at timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
