CREATE TABLE public.medication_dispense_dosage_instructions (
    medication_dispense_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    dosage_text character varying(65535) ENCODE lzo,
    dosage_timing_frequency integer ENCODE az64,
    dosage_timing_period integer ENCODE az64,
    dosage_timing_period_unit character varying(65535) ENCODE lzo,
    dosage_route_code character varying(65535) ENCODE lzo,
    dosage_route_system character varying(65535) ENCODE lzo,
    dosage_route_display character varying(65535) ENCODE lzo,
    dosage_dose_value numeric(10,2) ENCODE az64,
    dosage_dose_unit character varying(65535) ENCODE lzo,
    dosage_dose_system character varying(65535) ENCODE lzo,
    dosage_dose_code character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
