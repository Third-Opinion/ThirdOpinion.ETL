CREATE TABLE public.medication_dispense_dosage_instructions (
    medication_dispense_id character varying(255),
    dosage_text character varying(65535),
    dosage_timing_frequency integer,
    dosage_timing_period integer,
    dosage_timing_period_unit character varying(20),
    dosage_route_code character varying(50),
    dosage_route_system character varying(255),
    dosage_route_display character varying(255),
    dosage_dose_value numeric(10,2),
    dosage_dose_unit character varying(100),
    dosage_dose_system character varying(255),
    dosage_dose_code character varying(50)
);