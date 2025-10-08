CREATE TABLE public.medication_dispense_performers (
    medication_dispense_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    performer_actor_reference character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
