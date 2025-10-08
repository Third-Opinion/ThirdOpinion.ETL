CREATE TABLE public.encounter_locations (
    encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    location_id character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
