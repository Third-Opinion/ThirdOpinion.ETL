CREATE TABLE public.encounter_hospitalization (
    encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    discharge_disposition_text character varying(65535) ENCODE lzo,
    discharge_code character varying(65535) ENCODE lzo,
    discharge_system character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
