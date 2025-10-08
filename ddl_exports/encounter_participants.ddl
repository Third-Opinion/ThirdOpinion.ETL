CREATE TABLE public.encounter_participants (
    encounter_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    participant_type character varying(65535) ENCODE lzo,
    participant_id character varying(65535) ENCODE lzo,
    participant_display character varying(65535) ENCODE lzo,
    period_start timestamp without time zone ENCODE az64,
    period_end timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;
