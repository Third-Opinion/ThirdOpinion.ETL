CREATE TABLE public.encounter_participants (
    encounter_id character varying(255),
    participant_type character varying(50),
    participant_id character varying(255),
    participant_display character varying(255),
    period_start timestamp without time zone,
    period_end timestamp without time zone
);