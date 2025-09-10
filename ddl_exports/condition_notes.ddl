CREATE TABLE public.condition_notes (
    condition_id character varying(255),
    note_text character varying(65535),
    note_author_reference character varying(255),
    note_time timestamp without time zone
);