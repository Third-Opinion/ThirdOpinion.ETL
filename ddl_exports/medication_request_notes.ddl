CREATE TABLE public.medication_request_notes (
    medication_request_id character varying(65535) ENCODE lzo,
    note_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
