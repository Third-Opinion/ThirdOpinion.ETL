CREATE TABLE public.condition_evidence (
    condition_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    evidence_code character varying(65535) ENCODE lzo,
    evidence_system character varying(65535) ENCODE lzo,
    evidence_display character varying(65535) ENCODE lzo,
    evidence_detail_reference character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
