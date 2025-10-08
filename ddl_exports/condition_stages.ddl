CREATE TABLE public.condition_stages (
    condition_id character varying(65535) ENCODE lzo,
    meta_last_updated timestamp without time zone ENCODE az64,
    stage_summary_code character varying(65535) ENCODE lzo,
    stage_summary_system character varying(65535) ENCODE lzo,
    stage_summary_display character varying(65535) ENCODE lzo,
    stage_assessment_code character varying(65535) ENCODE lzo,
    stage_assessment_system character varying(65535) ENCODE lzo,
    stage_assessment_display character varying(65535) ENCODE lzo,
    stage_type_code character varying(65535) ENCODE lzo,
    stage_type_system character varying(65535) ENCODE lzo,
    stage_type_display character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
