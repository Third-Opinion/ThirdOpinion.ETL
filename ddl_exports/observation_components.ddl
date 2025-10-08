CREATE TABLE public.observation_components (
    observation_id character varying(65535) ENCODE lzo,
    component_code character varying(65535) ENCODE lzo,
    component_system character varying(65535) ENCODE lzo,
    component_display character varying(65535) ENCODE lzo,
    component_text character varying(65535) ENCODE lzo,
    component_value_string character varying(65535) ENCODE lzo,
    component_value_quantity_value numeric(10,2) ENCODE az64,
    component_value_quantity_unit character varying(65535) ENCODE lzo,
    component_value_codeable_concept_code character varying(65535) ENCODE lzo,
    component_value_codeable_concept_system character varying(65535) ENCODE lzo,
    component_value_codeable_concept_display character varying(65535) ENCODE lzo,
    component_data_absent_reason_code character varying(65535) ENCODE lzo,
    component_data_absent_reason_display character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
