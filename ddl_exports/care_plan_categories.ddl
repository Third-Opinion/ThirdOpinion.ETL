CREATE TABLE public.care_plan_categories (
    care_plan_id character varying(65535) ENCODE lzo,
    category_code character varying(65535) ENCODE lzo,
    category_system character varying(65535) ENCODE lzo,
    category_display character varying(65535) ENCODE lzo,
    category_text character varying(65535) ENCODE lzo
)
DISTSTYLE EVEN;
