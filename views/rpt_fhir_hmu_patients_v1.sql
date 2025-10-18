CREATE MATERIALIZED VIEW rpt_fhir_hmu_patients_v1
DISTKEY(patient_id)
SORTKEY(patient_id, last_encounter_date)
AS
WITH target_patients AS (
    SELECT
        DISTINCT c.patient_id,
        e.last_encounter_date,
        c.code_code,
        c.code_system
    FROM
        fact_fhir_conditions_view_v1 AS c
        INNER JOIN (
            SELECT
                patient_id,
                MAX(start_time) AS last_encounter_date
            FROM
                fact_fhir_encounters_view_v1
            GROUP BY
                patient_id
        ) AS e ON c.patient_id = e.patient_id
    WHERE
        c.code_code IN ('C61')
        AND c.code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
        AND e.last_encounter_date >= '2024-01-01'
)
SELECT
    patient_id,
    last_encounter_date,
    code_code,
    code_system
FROM target_patients;