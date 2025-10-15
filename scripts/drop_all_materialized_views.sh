#!/bin/bash

# Quick script to drop all materialized views before redeploying

CLUSTER_ID="prod-redshift-main-ue2"
DATABASE="dev"
SECRET_ARN="arn:aws:secretsmanager:us-east-2:442042533707:secret:redshift!prod-redshift-main-ue2-awsuser-yp5Lq4"
REGION="us-east-2"

VIEWS=(
    "fact_fhir_practitioners_view_v1"
    "fact_fhir_patients_view_v1"
    "fact_fhir_allergy_intolerance_view_v1"
    "fact_fhir_care_plans_view_v1"
    "fact_fhir_medication_requests_view_v1"
    "fact_fhir_procedures_view_v1"
    "fact_fhir_diagnostic_reports_view_v1"
    "fact_fhir_document_references_view_v1"
    "fact_fhir_observations_view_v1"
    "fact_fhir_conditions_view_v1"
    "fact_fhir_encounters_view_v1"
    "rpt_fhir_hmu_patients_v1"
    "rpt_fhir_medication_requests_adt_meds_hmu_view_v1"
    "rpt_fhir_observations_psa_total_hmu_v1"
    "rpt_fhir_observations_testosterone_total_hmu_v1"
    "rpt_fhir_observations_absolute_neutrophil_count_hmu_v1"
    "rpt_fhir_observations_platelet_count_hmu_v1"
    "rpt_fhir_observations_hemoglobin_hmu_v1"
    "rpt_fhir_observations_creatinine_hmu_v1"
    "rpt_fhir_observations_egfr_hmu_v1"
    "rpt_fhir_observations_alt_hmu_v1"
    "rpt_fhir_observations_ast_hmu_v1"
    "rpt_fhir_observations_total_bilirubin_hmu_v1"
    "rpt_fhir_observations_serum_albumin_hmu_v1"
    "rpt_fhir_observations_serum_potassium_hmu_v1"
    "rpt_fhir_observations_hba1c_hmu_v1"
    "rpt_fhir_observations_bmi_hmu_v1"
    "rpt_fhir_observations_cd4_count_hmu_v1"
    "rpt_fhir_observations_hiv_viral_load_hmu_v1"
    "rpt_fhir_conditions_additional_malignancy_hmu_v1"
    "rpt_fhir_conditions_active_liver_disease_hmu_v1"
    "rpt_fhir_conditions_cns_metastases_hmu_v1"
)

echo "Dropping all materialized views..."

for view in "${VIEWS[@]}"; do
    echo "Dropping $view..."

    statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier "$CLUSTER_ID" \
        --database "$DATABASE" \
        --secret-arn "$SECRET_ARN" \
        --sql "DROP MATERIALIZED VIEW IF EXISTS public.$view CASCADE;" \
        --region "$REGION" \
        --query Id \
        --output text 2>/dev/null)

    if [ ! -z "$statement_id" ]; then
        # Wait for completion
        status="SUBMITTED"
        timeout=30
        while [ "$status" != "FINISHED" ] && [ "$status" != "FAILED" ] && [ "$status" != "ABORTED" ] && [ $timeout -gt 0 ]; do
            sleep 1
            status=$(aws redshift-data describe-statement \
                --id "$statement_id" \
                --region "$REGION" \
                --query Status \
                --output text 2>/dev/null)
            timeout=$((timeout - 1))
        done

        if [ "$status" = "FINISHED" ]; then
            echo "  ✓ Dropped $view"
        else
            echo "  ○ $view (may not have existed)"
        fi
    fi
done

echo ""
echo "All materialized views dropped. You can now run the deployment script."
