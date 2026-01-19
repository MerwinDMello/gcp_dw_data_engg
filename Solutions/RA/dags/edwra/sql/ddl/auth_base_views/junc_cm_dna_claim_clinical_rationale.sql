CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.junc_cm_dna_claim_clinical_rationale AS
SELECT
    junc_cm_dna_claim_clinical_rationale.claim_clinical_rationale_id,
    junc_cm_dna_claim_clinical_rationale.eff_from_date_time,
    junc_cm_dna_claim_clinical_rationale.claim_id,
    junc_cm_dna_claim_clinical_rationale.clinical_rationale_id,
    junc_cm_dna_claim_clinical_rationale.user_id,
    junc_cm_dna_claim_clinical_rationale.source_created_date_time_utc,
    junc_cm_dna_claim_clinical_rationale.source_updated_date_time_utc,
    junc_cm_dna_claim_clinical_rationale.eff_to_date_time,
    junc_cm_dna_claim_clinical_rationale.source_system_code,
    junc_cm_dna_claim_clinical_rationale.dw_last_update_date_time
FROM
{{ params.param_cm_project_id }}.auth_base_views.junc_cm_dna_claim_clinical_rationale