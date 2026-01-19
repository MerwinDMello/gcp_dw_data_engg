CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.ref_cm_dna_closure_status AS
SELECT
    ref_cm_dna_closure_status.closure_status_id,
    ref_cm_dna_closure_status.eff_from_date_time,
    ref_cm_dna_closure_status.closure_status_desc,
    ref_cm_dna_closure_status.source_created_date_time_utc,
    ref_cm_dna_closure_status.source_updated_date_time_utc,
    ref_cm_dna_closure_status.eff_to_date_time,
    ref_cm_dna_closure_status.source_system_code,
    ref_cm_dna_closure_status.dw_last_update_date_time
  FROM
   `{{ params.param_cm_project_id }}`.auth_base_views.ref_cm_dna_closure_status