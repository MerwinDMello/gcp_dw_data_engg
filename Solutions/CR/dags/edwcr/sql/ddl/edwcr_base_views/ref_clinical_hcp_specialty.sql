CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_clinical_hcp_specialty AS SELECT
    ref_clinical_hcp_specialty.hcp_specialty_code,
    ref_clinical_hcp_specialty.company_code,
    ref_clinical_hcp_specialty.coid,
    ref_clinical_hcp_specialty.hcp_specialty_desc,
    ref_clinical_hcp_specialty.source_system_code,
    ref_clinical_hcp_specialty.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_clinical_hcp_specialty
;
