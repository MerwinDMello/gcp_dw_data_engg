CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_diagnosis_code
   OPTIONS(description='Contains information for radiation oncology diagnosis code')
  AS SELECT
      ref_rad_onc_diagnosis_code.diagnosis_code_sk,
      ref_rad_onc_diagnosis_code.site_sk,
      ref_rad_onc_diagnosis_code.source_diagnosis_code_id,
      ref_rad_onc_diagnosis_code.diagnosis_code,
      ref_rad_onc_diagnosis_code.diagnosis_site_text,
      ref_rad_onc_diagnosis_code.diagnosis_code_class_schema_id,
      ref_rad_onc_diagnosis_code.diagnosis_clinical_desc,
      ref_rad_onc_diagnosis_code.diagnosis_long_desc,
      ref_rad_onc_diagnosis_code.diagnosis_type_code,
      ref_rad_onc_diagnosis_code.log_id,
      ref_rad_onc_diagnosis_code.run_id,
      ref_rad_onc_diagnosis_code.source_system_code,
      ref_rad_onc_diagnosis_code.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_diagnosis_code
  ;
