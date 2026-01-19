CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_diagnosis_result
   OPTIONS(description='Contains a distinct list of diagnosis result outcomes.')
  AS SELECT
      ref_diagnosis_result.diagnosis_result_id,
      ref_diagnosis_result.diagnosis_result_desc,
      ref_diagnosis_result.source_system_code,
      ref_diagnosis_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_diagnosis_result
  ;
