CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_diagnosis_detail
   OPTIONS(description='Contains the different queries associated the patient diagnosis.')
  AS SELECT
      ref_diagnosis_detail.diagnosis_detail_id,
      ref_diagnosis_detail.diagnosis_detail_desc,
      ref_diagnosis_detail.diagnosis_indicator_text,
      ref_diagnosis_detail.source_system_code,
      ref_diagnosis_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail
  ;
