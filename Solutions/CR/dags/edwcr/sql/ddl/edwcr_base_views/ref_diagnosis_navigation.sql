CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_diagnosis_navigation
   OPTIONS(description='Contains a distinct list of diagnoses.')
  AS SELECT
      ref_diagnosis_navigation.nav_diagnosis_id,
      ref_diagnosis_navigation.nav_diagnosis_desc,
      ref_diagnosis_navigation.source_system_code,
      ref_diagnosis_navigation.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_diagnosis_navigation
  ;
