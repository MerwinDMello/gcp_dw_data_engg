CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_family_history
   OPTIONS(description='Contains oncology details associated with a patients family history.')
  AS SELECT
      cn_patient_family_history.cn_patient_family_history_sid,
      cn_patient_family_history.family_history_query_id,
      cn_patient_family_history.nav_patient_id,
      cn_patient_family_history.coid,
      cn_patient_family_history.company_code,
      cn_patient_family_history.family_history_value_text,
      cn_patient_family_history.hashbite_ssk,
      cn_patient_family_history.source_system_code,
      cn_patient_family_history.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_family_history
  ;
