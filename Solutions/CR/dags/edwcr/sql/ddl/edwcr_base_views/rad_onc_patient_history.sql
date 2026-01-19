CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_history
   OPTIONS(description='Contains history details of the patient')
  AS SELECT
      rad_onc_patient_history.patient_sk,
      rad_onc_patient_history.history_query_id,
      rad_onc_patient_history.history_value_text,
      rad_onc_patient_history.source_system_code,
      rad_onc_patient_history.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_patient_history
  ;
