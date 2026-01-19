CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_score
   OPTIONS(description='Contains the detail scores associated with the cancer id output model on Cloudera.')
  AS SELECT
      cancer_patient_id_score.cancer_patient_id_output_sk,
      cancer_patient_id_score.score_sequence_num,
      cancer_patient_id_score.message_control_id_text,
      cancer_patient_id_score.site_associated_model_output_site_desc,
      cancer_patient_id_score.site_associated_model_output_score_num,
      cancer_patient_id_score.source_system_code,
      cancer_patient_id_score.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_id_score
  ;
