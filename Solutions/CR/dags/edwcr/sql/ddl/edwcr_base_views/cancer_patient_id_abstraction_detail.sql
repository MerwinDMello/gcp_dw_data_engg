CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_abstraction_detail
   OPTIONS(description='This table contains the abstraction detail data of the cancer patient identifier model on HDFS.')
  AS SELECT
      cancer_patient_id_abstraction_detail.cancer_abstraction_sk,
      cancer_patient_id_abstraction_detail.abstraction_measure_sk,
      cancer_patient_id_abstraction_detail.cancer_patient_id_output_sk,
      cancer_patient_id_abstraction_detail.message_control_id_text,
      cancer_patient_id_abstraction_detail.coid,
      cancer_patient_id_abstraction_detail.company_code,
      cancer_patient_id_abstraction_detail.patient_dw_id,
      cancer_patient_id_abstraction_detail.pat_acct_num,
      cancer_patient_id_abstraction_detail.predicted_value_text,
      cancer_patient_id_abstraction_detail.submitted_value_text,
      cancer_patient_id_abstraction_detail.suggested_value_text,
      cancer_patient_id_abstraction_detail.source_system_code,
      cancer_patient_id_abstraction_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction_detail
  ;
