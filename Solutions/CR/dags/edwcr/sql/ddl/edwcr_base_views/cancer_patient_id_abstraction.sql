CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_abstraction
   OPTIONS(description='This table contains the abstraction data of the cancer patient identifier model on HDFS.')
  AS SELECT
      cancer_patient_id_abstraction.cancer_abstraction_sk,
      cancer_patient_id_abstraction.cancer_patient_id_output_sk,
      cancer_patient_id_abstraction.message_control_id_text,
      cancer_patient_id_abstraction.coid,
      cancer_patient_id_abstraction.company_code,
      cancer_patient_id_abstraction.patient_dw_id,
      cancer_patient_id_abstraction.pat_acct_num,
      cancer_patient_id_abstraction.abstraction_report_assigned_date_time,
      cancer_patient_id_abstraction.abstraction_date_time,
      cancer_patient_id_abstraction.abstraction_action_user_3_4_id,
      cancer_patient_id_abstraction.abstraction_action_desc,
      cancer_patient_id_abstraction.abstraction_action_date_time,
      cancer_patient_id_abstraction.primary_icd_oncology_code,
      cancer_patient_id_abstraction.primary_icd_site_desc,
      cancer_patient_id_abstraction.primary_icd_site_and_model_score_desc,
      cancer_patient_id_abstraction.changed_primary_icd_oncology_code,
      cancer_patient_id_abstraction.changed_primary_icd_site_desc,
      cancer_patient_id_abstraction.topography_icd_oncology_code,
      cancer_patient_id_abstraction.topography_icd_site_desc,
      cancer_patient_id_abstraction.laterality_icd_oncology_code,
      cancer_patient_id_abstraction.laterality_icd_site_desc,
      cancer_patient_id_abstraction.secondary_icd_oncology_code,
      cancer_patient_id_abstraction.secondary_icd_site_desc,
      cancer_patient_id_abstraction.source_system_code,
      cancer_patient_id_abstraction.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction
  ;
