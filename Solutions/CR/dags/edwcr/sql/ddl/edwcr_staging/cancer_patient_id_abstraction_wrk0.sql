CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_abstraction_wrk0 (
cancer_abstraction_sk INT64
, cancer_patient_id_output_sk INT64
, message_control_id_text STRING
, coid STRING
, company_code STRING
, patient_dw_id NUMERIC(18,0)
, pat_acct_num NUMERIC(12,0)
, abstraction_report_assigned_date_time DATETIME
, abstraction_date_time DATETIME
, abstraction_action_user_3_4_id STRING
, abstraction_action_desc STRING
, abstraction_action_date_time DATETIME
, primary_icd_oncology_code STRING
, primary_icd_site_desc STRING
, primary_icd_site_and_model_score_desc STRING
, changed_primary_icd_oncology_code STRING
, changed_primary_icd_site_desc STRING
, topography_icd_oncology_code STRING
, topography_icd_site_desc STRING
, laterality_icd_oncology_code STRING
, laterality_icd_site_desc STRING
, secondary_icd_oncology_code STRING
, secondary_icd_site_desc STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
