CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_id_abstraction_detail_wrk0 (
cancer_abstraction_sk INT64 NOT NULL
, abstraction_measure_sk INT64 NOT NULL
, cancer_patient_id_output_sk INT64 NOT NULL
, message_control_id_text STRING
, coid STRING
, company_code STRING
, patient_dw_id NUMERIC(18,0)
, pat_acct_num NUMERIC(12,0)
, predicted_value_text STRING
, submitted_value_text STRING
, suggested_value_text STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
