CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_treatment_wrk (
treatment_id INT64 NOT NULL
, tumor_id INT64
, treatment_hospital_id INT64
, treatment_type_id INT64
, surgical_site_id INT64
, surgical_margin_result_id INT64
, treatment_type_group_id INT64
, clinical_trial_start_date DATE
, treatment_start_date DATE
, clinical_trial_text STRING
, comment_text STRING
, treatment_performing_physician_code STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
