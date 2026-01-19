CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.rad_onc_patient_course (
patient_course_sk INT64
, patient_sk INT64
, clinical_status_id INT64
, treatment_intent_type_id INT64
, site_sk INT64
, source_patient_course_id INT64
, course_id_text STRING
, course_start_date_time DATETIME
, course_session_planned_num INT64
, course_session_delivered_num INT64
, course_session_remaining_num INT64
, dose_delivered_amt NUMERIC(18,9)
, course_duration_num INT64
, comment_text STRING
, log_id INT64
, run_id INT64
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
