CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_lab_result_stg (
nav_patient_lab_result_sid INT64 NOT NULL
, lab_type_id INT64
, core_record_type_id INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, lab_date DATE
, lab_result_text STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
