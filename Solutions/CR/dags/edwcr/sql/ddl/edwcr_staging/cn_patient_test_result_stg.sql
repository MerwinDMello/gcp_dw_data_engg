CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_test_result_stg (
nav_patient_test_result_sid INT64 NOT NULL
, test_type_id INT64 NOT NULL
, nav_patient_id NUMERIC(18,0) NOT NULL
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING
, test_date DATE
, test_performed_ind STRING
, test_value_num NUMERIC(4,2)
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
