CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_diagnosis_stg (
cn_patient_diagnosis_sid INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING
, general_diagnosis_name STRING
, diagnosis_date DATE
, diagnosismetastatic STRING
, diagnosisindicator STRING
, diagnosisside STRING
, hashbite_ssk STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
