CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_complication_stg (
cn_patient_complication_sid INT64
, core_record_type_id INT64
, associatetherapytype STRING
, complicationoutcome STRING
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING
, complication_date DATE
, treatment_stopped_ind STRING
, complication_text STRING
, specific_complication_text STRING
, comment_text STRING
, hashbite_ssk STRING
, dw_last_update_date_time DATETIME
)
  ;
