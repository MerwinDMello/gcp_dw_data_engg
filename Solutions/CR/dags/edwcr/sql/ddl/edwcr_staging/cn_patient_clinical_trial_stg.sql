CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_clinical_trial_stg (
cn_patient_clinical_trial_sid INT64
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING
, company_code STRING
, clinical_trial_name STRING
, clinical_trial_enrolled_ind STRING
, clinical_trial_enrolled_date DATE
, clinical_trial_offered_ind STRING
, clinical_trial_offered_date DATE
, hashbite_ssk STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
