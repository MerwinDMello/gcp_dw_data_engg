CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_consultation_stg (
cn_patient_consultation_sid INT64 NOT NULL
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, consult_type_id INT64
, nav_patient_id NUMERIC(18,0)
, coid STRING NOT NULL
, company_code STRING NOT NULL
, med_spcl_physician_id INT64
, consult_other_type_text STRING
, consult_date DATE
, consult_phone_num STRING
, consult_notes_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
