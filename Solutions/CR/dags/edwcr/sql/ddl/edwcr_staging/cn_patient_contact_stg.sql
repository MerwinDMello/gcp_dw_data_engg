CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_contact_stg (
cn_patient_contact_sid INT64 NOT NULL
, purpose_of_contact STRING
, method_of_contact STRING
, person_of_contact STRING
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, diagnosis_result_id INT64
, nav_diagnosis_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, contact_date DATE
, other_purpose_detail_text STRING
, other_person_contacted_text STRING
, time_spent_amount_text STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
