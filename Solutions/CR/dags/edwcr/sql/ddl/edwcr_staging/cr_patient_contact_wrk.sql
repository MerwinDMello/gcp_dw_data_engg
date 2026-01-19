CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_contact_wrk (
patient_contact_id INT64 NOT NULL
, cr_patient_id INT64 NOT NULL
, contact_relation_id INT64
, contact_type_id INT64
, contact_num_code STRING
, contact_first_name STRING
, contact_last_name STRING
, contact_middle_name STRING
, preferred_contact_method_text STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
