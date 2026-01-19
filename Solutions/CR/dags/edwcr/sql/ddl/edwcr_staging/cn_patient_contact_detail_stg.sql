CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_contact_detail_stg (
cn_patient_contact_sid INT64 NOT NULL
, contact_detail_measure_type_id INT64 NOT NULL
, contact_detail_measure_val_txt STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
