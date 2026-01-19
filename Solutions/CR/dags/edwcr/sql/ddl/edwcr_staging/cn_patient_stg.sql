CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_stg (
nav_patient_id NUMERIC(18,0)
, navigator_id INT64
, coid STRING
, patient_market_urn STRING
, medical_record_num STRING
, empi_text STRING
, nav_create_date DATE
, gynecologist STRING
, gynecologistphone STRING
, primarycarephysician STRING
, pcpphone STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
