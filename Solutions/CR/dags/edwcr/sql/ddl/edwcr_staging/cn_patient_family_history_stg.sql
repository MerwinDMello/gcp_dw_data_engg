CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_family_history_stg (
patienthistoryfactid INT64
, patientdimid NUMERIC(18,0)
, coid STRING
, family_history_query_id INT64
, family_history_value_text STRING
, hbsource STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
