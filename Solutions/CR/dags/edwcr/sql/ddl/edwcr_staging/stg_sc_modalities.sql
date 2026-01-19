CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_sc_modalities (
treatment_category STRING
, treatment_type STRING
, procedure_code STRING
, dw_last_update_date_time DATETIME
)
  ;
