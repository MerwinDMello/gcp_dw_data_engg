CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_sc_modalities_wrk (
treatment_type_sk INT64
, treatment_category_desc STRING
, treatment_type_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
