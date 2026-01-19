CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimlocation (
dimsiteid STRING
, dimlocationid STRING
, country STRING
, state STRING
, city STRING
, county STRING
, postalcode STRING
, logid STRING
, runid STRING
, dw_last_update_date_time DATETIME
)
  ;
