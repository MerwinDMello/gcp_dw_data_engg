CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimaddon (
dimsiteid INT64
, dimaddonid INT64
, dimmachineid INT64
, addonid STRING
, addonname STRING
, addontype STRING
, addonsubtype STRING
, addonser INT64
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
