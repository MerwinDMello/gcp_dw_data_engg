CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimenergy (
dimsiteid INT64
, dimenergyid INT64
, dimmachineid INT64
, radiationtype STRING
, energy INT64
, maxenergy INT64
, mindoserate INT64
, maxdoserate INT64
, ctrenergymodeser INT64
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
