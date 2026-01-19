CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimdxsite (
dimsiteid INT64
, dimdxsiteid INT64
, tpclstypeid INT64
, tpclsvalueid INT64
, sitedescenu STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
