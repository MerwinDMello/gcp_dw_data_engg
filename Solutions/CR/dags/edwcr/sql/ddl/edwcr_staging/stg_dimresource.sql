CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimresource (
dimsiteid INT64
, dimresourceid INT64
, dimlookupid_resourcetype INT64
, actualresourceid INT64
, ctrresourceser INT64
, logid INT64
, runid INT64
, ctrstkh_id STRING
, dw_last_update_date_time DATETIME
)
  ;
