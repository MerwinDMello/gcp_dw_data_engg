CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimlookup (
dimsiteid INT64
, dimlookupid INT64
, tablename STRING
, lookupcode STRING
, lookuptype INT64
, subselector INT64
, lookupdescriptionenu STRING
, ctrlookuptableser INT64
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
