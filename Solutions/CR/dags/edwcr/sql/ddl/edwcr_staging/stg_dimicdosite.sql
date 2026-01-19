CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimicdosite (
dimsiteid INT64
, dimicdositeid INT64
, icdositecodeid STRING
, icdosequence INT64
, icdoclsschemeid INT64
, icdositecodeenu STRING
, icdoversionenu STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
