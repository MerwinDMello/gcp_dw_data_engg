CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimcelltype (
dimsiteid INT64
, dimcelltypeid INT64
, celltypebehaviorcode STRING
, clsschemeid INT64
, morphcode STRING
, morphcodesequence INT64
, varishistologycode STRING
, celltypeenu STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
