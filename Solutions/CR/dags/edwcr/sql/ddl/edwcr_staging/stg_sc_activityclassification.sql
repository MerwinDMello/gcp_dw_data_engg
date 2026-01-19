CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_sc_activityclassification (
hospital STRING
, category STRING
, activityname STRING
, activitytype STRING
, level_1 STRING
, level_2 STRING
, level_3 STRING
, level_4 STRING
, dw_last_update_date_time DATETIME
)
  ;
