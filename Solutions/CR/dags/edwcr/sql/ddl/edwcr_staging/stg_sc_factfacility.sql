CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_sc_factfacility (
enterprise STRING
, group_name STRING
, division STRING
, market STRING
, facility STRING
, hospitalname STRING
, coid STRING
, dw_last_update_date_time DATETIME
)
  ;
