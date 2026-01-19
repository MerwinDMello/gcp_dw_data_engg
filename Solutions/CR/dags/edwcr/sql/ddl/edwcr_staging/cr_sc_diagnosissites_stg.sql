CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_sc_diagnosissites_stg (
diagnosiscode STRING
, diagnosissites STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
