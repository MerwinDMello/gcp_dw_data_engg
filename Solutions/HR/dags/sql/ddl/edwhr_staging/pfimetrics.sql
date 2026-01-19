CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.pfimetrics (
pfiworkunit INT64
, pfiactivity INT64
, pfiqueueassignment INT64
, pmpfiuserprofile STRING
, pmpttaskname STRING
, pmpttasktype INT64
, actiondate DATETIME
, actiontaken STRING
, comment STRING
, authenticatedactor STRING
, dw_last_update_date_time DATETIME
)
  ;

