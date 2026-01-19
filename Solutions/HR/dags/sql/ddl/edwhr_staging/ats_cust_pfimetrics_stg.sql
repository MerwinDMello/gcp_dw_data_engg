CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_pfimetrics_stg
(
  _action STRING,
  _asoftimestamp STRING,
  actiondate STRING,
  actiontaken STRING,
  arraytest STRING,
  authenticatedactor STRING,
  comment STRING,
  message STRING,
  pfiactivity INT64,
  pfimetrics_pfitask_taskname STRING,
  pfimetrics_pfitask_tasktype INT64,
  pfimetrics_pfitask_tasktype_state STRING,
  pfimetrics_pfiuserprofile STRING,
  pfiqueueassignment INT64,
  pfiworkunit INT64,
  repset_variation_id INT64,
  uniqueid STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);