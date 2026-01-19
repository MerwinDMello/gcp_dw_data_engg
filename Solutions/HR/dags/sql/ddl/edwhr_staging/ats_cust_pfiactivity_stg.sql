CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiactivity_stg
(
  _action STRING,
  _asoftimestamp STRING,
  activityname STRING,
  activitytype STRING,
  actstatus STRING,
  actstatus_state STRING,
  enddate STRING,
  log STRING,
  pfiactivity INT64,
  pfiworkunit INT64,
  repset_variation_id INT64,
  startdate STRING,
  uniqueid STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);