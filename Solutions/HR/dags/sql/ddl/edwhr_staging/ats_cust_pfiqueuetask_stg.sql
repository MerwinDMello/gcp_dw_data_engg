CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiqueuetask_stg
(
  _action STRING,
  _asoftimestamp STRING,
  documentid INT64,
  duedate STRING,
  filterkey STRING,
  filtervalue STRING,
  iscurrentactionwaitingsnapshot STRING,
  isproxy STRING,
  pfiactivity INT64,
  pfiqueueassignment INT64,
  pfitask_taskname STRING,
  pfitask_tasktype INT64,
  pfitask_tasktype_state STRING,
  pfiworkunit INT64,
  proxiedtaskname STRING,
  proxiedtasktype STRING,
  proxiedtasktype_state STRING,
  proxieduser STRING,
  repset_variation_id INT64,
  startdate STRING,
  status INT64,
  status_state STRING,
  uniqueid STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);