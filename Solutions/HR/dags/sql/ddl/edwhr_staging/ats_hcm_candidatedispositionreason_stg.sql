CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_candidatedispositionreason_stg
(
  _action STRING,
  active INT64,
  candidatedispositionreason STRING,
  candidatedispositionreasonkey STRING,
  createstamp STRING,
  description STRING,
  hrorganization STRING,
  repset_variation_id INT64,
  updatestamp STRING,
  useforwithdrawaction INT64,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);