CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_worktype_stg
(
  _action STRING,
  active INT64,
  candidatedisplayindicator STRING,
  candidatedisplayindicator_state STRING,
  createstamp STRING,
  description STRING,
  hrorganization STRING,
  limitworktype INT64,
  repset_variation_id INT64,
  updatestamp STRING,
  worktype STRING,
  worktypekey STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);