CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_source_stg
(
  _action STRING,
  active INT64,
  createstamp STRING,
  description STRING,
  hrorganization STRING,
  repset_variation_id INT64,
  source STRING,
  sourcekey STRING,
  updatestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);