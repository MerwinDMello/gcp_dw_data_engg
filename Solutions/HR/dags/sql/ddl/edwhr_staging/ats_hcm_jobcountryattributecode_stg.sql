CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobcountryattributecode_stg
(
  _action STRING,
  active INT64,
  country STRING,
  countrykey STRING,
  createstamp STRING,
  description STRING,
  hrorganization STRING,
  jobcountryattributecategory STRING,
  jobcountryattributecategorykey STRING,
  jobcountryattributecode STRING,
  jobcountryattributecodekey STRING,
  repset_variation_id INT64,
  updatestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);