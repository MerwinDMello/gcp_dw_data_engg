CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobcountryattribute_stg
(
  _action STRING,
  active STRING,
  country STRING,
  countrykey STRING,
  createstamp STRING,
  hrorganization STRING,
  job STRING,
  jobcountryattributecategory STRING,
  jobcountryattributecategorykey STRING,
  jobcountryattributecode STRING,
  jobcountryattributecodekey STRING,
  jobcountryattributesubcode STRING,
  jobcountryattributesubcodekey STRING,
  jobkey STRING,
  repset_variation_id STRING,
  updatestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);