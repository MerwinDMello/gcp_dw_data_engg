CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_candidateeducation_stg
(
  _action STRING,
  candidate INT64,
  candidateeducation INT64,
  candidatekey STRING,
  completiondatemonth INT64,
  completiondateyear INT64,
  cost FLOAT64,
  createstamp STRING,
  education STRING,
  educationkey STRING,
  hrorganization STRING,
  inprocess INT64,
  institution STRING,
  rating STRING,
  repset_variation_id INT64,
  specialization STRING,
  updatestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);