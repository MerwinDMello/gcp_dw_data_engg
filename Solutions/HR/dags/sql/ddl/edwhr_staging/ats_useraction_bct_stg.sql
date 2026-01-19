CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_useraction_bct_stg
(
  action STRING,
  repset_variation_id INT64,
  asoftimestamp DATETIME,
  useraction STRING,
  actionname STRING,
  businessclass STRING,
  businessclass_cdv STRING,
  description STRING,
  errortext STRING,
  status INT64,
  status_state STRING,
  type_integer INT64,
  type_state STRING,
  useraction_cdv STRING,
  usereditortype INT64,
  usereditortype_state STRING,
  createstamp DATETIME,
  updatestamp DATETIME
);