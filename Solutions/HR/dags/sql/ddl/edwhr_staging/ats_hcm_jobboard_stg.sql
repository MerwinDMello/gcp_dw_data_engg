CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobboard_stg
(
  _action STRING,
  active INT64,
  boardtype INT64,
  boardtype_state STRING,
  contactperson INT64,
  costperposting FLOAT64,
  createstamp STRING,
  description STRING,
  hrorganization STRING,
  jobboard STRING,
  jobboardkey STRING,
  jobboardurl STRING,
  partyid STRING,
  partyname STRING,
  recentjobdays INT64,
  repset_variation_id INT64,
  requisitioncostreason STRING,
  thirdpartyidvalue STRING,
  updatestamp STRING,
  validdaterange_begin STRING,
  validdaterange_end STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);