CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_jobreqquestionlistvalue_stg
(
  _action STRING,
  _asoftimestamp STRING,
  create_stamp_actor STRING,
  create_stamp_timestamp STRING,
  createstamp STRING,
  description STRING,
  hrorganization STRING,
  jobreqquestion INT64,
  jobreqquestionlistvalue STRING,
  jobrequisition INT64,
  repset_variation_id INT64,
  uniqueid STRING,
  update_stamp_actor STRING,
  update_stamp_timestamp STRING,
  updatestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);