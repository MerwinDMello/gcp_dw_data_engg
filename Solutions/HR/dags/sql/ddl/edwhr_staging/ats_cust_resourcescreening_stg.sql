CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_resourcescreening_stg
(
  _action STRING,
  _asoftimestamp STRING,
  create_stamp_actor STRING,
  create_stamp_timestamp STRING,
  hcaresourcescreeningvendorresultsurl STRING,
  hcaresourcescreeningvendorreviewurl STRING,
  hcaresourcescreeningvendorstatus STRING,
  hcaresourcescreeningvendorstatusdate STRING,
  hrorganization STRING,
  passfail INT64,
  repset_variation_id INT64,
  resourcescreeningpackage INT64,
  resultsdocumentation_mimetype STRING,
  resultsdocumentation_title STRING,
  resultstatus STRING,
  score INT64,
  screening STRING,
  sequencenumber INT64,
  uniqueid STRING,
  update_stamp_actor STRING,
  update_stamp_timestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);