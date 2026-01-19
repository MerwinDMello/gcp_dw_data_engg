CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_candidateidentificationnumber_stg
(
  _action STRING,
  _asoftimestamp STRING,
  candidate INT64,
  candidateidentificationnumber_auditlog_encrypted INT64,
  candidateidentificationnumber_effectivedatedlog_encrypted INT64,
  candidateidentificationnumber_encryptedobject_encrypted INT64,
  comments STRING,
  country STRING,
  create_stamp_actor STRING,
  create_stamp_timestamp STRING,
  expirationdate STRING,
  hcatestfield STRING,
  hrorganization STRING,
  identificationnumber STRING,
  identificationnumberdisplay STRING,
  idnumberdocument_file_encrypted FLOAT64,
  idnumberdocument_mimetype STRING,
  idnumberdocument_title STRING,
  pending INT64,
  repset_variation_id INT64,
  sequencenumber INT64,
  uniqueid STRING,
  update_stamp_actor STRING,
  update_stamp_timestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);