CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunitvariable_stg
(
  _action STRING,
  _asoftimestamp STRING,
  pfiworkunit INT64,
  pfiworkunit_cube_dimension_value STRING,
  pfiworkunitvariable STRING,
  pfiworkunitvariable_cube_dimension_value STRING,
  repset_variation_id INT64,
  seqnbr INT64,
  uniqueid STRING,
  variabletype INT64,
  variabletype_state STRING,
  variablevalue STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);