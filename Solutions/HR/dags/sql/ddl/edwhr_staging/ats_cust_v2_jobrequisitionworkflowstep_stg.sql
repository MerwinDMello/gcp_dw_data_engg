CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobrequisitionworkflowstep_stg
(
  _action STRING,
  _asoftimestamp STRING,
  atsworkflow STRING,
  atsworkflowkey STRING,
  atsworkflowstep STRING,
  atsworkflowstepdescription STRING,
  atsworkflowstepkey STRING,
  create_stamp_actor STRING,
  create_stamp_timestamp STRING,
  createstamp STRING,
  displayorder INT64,
  hrorganization STRING,
  hrorganization_cube_dimension_value STRING,
  jobapplicationcount INT64,
  jobrequisition INT64,
  jobrequisitionkey STRING,
  jobrequisitionworkflowstep INT64,
  jobrequisitionworkflowstep_cube_dimension_value STRING,
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