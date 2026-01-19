create table if not exists `{{ params.param_hr_stage_dataset_name }}.znmetrics`
(
  workunit STRING,
  hca_pfi_actvty INT64,
  status INT64,
  wf_task STRING,
  wf_work_desc STRING,
  start_date DATE,
  start_time TIME,
  end_date DATE,
  end_time TIME,
  hca_wf_id STRING,
  work_cat_value STRING,
  operation STRING,
  object_name STRING,
  key_string STRING,
  originator STRING,
  company INT64,
  z59set6_ss_sw STRING,
  dw_last_update_date_time DATETIME
)
