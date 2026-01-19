CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_jobtemplate
(
  file_date DATE,
  job_template_num STRING,
  effectivefrom STRING,
  effectivetill STRING,
  jobcode STRING,
  basejobtemplate_number STRING,
  department_number STRING,
  jobinformation_number STRING,
  state_number STRING,
  dw_last_update_date_time DATETIME
);