CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_jobapplication_bct_stg_wrk
(
  candidate INT64,
  jobapplication INT64,
  jobrequisition INT64
);