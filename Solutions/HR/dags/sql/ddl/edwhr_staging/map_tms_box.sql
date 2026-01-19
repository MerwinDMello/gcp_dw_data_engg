CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.map_tms_box
(
  tms_box_potential STRING,
  ovrall_calibrated_perf_rvw STRING,
  box_score STRING,
  box_description STRING,
  box_bracket STRING,
  dw_last_update_date DATETIME
);
