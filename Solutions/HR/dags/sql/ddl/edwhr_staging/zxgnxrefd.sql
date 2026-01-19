create table if not exists `{{ params.param_hr_stage_dataset_name }}.zxgnxrefd`
(
  lvl_1_key STRING NOT NULL,
  lvl_2_key STRING NOT NULL,
  lvl_3_key STRING NOT NULL,
  dtl_key STRING NOT NULL,
  dtl_value STRING NOT NULL,
  dtl_filler STRING NOT NULL,
  hca_filler STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
