CREATE TABLE  IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_phone_mode_adjustment
(
  measure_id_text STRING NOT NULL,
  eff_from_date DATE NOT NULL,
  mode_adjustment_amt NUMERIC(32, 3),
  bottom_mode_adjustment_amt NUMERIC(32, 3),
  eff_to_date DATE NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);