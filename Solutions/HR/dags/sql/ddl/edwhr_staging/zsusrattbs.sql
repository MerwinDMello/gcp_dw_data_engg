create table if not exists `{{ params.param_hr_stage_dataset_name }}.zsusrattbs`
(
  hca_uid_id STRING,
  hca_syscode STRING,
  obj_type STRING,
  hca_obj_val1 STRING,
  hca_obj_val2 STRING,
  hca_obj_val3 STRING,
  hca_obj_val4 STRING,
  hca_obj_val5 STRING,
  status_cd STRING,
  hca_setup_flag STRING,
  create_date DATE,
  create_time TIME,
  create_user STRING,
  update_date DATE,
  update_tm TIME,
  user_id STRING,
  dw_last_update_date_time DATETIME NOT NULL
)
