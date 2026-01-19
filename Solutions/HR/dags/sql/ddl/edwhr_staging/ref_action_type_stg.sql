create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_action_type_stg`
(
  action_type_code STRING NOT NULL,
  action_type_desc STRING NOT NULL
)
