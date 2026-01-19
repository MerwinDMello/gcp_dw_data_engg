create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_status_type_stg`
(
  status_type_code STRING,
  status_type_desc STRING
)
