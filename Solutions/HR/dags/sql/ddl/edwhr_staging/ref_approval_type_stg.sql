create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_approval_type_stg`
(
  approval_type_code STRING NOT NULL,
  approval_type_desc STRING
)
